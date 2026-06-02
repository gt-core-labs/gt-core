//! Domain persistence port + in-memory implementation.
//!
//! The catalog tables (`skills` + `role_bindings` in Dolt, eventually) are write-light
//! (registrations are rare, toggles small-volume) and read-mostly. The port lets a
//! `gt-store-dolt` adapter plug a real implementation later; the in-memory version is
//! the test safety net and the default the actor uses until the edge installs one.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::{RoleBinding, Skill};

/// Port: skills + role-binding persistence. RPITIT with `+ Send` so the port needs no
/// `async_trait` or boxing (same pattern as `RigRepository`, `QuotaRepository`).
pub trait SkillsRepository: Send + Sync {
    /// Upsert a skill (idempotent: re-insert with the same id replaces).
    fn upsert_skill(&self, skill: &Skill) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Remove a skill by id. Returns `Ok(false)` if the row was absent. Adapters MUST
    /// cascade the deletion to every binding so the live state matches the rebuilt
    /// reducer state byte-for-byte.
    fn remove_skill(&self, id: &str) -> impl Future<Output = Result<bool, AppError>> + Send;

    /// Read a single skill by id.
    fn get_skill(&self, id: &str) -> impl Future<Output = Result<Option<Skill>, AppError>> + Send;

    /// Snapshot every registered skill, sorted by id.
    fn list_skills(&self) -> impl Future<Output = Result<Vec<Skill>, AppError>> + Send;

    /// Upsert a role binding (replaces the row keyed by `role`).
    fn upsert_binding(
        &self,
        binding: &RoleBinding,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Read a single binding by role name. `None` when the role has no binding row.
    fn get_binding(
        &self,
        role: &str,
    ) -> impl Future<Output = Result<Option<RoleBinding>, AppError>> + Send;

    /// Snapshot every binding, sorted by role.
    fn list_bindings(&self) -> impl Future<Output = Result<Vec<RoleBinding>, AppError>> + Send;
}

/// Delegate through `Arc` so callers can share one adapter across multiple actors.
impl<R: SkillsRepository + ?Sized> SkillsRepository for std::sync::Arc<R> {
    fn upsert_skill(&self, skill: &Skill) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_skill(skill).await }
    }
    fn remove_skill(&self, id: &str) -> impl Future<Output = Result<bool, AppError>> + Send {
        async move { (**self).remove_skill(id).await }
    }
    fn get_skill(&self, id: &str) -> impl Future<Output = Result<Option<Skill>, AppError>> + Send {
        async move { (**self).get_skill(id).await }
    }
    fn list_skills(&self) -> impl Future<Output = Result<Vec<Skill>, AppError>> + Send {
        async move { (**self).list_skills().await }
    }
    fn upsert_binding(
        &self,
        binding: &RoleBinding,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_binding(binding).await }
    }
    fn get_binding(
        &self,
        role: &str,
    ) -> impl Future<Output = Result<Option<RoleBinding>, AppError>> + Send {
        async move { (**self).get_binding(role).await }
    }
    fn list_bindings(&self) -> impl Future<Output = Result<Vec<RoleBinding>, AppError>> + Send {
        async move { (**self).list_bindings().await }
    }
}

/// In-memory implementation: tests + the default before an edge adapter is wired.
#[derive(Default)]
pub struct InMemorySkills {
    skills: Mutex<std::collections::BTreeMap<String, Skill>>,
    bindings: Mutex<std::collections::BTreeMap<String, RoleBinding>>,
}

impl SkillsRepository for InMemorySkills {
    async fn upsert_skill(&self, skill: &Skill) -> Result<(), AppError> {
        self.skills
            .lock()
            .unwrap()
            .insert(skill.id.clone(), skill.clone());
        Ok(())
    }

    async fn remove_skill(&self, id: &str) -> Result<bool, AppError> {
        let removed = self.skills.lock().unwrap().remove(id).is_some();
        if removed {
            // Cascade: drop the skill from every binding so the persisted view matches
            // the in-process catalog. The Dolt adapter is expected to do the same in a
            // single transaction.
            for b in self.bindings.lock().unwrap().values_mut() {
                b.enabled_skills.remove(id);
            }
        }
        Ok(removed)
    }

    async fn get_skill(&self, id: &str) -> Result<Option<Skill>, AppError> {
        Ok(self.skills.lock().unwrap().get(id).cloned())
    }

    async fn list_skills(&self) -> Result<Vec<Skill>, AppError> {
        Ok(self.skills.lock().unwrap().values().cloned().collect())
    }

    async fn upsert_binding(&self, binding: &RoleBinding) -> Result<(), AppError> {
        self.bindings
            .lock()
            .unwrap()
            .insert(binding.role.clone(), binding.clone());
        Ok(())
    }

    async fn get_binding(&self, role: &str) -> Result<Option<RoleBinding>, AppError> {
        Ok(self.bindings.lock().unwrap().get(role).cloned())
    }

    async fn list_bindings(&self) -> Result<Vec<RoleBinding>, AppError> {
        Ok(self.bindings.lock().unwrap().values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn in_memory_roundtrips_skills() {
        let repo = InMemorySkills::default();
        let s = Skill::new("alpha", "Alpha", "first skill", vec!["alpha.read".into()], 1);
        repo.upsert_skill(&s).await.unwrap();
        assert_eq!(repo.get_skill("alpha").await.unwrap(), Some(s.clone()));
        assert_eq!(repo.list_skills().await.unwrap(), vec![s]);
        assert!(repo.remove_skill("alpha").await.unwrap());
        assert!(!repo.remove_skill("alpha").await.unwrap());
        assert!(repo.get_skill("alpha").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn remove_skill_cascades_to_bindings() {
        let repo = InMemorySkills::default();
        let s = Skill::new("alpha", "Alpha", "", vec![], 1);
        repo.upsert_skill(&s).await.unwrap();

        let mut binding = RoleBinding::new("deacon");
        binding.enabled_skills.insert("alpha".into());
        binding.enabled_skills.insert("beta".into());
        repo.upsert_binding(&binding).await.unwrap();

        repo.remove_skill("alpha").await.unwrap();
        let after = repo.get_binding("deacon").await.unwrap().unwrap();
        let expected: BTreeSet<String> = ["beta".into()].into_iter().collect();
        assert_eq!(after.enabled_skills, expected);
    }
}
