//! `gt-beads` — vocabulario de beads + puerto de repositorio (Paso 4).
//!
//! El bead es la unidad de trabajo del orquestador. Este crate define el **tipo** y el
//! **puerto** (`BeadRepository`); la BD lo implementa por inversión de dependencia
//! (`gt-store-dolt`). Incluye una impl in-memory para que cualquier test del dominio corra
//! sin levantar Dolt — y el mismo test debe pasar contra el adaptador real (gate del Paso 4).

mod bead;
mod memory;
mod repo;

pub use bead::{Bead, BeadStatus, IssueDep};
pub use memory::InMemoryBeads;
pub use repo::BeadRepository;
