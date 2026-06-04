//! `gt-store-blob` — S3-compatible object store for binary document attachments
//! (hq-docs-store.2, docs/11).
//!
//! Holds the **bytes** of `kind='blob'` documents (PDF / image / Office). The Postgres
//! `documents` row keeps only the `bucket`/`key` pointer + the extracted text; `.md`
//! content never reaches this crate (it lives in `documents.body_md`).
//!
//! Backend-agnostic over [`opendal`]: [`BlobStore::from_s3`] targets MinIO/S3 in prod,
//! [`BlobStore::from_fs`] backs local dev + tests with no running object store. Callers
//! (the `DocumentsRepository` PG adapter, hq-docs-store.3) never branch on backend.
//!
//! Keys are per-tenant via [`BlobStore::key_for`]:
//! `<ws>/<owner_type>/<owner_id>/<sha256>-<filename>`, so one workspace's blobs are
//! namespaced under its slug and content-addressed for dedup (hq-docs decision 3).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::time::Duration;

use opendal::Operator;
use sha2::{Digest, Sha256};

/// What can go wrong talking to the object store.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// The backend (config or I/O) failed.
    #[error("blob store backend error: {0}")]
    Backend(#[from] opendal::Error),
    /// A presigned URL was requested from a backend that cannot mint one (e.g. the
    /// local `fs` dev backend). Callers should serve such blobs by streaming instead.
    #[error("backend does not support presigned URLs")]
    PresignUnsupported,
}

/// Hex-encoded SHA-256 of `bytes` — the content hash used as the dedup key and the
/// key-name component. Stable across uploads of identical content.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// An object store handle. Cheap to clone ([`Operator`] is internally reference-counted).
#[derive(Clone)]
pub struct BlobStore {
    op: Operator,
}

impl BlobStore {
    /// Build a store over an S3-compatible endpoint (MinIO in the `gt-app` deploy,
    /// or AWS S3). `endpoint` is the base URL (e.g. `http://minio:9000`).
    pub fn from_s3(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<Self, BlobError> {
        let builder = opendal::services::S3::default()
            .endpoint(endpoint)
            .bucket(bucket)
            .region(region)
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key);
        let op = Operator::new(builder)?.finish();
        Ok(Self { op })
    }

    /// Build a store backed by a local directory. For dev + tests with no object store
    /// running; presigned URLs are unsupported on this backend (see [`Self::presign_read`]).
    pub fn from_fs(root: &str) -> Result<Self, BlobError> {
        let builder = opendal::services::Fs::default().root(root);
        let op = Operator::new(builder)?.finish();
        Ok(Self { op })
    }

    /// Wrap an already-built [`Operator`] (advanced / test injection).
    pub fn from_operator(op: Operator) -> Self {
        Self { op }
    }

    /// The per-tenant, content-addressed key for a blob:
    /// `<ws>/<owner_type>/<owner_id>/<sha256>-<filename>`.
    pub fn key_for(
        ws: &str,
        owner_type: &str,
        owner_id: &str,
        sha256: &str,
        filename: &str,
    ) -> String {
        format!("{ws}/{owner_type}/{owner_id}/{sha256}-{filename}")
    }

    /// Store `bytes` at `key`, tagging the object with `content_type` when given.
    /// Idempotent for identical content at a content-addressed key.
    pub async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), BlobError> {
        match content_type {
            Some(ct) => {
                self.op.write_with(key, bytes).content_type(ct).await?;
            }
            None => {
                self.op.write(key, bytes).await?;
            }
        }
        Ok(())
    }

    /// Fetch the bytes at `key`.
    pub async fn get(&self, key: &str) -> Result<Vec<u8>, BlobError> {
        let buf = self.op.read(key).await?;
        Ok(buf.to_vec())
    }

    /// Whether an object exists at `key` — the dedup probe before an upload.
    pub async fn exists(&self, key: &str) -> Result<bool, BlobError> {
        Ok(self.op.exists(key).await?)
    }

    /// Remove the object at `key` (the hard-delete a reconciler runs after a row's
    /// soft-delete window; the soft-delete itself only flips the PG row).
    pub async fn delete(&self, key: &str) -> Result<(), BlobError> {
        self.op.delete(key).await?;
        Ok(())
    }

    /// Mint a short-TTL presigned GET URL so a human/UI downloads the original directly
    /// from the object store without proxying bytes through the backend. Unsupported on
    /// the `fs` dev backend ([`BlobError::PresignUnsupported`]).
    pub async fn presign_read(&self, key: &str, ttl: Duration) -> Result<String, BlobError> {
        match self.op.presign_read(key, ttl).await {
            Ok(req) => Ok(req.uri().to_string()),
            Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                Err(BlobError::PresignUnsupported)
            }
            Err(e) => Err(BlobError::Backend(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_is_stable_and_hex() {
        let h = sha256_hex(b"hello");
        assert_eq!(h, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        assert_eq!(sha256_hex(b"hello"), h, "stable across calls");
    }

    #[test]
    fn key_is_per_tenant_and_content_addressed() {
        let k = BlobStore::key_for("acme", "epic", "hq-docs", "deadbeef", "spec.pdf");
        assert_eq!(k, "acme/epic/hq-docs/deadbeef-spec.pdf");
    }

    #[tokio::test]
    async fn fs_backend_roundtrips_put_get_exists_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::from_fs(dir.path().to_str().unwrap()).unwrap();
        let key = "acme/epic/hq-docs/abc-spec.pdf";

        assert!(!store.exists(key).await.unwrap(), "absent before put");
        store.put(key, b"PDFBYTES".to_vec(), Some("application/pdf")).await.unwrap();
        assert!(store.exists(key).await.unwrap(), "present after put");
        assert_eq!(store.get(key).await.unwrap(), b"PDFBYTES");

        store.delete(key).await.unwrap();
        assert!(!store.exists(key).await.unwrap(), "absent after delete");
    }

    #[tokio::test]
    async fn fs_backend_presign_is_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::from_fs(dir.path().to_str().unwrap()).unwrap();
        let err = store.presign_read("k", Duration::from_secs(60)).await.unwrap_err();
        assert!(matches!(err, BlobError::PresignUnsupported));
    }

    /// hq-docs-test-blob.3 — content-addressed dedup: at a content-addressed key the
    /// `exists` probe gates the upload, so identical content is written once (write-once),
    /// and a second attempt is a no-op that leaves the stored bytes intact.
    #[tokio::test]
    async fn identical_content_is_not_re_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::from_fs(dir.path().to_str().unwrap()).unwrap();

        let bytes = b"the exact same blob bytes".to_vec();
        let sha = sha256_hex(&bytes);
        // The key carries the content hash — identical content resolves to the same key.
        let key = BlobStore::key_for("acme", "epic", "hq-docs", &sha, "design.pdf");

        // The dedup guard the document handler runs: upload only when the object is absent.
        async fn put_if_absent(store: &BlobStore, key: &str, bytes: Vec<u8>) -> bool {
            if store.exists(key).await.unwrap() {
                return false; // already present — skip the upload
            }
            store.put(key, bytes, Some("application/pdf")).await.unwrap();
            true
        }

        assert!(put_if_absent(&store, &key, bytes.clone()).await, "first put uploads");
        assert!(
            !put_if_absent(&store, &key, bytes.clone()).await,
            "identical content at the same key is not re-uploaded (write-once)"
        );
        assert_eq!(store.get(&key).await.unwrap(), bytes, "stored bytes unchanged after the no-op");
    }

    /// hq-docs-test-blob.2 — MinIO/S3 backend roundtrip. Gated on `GT_BLOB_*` (a running
    /// object store): put → object present → get bytes match → presign returns a URL.
    /// No-op without the env, mirroring the `GT_PG_URL` contract gating.
    #[tokio::test]
    async fn s3_backend_roundtrips_and_presigns() {
        let (Ok(endpoint), Ok(bucket)) =
            (std::env::var("GT_BLOB_ENDPOINT"), std::env::var("GT_BLOB_BUCKET"))
        else {
            eprintln!("GT_BLOB_ENDPOINT/GT_BLOB_BUCKET unset; skipping S3 roundtrip test");
            return;
        };
        let region = std::env::var("GT_BLOB_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access = std::env::var("GT_BLOB_ACCESS_KEY").unwrap_or_default();
        let secret = std::env::var("GT_BLOB_SECRET_KEY").unwrap_or_default();

        let store = BlobStore::from_s3(&endpoint, &bucket, &region, &access, &secret)
            .expect("build S3 store");

        let bytes = format!("blob-{}", nonce_secs()).into_bytes();
        let sha = sha256_hex(&bytes);
        let key = BlobStore::key_for("default", "epic", "hq-docs", &sha, "roundtrip.bin");

        store.put(&key, bytes.clone(), Some("application/octet-stream")).await.expect("put");
        assert!(store.exists(&key).await.expect("exists"), "object present after put");
        assert_eq!(store.get(&key).await.expect("get"), bytes, "fetched bytes match");

        let url = store
            .presign_read(&key, Duration::from_secs(60))
            .await
            .expect("S3 backend mints a presigned URL");
        assert!(url.starts_with("http"), "presign returns an http(s) URL, got {url:?}");

        store.delete(&key).await.expect("cleanup");
    }

    /// A coarse unique suffix for S3 object keys (seconds-since-epoch); avoids pulling a
    /// time/uuid dependency just for the gated roundtrip key.
    fn nonce_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
