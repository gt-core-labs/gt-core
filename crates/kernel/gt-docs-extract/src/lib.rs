//! `gt-docs-extract` — text extraction for binary document attachments
//! (hq-docs-store.4, docs/11).
//!
//! Turns a `kind='blob'` upload (PDF / Office / image) into the `extracted_text` a model
//! reads as context and search indexes. [`Extractor::extract`] dispatches on the MIME type.
//!
//! **OCR is decoupled.** Image extraction goes through the [`OcrEngine`] *trait*, never a
//! concrete engine. The default [`Extractor::new`] wires [`NoOcr`] (images yield
//! [`ExtractError::Unsupported`]); a deployment injects a real engine. Tesseract is one
//! impl behind the non-default `ocr-tesseract` feature ([`tesseract::TesseractOcr`]); a
//! future engine (cloud OCR, PaddleOCR, …) is a new [`OcrEngine`] impl with no change to
//! callers. PDF + Office extractors are pure-Rust and always compiled.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io::{Cursor, Read};
use std::sync::Arc;

use async_trait::async_trait;

/// Why extraction failed.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    /// No extractor (or OCR engine) handles this content type.
    #[error("unsupported content type for extraction: {0}")]
    Unsupported(String),
    /// The document could not be parsed.
    #[error("parse error: {0}")]
    Parse(String),
    /// The OCR engine failed.
    #[error("ocr error: {0}")]
    Ocr(String),
}

/// The swappable OCR seam. An implementation turns image bytes into text; it is the ONLY
/// coupling point to a specific OCR technology, so swapping engines never touches the
/// pipeline or its callers.
#[async_trait]
pub trait OcrEngine: Send + Sync {
    /// Extract text from `image` bytes (`content_type` is the MIME, e.g. `image/png`).
    async fn ocr(&self, image: &[u8], content_type: &str) -> Result<String, ExtractError>;
}

/// The null OCR engine: every image is [`ExtractError::Unsupported`]. The default until a
/// deployment injects a real [`OcrEngine`], so a build with no OCR feature still links.
pub struct NoOcr;

#[async_trait]
impl OcrEngine for NoOcr {
    async fn ocr(&self, _image: &[u8], content_type: &str) -> Result<String, ExtractError> {
        Err(ExtractError::Unsupported(format!("{content_type} (no OCR engine configured)")))
    }
}

/// Extract text from a PDF (pure-Rust, synchronous — CPU-bound).
pub fn extract_pdf(bytes: &[u8]) -> Result<String, ExtractError> {
    pdf_extract::extract_text_from_mem(bytes).map_err(|e| ExtractError::Parse(e.to_string()))
}

/// Extract visible text from an OOXML document (`.docx`/`.xlsx`/`.pptx`): unzip and
/// concatenate the text nodes of every relevant XML part. Pure-Rust, synchronous.
pub fn extract_ooxml(bytes: &[u8]) -> Result<String, ExtractError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut zip = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| ExtractError::Parse(format!("not a zip/OOXML container: {e}")))?;

    let mut out = String::new();
    for i in 0..zip.len() {
        let mut file = zip.by_index(i).map_err(|e| ExtractError::Parse(e.to_string()))?;
        let name = file.name().to_string();
        // The text-bearing parts across docx/pptx/xlsx.
        let relevant = name.starts_with("word/")
            || name.starts_with("ppt/slides/")
            || name == "xl/sharedStrings.xml";
        if !relevant || !name.ends_with(".xml") {
            continue;
        }
        let mut xml = String::new();
        file.read_to_string(&mut xml).map_err(|e| ExtractError::Parse(e.to_string()))?;

        let mut reader = Reader::from_str(&xml);
        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Text(t)) => {
                    let text = t.unescape().unwrap_or_default();
                    if !text.trim().is_empty() {
                        out.push_str(&text);
                        out.push(' ');
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(ExtractError::Parse(e.to_string())),
                _ => {}
            }
            buf.clear();
        }
    }
    Ok(out.trim().to_string())
}

/// The extraction pipeline: dispatches a blob to the right extractor by MIME type, routing
/// images through the injected [`OcrEngine`].
#[derive(Clone)]
pub struct Extractor {
    ocr: Arc<dyn OcrEngine>,
}

impl Extractor {
    /// Build a pipeline with a specific OCR engine.
    pub fn new(ocr: Arc<dyn OcrEngine>) -> Self {
        Self { ocr }
    }

    /// Build a pipeline with no OCR ([`NoOcr`]) — PDF/Office work; images are unsupported.
    pub fn without_ocr() -> Self {
        Self { ocr: Arc::new(NoOcr) }
    }

    /// Extract text from `bytes` of the given `content_type`. PDF/Office run on a blocking
    /// thread (CPU-bound); images go through the OCR engine.
    pub async fn extract(&self, content_type: &str, bytes: &[u8]) -> Result<String, ExtractError> {
        let ct = content_type.split(';').next().unwrap_or(content_type).trim().to_lowercase();
        match ct.as_str() {
            "application/pdf" => {
                let owned = bytes.to_vec();
                blocking(move || extract_pdf(&owned)).await
            }
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            | "application/vnd.openxmlformats-officedocument.presentationml.presentation" => {
                let owned = bytes.to_vec();
                blocking(move || extract_ooxml(&owned)).await
            }
            _ if ct.starts_with("image/") => self.ocr.ocr(bytes, &ct).await,
            other => Err(ExtractError::Unsupported(other.to_string())),
        }
    }
}

/// Run a CPU-bound extractor on the blocking pool, flattening the join error.
async fn blocking<F>(f: F) -> Result<String, ExtractError>
where
    F: FnOnce() -> Result<String, ExtractError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ExtractError::Parse(format!("extractor task panicked: {e}")))?
}

/// Tesseract-backed [`OcrEngine`] (feature `ocr-tesseract`). One implementation of the
/// decoupled OCR seam; links `libtesseract`/`libleptonica`.
#[cfg(feature = "ocr-tesseract")]
pub mod tesseract {
    use super::{ExtractError, OcrEngine};
    use async_trait::async_trait;

    /// OCR via the system tesseract. `lang` is the trained-data language (e.g. `"eng"`).
    pub struct TesseractOcr {
        lang: String,
    }

    impl TesseractOcr {
        /// Build an engine for the given tesseract language code.
        pub fn new(lang: impl Into<String>) -> Self {
            Self { lang: lang.into() }
        }
    }

    impl Default for TesseractOcr {
        fn default() -> Self {
            Self::new("eng")
        }
    }

    #[async_trait]
    impl OcrEngine for TesseractOcr {
        async fn ocr(&self, image: &[u8], _content_type: &str) -> Result<String, ExtractError> {
            let lang = self.lang.clone();
            let bytes = image.to_vec();
            tokio::task::spawn_blocking(move || {
                let mut lt = leptess::LepTess::new(None, &lang)
                    .map_err(|e| ExtractError::Ocr(e.to_string()))?;
                lt.set_image_from_mem(&bytes).map_err(|e| ExtractError::Ocr(e.to_string()))?;
                lt.get_utf8_text().map_err(|e| ExtractError::Ocr(e.to_string()))
            })
            .await
            .map_err(|e| ExtractError::Ocr(format!("ocr task panicked: {e}")))?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_type_is_unsupported() {
        let ex = Extractor::without_ocr();
        let err = ex.extract("application/zip", b"PK\x03\x04").await.unwrap_err();
        assert!(matches!(err, ExtractError::Unsupported(_)));
    }

    #[tokio::test]
    async fn image_without_ocr_engine_is_unsupported() {
        let ex = Extractor::without_ocr();
        let err = ex.extract("image/png", b"\x89PNG").await.unwrap_err();
        assert!(matches!(err, ExtractError::Unsupported(_)));
    }

    #[tokio::test]
    async fn injected_ocr_engine_is_used_for_images() {
        struct StubOcr;
        #[async_trait]
        impl OcrEngine for StubOcr {
            async fn ocr(&self, _image: &[u8], _ct: &str) -> Result<String, ExtractError> {
                Ok("ocr-text".to_string())
            }
        }
        let ex = Extractor::new(Arc::new(StubOcr));
        let got = ex.extract("image/png", b"\x89PNG").await.unwrap();
        assert_eq!(got, "ocr-text");
    }

    #[test]
    fn ooxml_rejects_non_zip() {
        let err = extract_ooxml(b"not a zip").unwrap_err();
        assert!(matches!(err, ExtractError::Parse(_)));
    }
}
