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

    /// Build a minimal OOXML container in memory: a zip with one text-bearing part holding
    /// `xml`. Mirrors the real docx/xlsx/pptx layout the extractor scans (`word/`,
    /// `ppt/slides/`, `xl/sharedStrings.xml`).
    fn ooxml_zip(part_name: &str, xml: &str) -> Vec<u8> {
        use std::io::Write;
        use zip::write::SimpleFileOptions;

        let mut cursor = Cursor::new(Vec::new());
        {
            let mut zw = zip::ZipWriter::new(&mut cursor);
            let opts = SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            // A content-types part so the container looks like a real OOXML file (the extractor
            // ignores it — only the text-bearing part below matters).
            zw.start_file("[Content_Types].xml", opts).unwrap();
            zw.write_all(br#"<?xml version="1.0"?><Types/>"#).unwrap();
            zw.start_file(part_name, opts).unwrap();
            zw.write_all(xml.as_bytes()).unwrap();
            zw.finish().unwrap();
        }
        cursor.into_inner()
    }

    /// hq-docs-test-extract.2 (Office) — DOCX/XLSX/PPTX fixtures yield their text nodes.
    #[test]
    fn ooxml_extracts_text_nodes_from_docx_xlsx_pptx() {
        // DOCX: word/document.xml with multiple <w:t> runs.
        let docx = ooxml_zip(
            "word/document.xml",
            r#"<?xml version="1.0"?><w:document><w:body><w:p><w:r><w:t>hello</w:t></w:r><w:r><w:t>world</w:t></w:r></w:p></w:body></w:document>"#,
        );
        let text = extract_ooxml(&docx).unwrap();
        assert!(text.contains("hello") && text.contains("world"), "docx text nodes extracted: {text:?}");

        // XLSX: xl/sharedStrings.xml holds the cell strings.
        let xlsx = ooxml_zip(
            "xl/sharedStrings.xml",
            r#"<?xml version="1.0"?><sst><si><t>revenue</t></si><si><t>2026</t></si></sst>"#,
        );
        let text = extract_ooxml(&xlsx).unwrap();
        assert!(text.contains("revenue") && text.contains("2026"), "xlsx shared strings extracted: {text:?}");

        // PPTX: ppt/slides/slideN.xml carries slide text.
        let pptx = ooxml_zip(
            "ppt/slides/slide1.xml",
            r#"<?xml version="1.0"?><p:sld><a:t>roadmap</a:t></p:sld>"#,
        );
        let text = extract_ooxml(&pptx).unwrap();
        assert!(text.contains("roadmap"), "pptx slide text extracted: {text:?}");
    }

    /// Build a single-page PDF whose content stream shows `text`, using the same lopdf the
    /// reader is built on (no version skew, no committed binary fixture).
    fn pdf_with_text(text: &str) -> Vec<u8> {
        use lopdf::content::{Content, Operation};
        use lopdf::{dictionary, Document, Object, Stream};

        let mut doc = Document::with_version("1.5");
        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });
        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! { "F1" => font_id },
        });
        let content = Content {
            operations: vec![
                Operation::new("BT", vec![]),
                Operation::new("Tf", vec!["F1".into(), 24.into()]),
                Operation::new("Td", vec![72.into(), 700.into()]),
                Operation::new("Tj", vec![Object::string_literal(text)]),
                Operation::new("ET", vec![]),
            ],
        };
        let content_id =
            doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let page_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => resources_id,
            "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
        });
        let pages = dictionary! {
            "Type" => "Pages",
            "Kids" => vec![page_id.into()],
            "Count" => 1,
        };
        doc.objects.insert(pages_id, Object::Dictionary(pages));
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);

        let mut buf = Vec::new();
        doc.save_to(&mut buf).expect("serialize pdf");
        buf
    }

    /// hq-docs-test-extract.2 (PDF) — a PDF fixture extracts to its expected text.
    #[tokio::test]
    async fn pdf_fixture_extracts_expected_text() {
        let pdf = pdf_with_text("GASTOWN DESIGN SPEC");

        // The pure function and the async dispatch agree.
        let direct = extract_pdf(&pdf).expect("extract_pdf");
        let normalized: String = direct.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            normalized.contains("GASTOWN") && normalized.contains("SPEC"),
            "pdf text extracted: {normalized:?}"
        );

        let via_pipeline = Extractor::without_ocr().extract("application/pdf", &pdf).await.unwrap();
        assert!(via_pipeline.contains("GASTOWN"), "dispatch routes application/pdf to the PDF extractor");
    }
}
