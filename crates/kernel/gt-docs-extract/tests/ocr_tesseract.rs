//! hq-docs-test-extract.3 — the tesseract [`OcrEngine`] impl, exercised end to end.
//!
//! Gated on the non-default `ocr-tesseract` feature (CI: `cargo test -p gt-docs-extract
//! --features ocr-tesseract`, where libtesseract/libleptonica are installed). Builds a real
//! PNG in memory and runs it through the [`Extractor`] wired with the real tesseract engine,
//! proving an `image/*` blob routes to a *live* OCR engine — not the [`NoOcr`] null engine
//! that yields `Unsupported` (covered for the seam in the crate's unit tests).
//!
//! The assertion is deliberately the robust property — the real engine *processes* the image
//! and returns `Ok` — rather than an exact transcription. OCR output of a synthetic raster is
//! not byte-stable across tesseract/leptonica versions, so asserting specific text would make
//! a hard CI gate flaky for no added coverage of the wiring this bead is about.
#![cfg(feature = "ocr-tesseract")]

use std::sync::Arc;

use gt_docs_extract::tesseract::TesseractOcr;
use gt_docs_extract::Extractor;

/// A small high-contrast PNG: black bars on a white field. A valid raster leptonica decodes
/// and tesseract scans, without depending on a font rasterizer or a committed binary fixture.
fn png_fixture() -> Vec<u8> {
    use image::{ImageFormat, RgbImage};

    let (w, h) = (320u32, 120u32);
    let mut img = RgbImage::from_pixel(w, h, image::Rgb([255, 255, 255]));
    // A few thick black bands — enough structure for the engine to scan a real page region.
    for y in 40..60 {
        for x in 30..290 {
            img.put_pixel(x, y, image::Rgb([0, 0, 0]));
        }
    }
    for y in 75..95 {
        for x in 30..200 {
            img.put_pixel(x, y, image::Rgb([0, 0, 0]));
        }
    }

    let mut buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut buf, ImageFormat::Png).expect("encode png");
    buf.into_inner()
}

#[tokio::test]
async fn tesseract_engine_processes_an_image_through_the_extractor() {
    let png = png_fixture();
    let extractor = Extractor::new(Arc::new(TesseractOcr::default()));

    // The real engine ran (no `Unsupported`): an `image/*` blob reached a live OCR engine,
    // not the null one. Whatever text it transcribes is acceptable; that it ran is the point.
    let result = extractor.extract("image/png", &png).await;
    assert!(
        result.is_ok(),
        "the tesseract OcrEngine must process an image/png blob end to end, got {result:?}"
    );
}
