//! End-to-end authoring lifecycle for a module contract (`hq-mod-contracts.5`).
//!
//! Drives the crate's whole public surface the way a module author and CI would,
//! using only `pub` API: declare a DTO, generate its TypeScript, freeze a
//! baseline, then evolve the surface and watch the drift check and semver
//! enforcement react. If this test compiles and passes, the pieces from `.1`–`.4`
//! compose into a coherent author workflow.

use gt_module_contracts::{
    enforce, typescript_module, version_file_name, ContractBaseline, ContractVersion, DriftCheck,
    SemverVerdict, SurfaceHash,
};
use schemars::JsonSchema;
use serde_json::Value;

/// A module's MCP input DTO at contract v1.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct CreateBeadV1 {
    title: String,
    priority: u32,
}

/// v2: an *additive* change — a new optional field, nothing removed.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct CreateBeadV2Additive {
    title: String,
    priority: u32,
    parent: Option<String>,
}

fn schema_of<T: JsonSchema>() -> Value {
    let root = schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>();
    serde_json::to_value(&root).unwrap()
}

/// The wire-surface items a module would register — here, one MCP tool plus the
/// event it emits. Field-level additions live inside the tool's DTO schema, so a
/// DTO that grows shows up as a changed item string.
fn surface(dto_marker: &str) -> Vec<String> {
    vec![
        format!("tool:beads.create.execute:{dto_marker}"),
        "event:bead.created.v1".to_string(),
    ]
}

#[test]
fn full_authoring_lifecycle() {
    // 1. Author declares a DTO and ships TypeScript for the frontend.
    let v1_schema = schema_of::<CreateBeadV1>();
    let ts = typescript_module([("CreateBead", &v1_schema)]);
    assert!(ts.contains("export interface CreateBead {"));
    assert!(ts.contains("title: string;"));
    assert!(ts.contains("priority: number;"));

    // 2. Freeze the contract baseline at v1.0.0.
    let v1 = ContractVersion::new(1, 0, 0);
    let items_v1 = surface("dto@v1");
    let hash_v1 = SurfaceHash::compute(items_v1.iter().map(String::as_str));
    let baseline = ContractBaseline::freeze(v1, hash_v1);

    // The frozen baseline lives in the per-major file.
    assert_eq!(version_file_name(v1), "contract.v1.json");

    // 3. CI on an unchanged surface: clean pass.
    assert_eq!(baseline.check(v1, hash_v1), DriftCheck::Unchanged);

    // 4. Author makes an additive change but forgets to bump — CI catches it.
    let items_v2 = surface("dto@v2"); // DTO grew → item string changed
    let hash_v2 = SurfaceHash::compute(items_v2.iter().map(String::as_str));
    assert_eq!(baseline.check(v1, hash_v2), DriftCheck::UnbumpedDrift);

    // 5. Bump the minor version → drift check passes...
    let v1_1 = ContractVersion::new(1, 1, 0);
    assert_eq!(baseline.check(v1_1, hash_v2), DriftCheck::BumpedAndChanged);

    // ...and semver enforcement agrees a minor bump fits an additive change.
    let added_item: Vec<&str> = vec!["tool:beads.create.execute:dto@v1", "event:bead.created.v1", "extra"];
    let base_item: Vec<&str> = vec!["tool:beads.create.execute:dto@v1", "event:bead.created.v1"];
    assert_eq!(
        enforce(v1, v1_1, base_item.clone(), added_item.clone()),
        SemverVerdict::Ok
    );
    // A mere patch bump would NOT satisfy an additive change.
    assert_eq!(
        enforce(v1, ContractVersion::new(1, 0, 1), base_item.clone(), added_item),
        SemverVerdict::MinorRequired
    );

    // 6. A breaking change (item removed) demands a major bump.
    let removed_item: Vec<&str> = vec!["tool:beads.create.execute:dto@v1"]; // event removed
    assert_eq!(
        enforce(v1, v1_1, base_item.clone(), removed_item.clone()),
        SemverVerdict::MajorRequired
    );
    assert_eq!(
        enforce(v1, ContractVersion::new(2, 0, 0), base_item, removed_item),
        SemverVerdict::Ok
    );
    // The new major gets its own frozen file.
    assert_eq!(version_file_name(ContractVersion::new(2, 0, 0)), "contract.v2.json");
}

#[test]
fn additive_dto_field_keeps_generating_valid_ts() {
    // The TS codegen tracks the DTO as it grows — the new optional field appears.
    let schema = schema_of::<CreateBeadV2Additive>();
    let ts = typescript_module([("CreateBead", &schema)]);
    assert!(ts.contains("export interface CreateBead {"), "{ts}");
    assert!(ts.contains("parent?:"), "optional field rendered optional: {ts}");
}

#[test]
fn version_compatibility_matches_semver_intent() {
    // An additive (minor) successor stays compatible; a major successor does not.
    let base = ContractVersion::new(1, 2, 0);
    assert!(ContractVersion::new(1, 3, 0).is_compatible_with(&base));
    assert!(!ContractVersion::new(2, 0, 0).is_compatible_with(&base));
}
