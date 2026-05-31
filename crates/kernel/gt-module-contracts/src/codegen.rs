//! TypeScript DTO codegen from a module's JSON Schemas (`hq-mod-contracts.2`).
//!
//! A module's wire surface is declared in Rust (route DTOs, MCP tool inputs) and
//! the schema is dumped as a [`serde_json::Value`] JSON Schema — the same shape
//! `gt-module-mcp::schema_for` produces for an `inputSchema`. The TS frontend
//! consumes those DTOs, so it needs matching `interface`/`type` declarations.
//! Rather than pull a Node toolchain into the build, this module emits the
//! TypeScript directly from the JSON Schema, in Rust, deterministically.
//!
//! ## What it handles
//!
//! The subset `schemars` 0.8 emits for ordinary serde DTOs:
//!
//! - objects with `properties` → `export interface Name { ... }`,
//! - `required` membership → required field vs optional `field?`,
//! - primitives (`string` / `integer` / `number` / `boolean` / `null`),
//! - arrays (`T[]`), maps (`additionalProperties` → `Record<string, T>`),
//! - string `enum` → a string-literal union,
//! - `$ref: "#/definitions/X"` → the referenced type name `X`,
//! - `oneOf` / `anyOf` → a union (with a `| null` nullable collapse),
//! - `allOf` → an intersection (single-element `allOf` unwrapped),
//! - `definitions` → one named declaration each, deduplicated across inputs.
//!
//! Anything unrecognised degrades to `unknown` rather than failing — codegen is
//! best-effort and must never block a build on an exotic schema.
//!
//! ## Determinism
//!
//! Output is a function of the input schemas only. Named declarations are
//! collected into a sorted map before rendering, and `serde_json` (without
//! `preserve_order`) iterates object keys in sorted order, so the same surface
//! always produces byte-identical TypeScript — the property `hq-mod-contracts.3`
//! freezes into a baseline and CI diffs against.

use serde_json::Value;
use std::collections::BTreeMap;

/// Render a TypeScript module from a set of named JSON Schemas.
///
/// Each `(name, schema)` pair contributes a top-level declaration named `name`,
/// plus one declaration per entry in that schema's `definitions`. Definitions
/// that recur across inputs (a shared nested type) are emitted once; the first
/// occurrence wins. The result is a single `String` of `export` declarations
/// separated by blank lines, suitable to write to a `.d.ts` / `.ts` file.
///
/// ```
/// use serde_json::json;
/// let schema = json!({
///     "type": "object",
///     "required": ["id"],
///     "properties": { "id": { "type": "string" }, "n": { "type": "integer" } }
/// });
/// let ts = gt_module_contracts::typescript_module([("Bead", &schema)]);
/// assert!(ts.contains("export interface Bead {"));
/// assert!(ts.contains("id: string;"));
/// assert!(ts.contains("n?: number;"));
/// ```
pub fn typescript_module<'a, I>(schemas: I) -> String
where
    I: IntoIterator<Item = (&'a str, &'a Value)>,
{
    // Collect into a sorted map so declaration order is deterministic and
    // duplicate definition names collapse to a single declaration.
    let mut decls: BTreeMap<String, String> = BTreeMap::new();

    for (name, schema) in schemas {
        for (def_name, def_schema) in definitions(schema) {
            decls
                .entry(def_name.to_string())
                .or_insert_with(|| declaration(def_name, def_schema));
        }
        decls
            .entry(name.to_string())
            .or_insert_with(|| declaration(name, schema));
    }

    decls
        .into_values()
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The `definitions` (a.k.a. `$defs`) map of a root schema, or empty.
fn definitions(schema: &Value) -> impl Iterator<Item = (&str, &Value)> {
    schema
        .get("definitions")
        .or_else(|| schema.get("$defs"))
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|m| m.iter().map(|(k, v)| (k.as_str(), v)))
}

/// Render one top-level named declaration.
///
/// An object-with-properties schema becomes an `interface`; everything else
/// becomes a `type` alias over the inline type expression.
fn declaration(name: &str, schema: &Value) -> String {
    let mut out = String::new();
    if let Some(doc) = doc_comment(schema, 0) {
        out.push_str(&doc);
    }
    if is_object_with_properties(schema) {
        out.push_str(&format!("export interface {name} {}", object_body(schema)));
    } else {
        out.push_str(&format!("export type {name} = {};", ts_type(schema)));
    }
    out
}

/// Whether a schema is an object carrying an explicit `properties` map.
fn is_object_with_properties(schema: &Value) -> bool {
    schema.get("properties").and_then(Value::as_object).is_some()
}

/// The `{ ... }` body of an interface (or inline object type), including braces.
fn object_body(schema: &Value) -> String {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    let props = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    if props.is_empty() {
        // Object with no declared properties: an open record.
        return record_type(schema);
    }

    let mut lines = String::from("{\n");
    for (field, field_schema) in &props {
        if let Some(doc) = doc_comment(field_schema, 1) {
            lines.push_str(&doc);
        }
        let opt = if required.iter().any(|r| r == field) { "" } else { "?" };
        lines.push_str(&format!("  {field}{opt}: {};\n", ts_type(field_schema)));
    }
    lines.push('}');
    lines
}

/// Map an object schema with no `properties` to a `Record<...>` type.
fn record_type(schema: &Value) -> String {
    match schema.get("additionalProperties") {
        Some(Value::Bool(false)) => "Record<string, never>".to_string(),
        Some(Value::Object(_)) => {
            let v = ts_type(&schema["additionalProperties"]);
            format!("Record<string, {v}>")
        }
        _ => "Record<string, unknown>".to_string(),
    }
}

/// Render the inline TypeScript type expression for a schema.
fn ts_type(schema: &Value) -> String {
    // A bare `true`/`false` schema, or a non-object, is unconstrained.
    let Some(obj) = schema.as_object() else {
        return "unknown".to_string();
    };

    if let Some(reference) = obj.get("$ref").and_then(Value::as_str) {
        return ref_name(reference);
    }

    // String-literal (or mixed) enum → a union of literals.
    if let Some(values) = obj.get("enum").and_then(Value::as_array) {
        return enum_union(values);
    }

    // `oneOf` / `anyOf` → a union of the alternatives.
    for key in ["oneOf", "anyOf"] {
        if let Some(alts) = obj.get(key).and_then(Value::as_array) {
            return union(alts.iter().map(ts_type));
        }
    }

    // `allOf` → intersection; the common single-element case unwraps cleanly.
    if let Some(all) = obj.get("allOf").and_then(Value::as_array) {
        let parts: Vec<String> = all.iter().map(ts_type).collect();
        return match parts.len() {
            0 => "unknown".to_string(),
            1 => parts.into_iter().next().unwrap(),
            _ => parts.join(" & "),
        };
    }

    match obj.get("type") {
        Some(Value::String(t)) => scalar_or_compound(t, obj),
        // `type: ["string", "null"]` → a nullable union.
        Some(Value::Array(types)) => {
            union(types.iter().filter_map(Value::as_str).map(|t| prim(t).to_string()))
        }
        // No `type` but has `properties` → an inline object.
        _ if obj.contains_key("properties") => object_body(schema),
        _ => "unknown".to_string(),
    }
}

/// Render a single typed schema once its `type` is a known string.
fn scalar_or_compound(t: &str, obj: &serde_json::Map<String, Value>) -> String {
    match t {
        "array" => {
            let item = obj
                .get("items")
                .map(ts_type)
                .unwrap_or_else(|| "unknown".to_string());
            // Parenthesise unions so `(A | B)[]` parses as an array of the union.
            if item.contains('|') || item.contains('&') {
                format!("({item})[]")
            } else {
                format!("{item}[]")
            }
        }
        "object" => {
            if obj.contains_key("properties") {
                object_body(&Value::Object(obj.clone()))
            } else {
                record_type(&Value::Object(obj.clone()))
            }
        }
        other => prim(other).to_string(),
    }
}

/// Map a JSON Schema primitive type name to its TypeScript equivalent.
fn prim(t: &str) -> &'static str {
    match t {
        "string" => "string",
        "integer" | "number" => "number",
        "boolean" => "boolean",
        "null" => "null",
        _ => "unknown",
    }
}

/// Build a `A | B | C` union from rendered parts, deduplicated and stable.
///
/// `null` is kept as an ordinary member (it sorts last after dedup), so a
/// nullable `anyOf: [T, null]` renders as `T | null`.
fn union<I: IntoIterator<Item = String>>(parts: I) -> String {
    let mut seen: Vec<String> = Vec::new();
    for p in parts {
        if !seen.contains(&p) {
            seen.push(p);
        }
    }
    match seen.len() {
        0 => "unknown".to_string(),
        _ => seen.join(" | "),
    }
}

/// Render an `enum` array as a union of TypeScript literals.
fn enum_union(values: &[Value]) -> String {
    let parts = values.iter().map(|v| match v {
        Value::String(s) => format!("\"{s}\""),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        // Non-scalar enum members are not expressible as literals.
        _ => "unknown".to_string(),
    });
    union(parts)
}

/// Extract the type name from a local `$ref` like `#/definitions/Foo`.
fn ref_name(reference: &str) -> String {
    reference
        .rsplit('/')
        .next()
        .unwrap_or(reference)
        .to_string()
}

/// Render a `description` as a JSDoc comment at the given indent depth, if any.
fn doc_comment(schema: &Value, indent: usize) -> Option<String> {
    let desc = schema.get("description")?.as_str()?;
    if desc.is_empty() {
        return None;
    }
    let pad = "  ".repeat(indent);
    // Single-line JSDoc keeps output compact and diff-stable.
    let one_line = desc.replace('\n', " ");
    Some(format!("{pad}/** {one_line} */\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use serde_json::json;

    /// Produce the `serde_json` JSON Schema for a type, as modules would dump it.
    fn schema_of<T: JsonSchema>() -> Value {
        let root = schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>();
        serde_json::to_value(&root).unwrap()
    }

    #[test]
    fn object_required_vs_optional() {
        let schema = json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" },
                "count": { "type": "integer" }
            }
        });
        let ts = typescript_module([("Bead", &schema)]);
        assert!(ts.contains("export interface Bead {"), "{ts}");
        assert!(ts.contains("id: string;"), "{ts}");
        assert!(ts.contains("count?: number;"), "{ts}");
    }

    #[test]
    fn arrays_and_unions() {
        let schema = json!({
            "type": "object",
            "required": ["tags", "mix"],
            "properties": {
                "tags": { "type": "array", "items": { "type": "string" } },
                "mix": {
                    "type": "array",
                    "items": { "anyOf": [{ "type": "string" }, { "type": "integer" }] }
                }
            }
        });
        let ts = typescript_module([("T", &schema)]);
        assert!(ts.contains("tags: string[];"), "{ts}");
        assert!(ts.contains("mix: (string | number)[];"), "{ts}");
    }

    #[test]
    fn string_enum_becomes_literal_union() {
        let schema = json!({ "enum": ["open", "working", "closed"] });
        let ts = typescript_module([("Status", &schema)]);
        assert_eq!(
            ts,
            "export type Status = \"open\" | \"working\" | \"closed\";"
        );
    }

    #[test]
    fn ref_resolves_to_definition_name() {
        let schema = json!({
            "type": "object",
            "required": ["inner"],
            "properties": { "inner": { "$ref": "#/definitions/Inner" } },
            "definitions": {
                "Inner": {
                    "type": "object",
                    "required": ["x"],
                    "properties": { "x": { "type": "boolean" } }
                }
            }
        });
        let ts = typescript_module([("Outer", &schema)]);
        // Both the referenced definition and the root are declared.
        assert!(ts.contains("export interface Inner {"), "{ts}");
        assert!(ts.contains("x: boolean;"), "{ts}");
        assert!(ts.contains("export interface Outer {"), "{ts}");
        assert!(ts.contains("inner: Inner;"), "{ts}");
    }

    #[test]
    fn nullable_type_array_collapses_to_union() {
        let schema = json!({
            "type": "object",
            "required": ["maybe"],
            "properties": { "maybe": { "type": ["string", "null"] } }
        });
        let ts = typescript_module([("N", &schema)]);
        assert!(ts.contains("maybe: string | null;"), "{ts}");
    }

    #[test]
    fn open_record_object() {
        let schema = json!({ "type": "object", "additionalProperties": { "type": "integer" } });
        let ts = typescript_module([("Counts", &schema)]);
        assert_eq!(ts, "export type Counts = Record<string, number>;");
    }

    #[test]
    fn unknown_schema_degrades_not_fails() {
        let schema = json!({ "weird": "shape" });
        let ts = typescript_module([("X", &schema)]);
        assert_eq!(ts, "export type X = unknown;");
    }

    #[test]
    fn description_renders_as_jsdoc() {
        let schema = json!({
            "type": "object",
            "description": "A bead.",
            "required": ["id"],
            "properties": {
                "id": { "type": "string", "description": "Stable id." }
            }
        });
        let ts = typescript_module([("Bead", &schema)]);
        assert!(ts.contains("/** A bead. */"), "{ts}");
        assert!(ts.contains("  /** Stable id. */"), "{ts}");
    }

    #[test]
    fn output_is_deterministic_across_input_order() {
        let a = json!({ "type": "object", "properties": { "a": { "type": "string" } } });
        let b = json!({ "type": "object", "properties": { "b": { "type": "integer" } } });
        let one = typescript_module([("A", &a), ("B", &b)]);
        let two = typescript_module([("B", &b), ("A", &a)]);
        assert_eq!(one, two, "declaration order must not depend on input order");
    }

    #[test]
    fn shared_definition_emitted_once() {
        let inner = json!({ "type": "object", "properties": { "x": { "type": "string" } } });
        let s1 = json!({
            "type": "object",
            "properties": { "i": { "$ref": "#/definitions/Inner" } },
            "definitions": { "Inner": inner }
        });
        let s2 = json!({
            "type": "object",
            "properties": { "j": { "$ref": "#/definitions/Inner" } },
            "definitions": { "Inner": inner }
        });
        let ts = typescript_module([("First", &s1), ("Second", &s2)]);
        assert_eq!(ts.matches("export interface Inner {").count(), 1, "{ts}");
    }

    // --- End-to-end against real schemars output ---------------------------

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct CreateBead {
        title: String,
        priority: u32,
        tags: Vec<String>,
        parent: Option<String>,
    }

    #[test]
    fn end_to_end_from_schemars_derive() {
        let schema = schema_of::<CreateBead>();
        let ts = typescript_module([("CreateBead", &schema)]);
        assert!(ts.contains("export interface CreateBead {"), "{ts}");
        assert!(ts.contains("title: string;"), "{ts}");
        assert!(ts.contains("priority: number;"), "{ts}");
        assert!(ts.contains("tags: string[];"), "{ts}");
        // `Option<String>` → not required → optional field.
        assert!(ts.contains("parent?:"), "{ts}");
    }
}
