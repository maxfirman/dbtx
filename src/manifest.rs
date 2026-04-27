//! dbt manifest parsing, node/edge extraction, and reconstructed state management.
use crate::error::{AppError, AppResult};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;
use tokio::fs;

#[derive(Debug)]
pub struct ManifestSnapshot {
    pub raw: Value,
    pub nodes: Vec<ManifestNode>,
    pub edges: Vec<ManifestEdge>,
}

#[derive(Debug)]
pub struct ManifestNode {
    pub unique_id: String,
    pub resource_type: Option<String>,
    pub name: Option<String>,
    pub package_name: Option<String>,
    pub original_file_path: Option<String>,
    pub tags: Value,
    pub fqn: Value,
    pub config: Value,
    pub checksum: Option<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub alias: Option<String>,
    pub relation_name: Option<String>,
}

#[derive(Debug)]
pub struct ManifestEdge {
    pub parent_unique_id: String,
    pub child_unique_id: String,
}

#[derive(Debug)]
pub struct ReconstructedManifest {
    pub temp_dir: TempDir,
}

impl ManifestSnapshot {
    pub fn from_raw(raw: Value) -> Self {
        let nodes = extract_nodes(&raw);
        let edges = extract_edges(&raw);
        Self { raw, nodes, edges }
    }

    pub async fn from_path(path: &Path) -> AppResult<Self> {
        let content = fs::read_to_string(path)
            .await
            .map_err(|_| AppError::MissingManifest(path.display().to_string()))?;
        let raw: Value = serde_json::from_str(&content)?;
        Ok(Self::from_raw(raw))
    }

    pub fn reconstruct(raw_manifest: Value, successful_nodes: &BTreeMap<String, Value>) -> Value {
        let mut raw_manifest = raw_manifest;

        if let Some(nodes) = raw_manifest.get_mut("nodes").and_then(Value::as_object_mut) {
            for (unique_id, raw_node) in successful_nodes {
                if nodes.contains_key(unique_id) {
                    nodes.insert(unique_id.clone(), raw_node.clone());
                }
            }
        }

        let (parent_map, child_map) = rebuild_dependency_maps(&raw_manifest);
        raw_manifest["parent_map"] = json_map_from_edges(parent_map);
        raw_manifest["child_map"] = json_map_from_edges(child_map);

        raw_manifest
    }
}

impl ReconstructedManifest {
    pub async fn write(manifest: &Value) -> AppResult<Self> {
        let temp_dir = TempDir::new().map_err(AppError::Io)?;
        let path = temp_dir.path().join("manifest.json");
        fs::write(path, serde_json::to_vec(manifest)?).await?;
        Ok(Self { temp_dir })
    }

    pub async fn write_empty_state(project_name: &str, adapter_type: &str) -> AppResult<Self> {
        let manifest = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "dbt_version": "0.0.0",
                "generated_at": "1970-01-01T00:00:00Z",
                "invocation_id": "00000000-0000-0000-0000-000000000000",
                "invocation_started_at": null,
                "env": {},
                "project_name": project_name,
                "project_id": "dbtx-empty-state",
                "user_id": null,
                "send_anonymous_usage_stats": null,
                "adapter_type": adapter_type,
                "quoting": null
            },
            "nodes": {},
            "sources": {},
            "macros": {},
            "docs": {},
            "exposures": {},
            "groups": {},
            "group_map": {},
            "metrics": {},
            "selectors": {},
            "semantic_models": {},
            "saved_queries": {},
            "unit_tests": {},
            "disabled": {},
            "parent_map": {},
            "child_map": {},
            "functions": {}
        });
        Self::write(&manifest).await
    }
}

fn extract_nodes(raw: &Value) -> Vec<ManifestNode> {
    ["nodes", "sources"]
        .iter()
        .flat_map(|section| {
            raw.get(section)
                .and_then(Value::as_object)
                .into_iter()
                .flat_map(|nodes| nodes.iter())
        })
        .map(|(unique_id, node)| ManifestNode {
            unique_id: unique_id.clone(),
            resource_type: node
                .get("resource_type")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            name: node
                .get("name")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            package_name: node
                .get("package_name")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            original_file_path: node
                .get("original_file_path")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            tags: node
                .get("tags")
                .cloned()
                .unwrap_or(Value::Array(Vec::new())),
            fqn: node.get("fqn").cloned().unwrap_or(Value::Array(Vec::new())),
            config: node
                .get("config")
                .cloned()
                .unwrap_or(Value::Object(Default::default())),
            checksum: node
                .get("checksum")
                .and_then(|value| value.get("checksum"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
            database_name: node
                .get("database")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            schema_name: node
                .get("schema")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            alias: node
                .get("alias")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            relation_name: node
                .get("relation_name")
                .and_then(Value::as_str)
                .map(ToString::to_string),
        })
        .collect()
}

fn extract_edges(raw: &Value) -> Vec<ManifestEdge> {
    let Some(parent_map) = raw.get("parent_map").and_then(Value::as_object) else {
        return Vec::new();
    };

    let mut edges = Vec::new();
    for (child, parents) in parent_map {
        if let Some(parents) = parents.as_array() {
            for parent in parents.iter().filter_map(Value::as_str) {
                edges.push(ManifestEdge {
                    parent_unique_id: parent.to_string(),
                    child_unique_id: child.clone(),
                });
            }
        }
    }
    edges
}

fn rebuild_dependency_maps(
    raw_manifest: &Value,
) -> (BTreeMap<String, Vec<String>>, BTreeMap<String, Vec<String>>) {
    let mut parent_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for section in [
        "nodes",
        "sources",
        "exposures",
        "metrics",
        "semantic_models",
        "saved_queries",
        "unit_tests",
    ] {
        let Some(entries) = raw_manifest.get(section).and_then(Value::as_object) else {
            continue;
        };

        for (unique_id, entry) in entries {
            let parents = entry
                .get("depends_on")
                .and_then(|depends_on| depends_on.get("nodes"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            parent_map.insert(unique_id.clone(), parents.clone());
            child_map.entry(unique_id.clone()).or_default();

            for parent in parents {
                child_map.entry(parent).or_default().push(unique_id.clone());
            }
        }
    }

    (parent_map, child_map)
}

fn json_map_from_edges(map: BTreeMap<String, Vec<String>>) -> Value {
    Value::Object(
        map.into_iter()
            .map(|(key, values)| {
                (
                    key,
                    Value::Array(values.into_iter().map(Value::String).collect()),
                )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::ManifestSnapshot;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn parses_manifest_nodes_and_edges() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("manifest.json");
        tokio::fs::write(
            &path,
            r#"{
              "nodes": {
                "model.pkg.a": {
                  "resource_type": "model",
                  "name": "a",
                  "package_name": "pkg",
                  "original_file_path": "models/a.sql",
                  "tags": ["x"],
                  "fqn": ["pkg", "a"],
                  "config": {"materialized":"table"},
                  "checksum": {"checksum": "abc"}
                }
              },
              "parent_map": {
                "model.pkg.a": ["model.pkg.b"]
              }
            }"#,
        )
        .await
        .expect("manifest");

        let snapshot = ManifestSnapshot::from_path(&path).await.expect("snapshot");
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.edges.len(), 1);
    }

    #[test]
    fn reconstruct_preserves_base_node_without_promoted_override() {
        let raw = json!({
            "nodes": {
                "model.pkg.a": {
                    "alias": "old_alias",
                    "database": "old_db",
                    "schema": "old_schema",
                    "relation_name": "old.rel",
                    "config": {"materialized":"view"},
                    "checksum": {"checksum":"old"}
                }
            },
            "parent_map": {},
            "child_map": {}
        });

        let reconstructed = ManifestSnapshot::reconstruct(raw, &BTreeMap::new());

        let node = &reconstructed["nodes"]["model.pkg.a"];
        assert_eq!(node["database"], "old_db");
        assert_eq!(node["schema"], "old_schema");
        assert_eq!(node["alias"], "old_alias");
        assert_eq!(node["relation_name"], "old.rel");
        assert_eq!(node["config"]["materialized"], "view");
        assert_eq!(node["checksum"]["checksum"], "old");
    }

    #[test]
    fn reconstruct_overwrites_edge_maps_when_edges_present() {
        let raw = json!({
            "nodes": {
                "model.pkg.child": {
                    "depends_on": {
                        "nodes": ["model.pkg.parent"]
                    }
                },
                "model.pkg.parent": {
                    "depends_on": {
                        "nodes": []
                    }
                }
            },
            "parent_map": {"old": ["x"]},
            "child_map": {"x": ["old"]}
        });

        let reconstructed = ManifestSnapshot::reconstruct(raw, &BTreeMap::new());

        assert_eq!(
            reconstructed["parent_map"]["model.pkg.child"],
            json!(["model.pkg.parent"])
        );
        assert_eq!(
            reconstructed["child_map"]["model.pkg.parent"],
            json!(["model.pkg.child"])
        );
    }

    #[test]
    fn reconstruct_preserves_source_entries_in_dependency_maps() {
        let raw = json!({
            "nodes": {
                "model.pkg.child": {
                    "depends_on": {
                        "nodes": ["source.pkg.raw"]
                    }
                }
            },
            "sources": {
                "source.pkg.raw": {
                    "depends_on": {
                        "nodes": []
                    }
                }
            },
            "parent_map": {"old": ["x"]},
            "child_map": {"x": ["old"]}
        });

        let reconstructed = ManifestSnapshot::reconstruct(raw, &BTreeMap::new());

        assert_eq!(reconstructed["parent_map"]["source.pkg.raw"], json!([]));
        assert_eq!(
            reconstructed["child_map"]["source.pkg.raw"],
            json!(["model.pkg.child"])
        );
    }

    #[test]
    fn reconstruct_replaces_node_with_last_successful_version() {
        let raw = json!({
            "nodes": {
                "model.pkg.a": {
                    "raw_code": "new code",
                    "checksum": {"checksum":"new"}
                }
            }
        });

        let successful_nodes = BTreeMap::from([(
            "model.pkg.a".to_string(),
            json!({
                "raw_code": "old code",
                "checksum": {"checksum":"old"}
            }),
        )]);

        let reconstructed = ManifestSnapshot::reconstruct(raw, &successful_nodes);
        assert_eq!(
            reconstructed["nodes"]["model.pkg.a"]["raw_code"],
            "old code"
        );
        assert_eq!(
            reconstructed["nodes"]["model.pkg.a"]["checksum"]["checksum"],
            "old"
        );
    }

    #[test]
    fn extract_nodes_includes_sources() {
        let raw = json!({
            "nodes": {
                "model.pkg.orders": {
                    "resource_type": "model",
                    "name": "orders",
                    "package_name": "pkg"
                }
            },
            "sources": {
                "source.pkg.raw_orders": {
                    "resource_type": "source",
                    "name": "raw_orders",
                    "package_name": "pkg"
                }
            },
            "parent_map": {
                "model.pkg.orders": ["source.pkg.raw_orders"]
            }
        });

        let snapshot = ManifestSnapshot::from_raw(raw);
        assert_eq!(snapshot.nodes.len(), 2);
        let source = snapshot
            .nodes
            .iter()
            .find(|n| n.unique_id == "source.pkg.raw_orders");
        assert!(source.is_some(), "source node should be extracted");
        assert_eq!(source.unwrap().resource_type.as_deref(), Some("source"));
    }
}
