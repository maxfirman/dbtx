use crate::error::{AppError, AppResult};
use serde_json::{Map, Value};
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

#[derive(Debug, Clone)]
pub struct CurrentNodeState {
    pub unique_id: String,
    pub materialized: Option<String>,
    pub relation_database: Option<String>,
    pub relation_schema: Option<String>,
    pub relation_alias: Option<String>,
    pub relation_name: Option<String>,
    pub checksum: Option<String>,
}

#[derive(Debug)]
pub struct ReconstructedManifest {
    pub temp_dir: TempDir,
}

impl ManifestSnapshot {
    pub async fn from_path(path: &Path) -> AppResult<Self> {
        let content = fs::read_to_string(path)
            .await
            .map_err(|_| AppError::MissingManifest(path.display().to_string()))?;
        let raw: Value = serde_json::from_str(&content)?;
        let nodes = extract_nodes(&raw);
        let edges = extract_edges(&raw);
        Ok(Self { raw, nodes, edges })
    }

    pub fn reconstruct(
        raw_manifest: Value,
        current_nodes: &[CurrentNodeState],
        edges: &[ManifestEdge],
    ) -> Value {
        let mut raw_manifest = raw_manifest;

        if let Some(nodes) = raw_manifest.get_mut("nodes").and_then(Value::as_object_mut) {
            for patch in current_nodes {
                let Some(node) = nodes.get_mut(&patch.unique_id).and_then(Value::as_object_mut) else {
                    continue;
                };

                patch_scalar(node, "database", patch.relation_database.as_ref());
                patch_scalar(node, "schema", patch.relation_schema.as_ref());
                patch_scalar(node, "alias", patch.relation_alias.as_ref());
                patch_scalar(node, "relation_name", patch.relation_name.as_ref());

                if let Some(materialized) = &patch.materialized {
                    let config = node
                        .entry("config".to_string())
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Some(config) = config.as_object_mut() {
                        config.insert(
                            "materialized".to_string(),
                            Value::String(materialized.clone()),
                        );
                    }
                }

                if let Some(checksum) = &patch.checksum {
                    let checksum_value = node
                        .entry("checksum".to_string())
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Some(checksum_obj) = checksum_value.as_object_mut() {
                        checksum_obj.insert(
                            "checksum".to_string(),
                            Value::String(checksum.clone()),
                        );
                    }
                }
            }
        }

        if !edges.is_empty() {
            let mut parent_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
            let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

            for edge in edges {
                parent_map
                    .entry(edge.child_unique_id.clone())
                    .or_default()
                    .push(edge.parent_unique_id.clone());
                child_map
                    .entry(edge.parent_unique_id.clone())
                    .or_default()
                    .push(edge.child_unique_id.clone());
            }

            raw_manifest["parent_map"] = json_map_from_edges(parent_map);
            raw_manifest["child_map"] = json_map_from_edges(child_map);
        }

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
}

fn extract_nodes(raw: &Value) -> Vec<ManifestNode> {
    raw.get("nodes")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|nodes| nodes.iter())
        .map(|(unique_id, node)| ManifestNode {
            unique_id: unique_id.clone(),
            resource_type: node
                .get("resource_type")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            name: node.get("name").and_then(Value::as_str).map(ToString::to_string),
            package_name: node
                .get("package_name")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            original_file_path: node
                .get("original_file_path")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            tags: node.get("tags").cloned().unwrap_or(Value::Array(Vec::new())),
            fqn: node.get("fqn").cloned().unwrap_or(Value::Array(Vec::new())),
            config: node.get("config").cloned().unwrap_or(Value::Object(Default::default())),
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
            alias: node.get("alias").and_then(Value::as_str).map(ToString::to_string),
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

fn patch_scalar(node: &mut Map<String, Value>, key: &str, value: Option<&String>) {
    if let Some(value) = value {
        node.insert(key.to_string(), Value::String(value.clone()));
    }
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
    use super::{CurrentNodeState, ManifestEdge, ManifestSnapshot};
    use serde_json::json;
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
    fn reconstruct_patches_current_state_fields() {
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

        let reconstructed = ManifestSnapshot::reconstruct(
            raw,
            &[CurrentNodeState {
                unique_id: "model.pkg.a".to_string(),
                materialized: Some("table".to_string()),
                relation_database: Some("new_db".to_string()),
                relation_schema: Some("new_schema".to_string()),
                relation_alias: Some("new_alias".to_string()),
                relation_name: Some("new.rel".to_string()),
                checksum: Some("new".to_string()),
            }],
            &[],
        );

        let node = &reconstructed["nodes"]["model.pkg.a"];
        assert_eq!(node["database"], "new_db");
        assert_eq!(node["schema"], "new_schema");
        assert_eq!(node["alias"], "new_alias");
        assert_eq!(node["relation_name"], "new.rel");
        assert_eq!(node["config"]["materialized"], "table");
        assert_eq!(node["checksum"]["checksum"], "new");
    }

    #[test]
    fn reconstruct_overwrites_edge_maps_when_edges_present() {
        let raw = json!({
            "nodes": {},
            "parent_map": {"old": ["x"]},
            "child_map": {"x": ["old"]}
        });

        let reconstructed = ManifestSnapshot::reconstruct(
            raw,
            &[],
            &[ManifestEdge {
                parent_unique_id: "model.pkg.parent".to_string(),
                child_unique_id: "model.pkg.child".to_string(),
            }],
        );

        assert_eq!(
            reconstructed["parent_map"]["model.pkg.child"],
            json!(["model.pkg.parent"])
        );
        assert_eq!(
            reconstructed["child_map"]["model.pkg.parent"],
            json!(["model.pkg.child"])
        );
    }
}
