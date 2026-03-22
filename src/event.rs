use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    #[serde(default)]
    pub info: EventInfo,
    #[serde(default)]
    pub data: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub invocation_id: String,
}

#[derive(Debug, Clone)]
pub struct NormalizedNodeEvent {
    pub unique_id: String,
    pub resource_type: Option<String>,
    pub node_name: Option<String>,
    pub node_path: Option<String>,
    pub materialized: Option<String>,
    pub status: Option<String>,
    pub relation_database: Option<String>,
    pub relation_schema: Option<String>,
    pub relation_alias: Option<String>,
    pub relation_name: Option<String>,
    pub node_checksum: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub execution_time_seconds: Option<f64>,
}

impl LogEvent {
    pub fn parse(line: &str) -> Option<Self> {
        serde_json::from_str(line).ok()
    }

    pub fn normalized_node_event(&self) -> Option<NormalizedNodeEvent> {
        let node_info = self.data.get("node_info")?;
        let unique_id = node_info.get("unique_id")?.as_str()?.to_string();

        let status = self
            .data
            .get("run_result")
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| self.data.get("status").and_then(Value::as_str).map(ToString::to_string))
            .or_else(|| {
                node_info
                    .get("node_status")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            });

        let execution_time_seconds = self
            .data
            .get("run_result")
            .and_then(|value| value.get("execution_time"))
            .and_then(Value::as_f64)
            .or_else(|| self.data.get("execution_time").and_then(Value::as_f64));

        let relation = node_info.get("node_relation");

        Some(NormalizedNodeEvent {
            unique_id,
            resource_type: node_info
                .get("resource_type")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            node_name: node_info
                .get("node_name")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            node_path: node_info
                .get("node_path")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            materialized: node_info
                .get("materialized")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            status,
            relation_database: relation
                .and_then(|value| value.get("database"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
            relation_schema: relation
                .and_then(|value| value.get("schema"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
            relation_alias: relation
                .and_then(|value| value.get("alias"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
            relation_name: relation
                .and_then(|value| value.get("relation_name"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
            node_checksum: node_info
                .get("node_checksum")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            started_at: parse_timestamp(
                node_info.get("node_started_at").and_then(Value::as_str),
            ),
            finished_at: parse_timestamp(
                node_info.get("node_finished_at").and_then(Value::as_str),
            ),
            execution_time_seconds,
        })
    }
}

fn parse_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?;
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::LogEvent;

    #[test]
    fn normalizes_node_finished_event() {
        let raw = r#"{
          "info":{"name":"NodeFinished","code":"Q025","invocation_id":"abc"},
          "data":{
            "node_info":{
              "unique_id":"model.pkg.orders",
              "resource_type":"model",
              "node_name":"orders",
              "node_path":"models/orders.sql",
              "materialized":"table",
              "node_status":"success",
              "node_started_at":"2025-01-01T00:00:00Z",
              "node_finished_at":"2025-01-01T00:00:01Z",
              "node_relation":{"database":"db","schema":"analytics","alias":"orders","relation_name":"db.analytics.orders"},
              "node_checksum":"abc123"
            },
            "run_result":{"status":"success","execution_time":1.0}
          }
        }"#;

        let event = LogEvent::parse(raw).expect("event should parse");
        let normalized = event
            .normalized_node_event()
            .expect("node event should normalize");
        assert_eq!(normalized.unique_id, "model.pkg.orders");
        assert_eq!(normalized.status.as_deref(), Some("success"));
        assert_eq!(normalized.execution_time_seconds, Some(1.0));
    }
}
