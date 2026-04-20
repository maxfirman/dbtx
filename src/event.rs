//! dbt log event parsing, normalization, and terminal rendering.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::IsTerminal;

const DBTX_SELECTED_RESOURCES_PREFIX: &str = "DBTX_SELECTED_RESOURCES::";

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
    #[serde(default)]
    pub level: String,
    #[serde(default)]
    pub msg: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SelectedResourcesMarker {
    #[serde(default)]
    selected_resources: Vec<String>,
}

impl LogEvent {
    pub fn parse(line: &str) -> Option<Self> {
        serde_json::from_str(line).ok()
    }

    pub fn render_text_line(&self) -> Option<String> {
        self.render_text_line_with_color(should_use_color())
    }

    fn render_text_line_with_color(&self, use_color: bool) -> Option<String> {
        match self.info.name.as_str() {
            "Generic" => render_generic_message(&self.info.msg, use_color),
            "LogModelResult"
            | "LogSeedResult"
            | "LogSnapshotResult"
            | "LogTestResult"
            | "LogFreshnessResult"
            | "LogSnapshotResultLine" => Some(render_result_line(self, use_color)),
            "CommandCompleted" => Some(colorize_summary(&self.info.msg, use_color)),
            _ => None,
        }
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
            .or_else(|| {
                self.data
                    .get("status")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
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
            started_at: parse_timestamp(node_info.get("node_started_at").and_then(Value::as_str)),
            finished_at: parse_timestamp(node_info.get("node_finished_at").and_then(Value::as_str)),
            execution_time_seconds,
        })
    }

    pub fn selected_resources(&self) -> Option<Vec<String>> {
        let payload = self.info.msg.strip_prefix(DBTX_SELECTED_RESOURCES_PREFIX)?;
        let marker: SelectedResourcesMarker = serde_json::from_str(payload).ok()?;
        Some(marker.selected_resources)
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

fn should_use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if matches!(std::env::var("TERM").ok().as_deref(), Some("dumb")) {
        return false;
    }
    if matches!(std::env::var("CLICOLOR_FORCE").ok().as_deref(), Some("1")) {
        return true;
    }
    std::io::stdout().is_terminal()
}

fn render_generic_message(msg: &str, use_color: bool) -> Option<String> {
    if msg.is_empty() {
        return None;
    }

    if msg.starts_with("dbt-fusion ") {
        let (name, rest) = msg.split_once(' ').unwrap_or((msg, ""));
        let rendered = if rest.is_empty() {
            style(name, &[AnsiStyle::Green, AnsiStyle::Bold], use_color)
        } else {
            format!(
                "{} {}",
                style(name, &[AnsiStyle::Green, AnsiStyle::Bold], use_color),
                rest
            )
        };
        return Some(rendered);
    }

    if let Some(rest) = msg.strip_prefix("error:") {
        return Some(format!(
            "{}{}",
            style("error:", &[AnsiStyle::Red, AnsiStyle::Bold], use_color),
            rest
        ));
    }

    if msg == "Loading profiles.yml" {
        return Some(format!(
            "{} profiles.yml",
            style(
                "   Loading",
                &[AnsiStyle::Green, AnsiStyle::Bold],
                use_color
            )
        ));
    }

    if msg.starts_with(' ') || msg.starts_with('\n') {
        return Some(msg.to_string());
    }

    Some(format!("   {msg}"))
}

fn render_result_line(event: &LogEvent, use_color: bool) -> String {
    let line = event.info.msg.as_str();
    let normalized = event.normalized_node_event();
    let Some(normalized) = normalized else {
        return line.to_string();
    };

    let status = normalized
        .status
        .as_deref()
        .map(result_status_label_and_style)
        .or_else(|| detect_status_from_msg(line))
        .unwrap_or(("Succeeded", &[AnsiStyle::Green, AnsiStyle::Bold]));
    let timing = extract_bracketed_segment(line).unwrap_or("[  0.00s]");
    let resource_type = normalized.resource_type.as_deref().unwrap_or("model");
    let relation = format_relation_name(
        normalized.relation_schema.as_deref(),
        normalized.relation_alias.as_deref(),
        normalized.relation_name.as_deref(),
        normalized.node_name.as_deref(),
        use_color,
    );
    let materialized = normalized.materialized.as_deref().unwrap_or(resource_type);

    format!(
        " {} {} {} {} ({})",
        style(status.0, status.1, use_color),
        timing,
        resource_type,
        relation,
        materialized
    )
}

fn colorize_summary(msg: &str, use_color: bool) -> String {
    let mut lines = msg.lines();
    let mut rendered = Vec::new();

    if let Some(first) = lines.next() {
        rendered.push(first.to_string());
    }

    for line in lines {
        rendered.push(colorize_summary_line(line, use_color));
    }

    rendered.join("\n")
}

fn colorize_summary_line(line: &str, use_color: bool) -> String {
    if line.starts_with("===") {
        return style(line, &[AnsiStyle::Dim], use_color);
    }

    if line.starts_with("Finished '") {
        return colorize_finished_summary(line, use_color);
    }

    if let Some(rest) = line.strip_prefix("Summary: ") {
        return format!("Summary: {}", colorize_summary_counts(rest, use_color));
    }

    line.to_string()
}

fn colorize_finished_summary(line: &str, use_color: bool) -> String {
    let mut rendered = line.to_string();

    for quoted in extract_quoted_segments(line) {
        let styled = style(quoted, &[AnsiStyle::White, AnsiStyle::Bold], use_color);
        rendered = rendered.replace(&format!("'{quoted}'"), &format!("'{styled}'"));
    }

    if let Some(timing) = extract_bracketed_segment(line) {
        let styled_timing = style(timing, &[AnsiStyle::Dim], use_color);
        rendered = rendered.replace(timing, &styled_timing);
    }

    for phrase in [
        ("successfully", &[AnsiStyle::Green, AnsiStyle::Bold][..]),
        ("with 1 error", &[AnsiStyle::Red, AnsiStyle::Bold][..]),
        ("with 1 warning", &[AnsiStyle::Yellow, AnsiStyle::Bold][..]),
    ] {
        if rendered.contains(phrase.0) {
            rendered = rendered.replace(phrase.0, &style(phrase.0, phrase.1, use_color));
        }
    }

    rendered
}

fn colorize_summary_counts(input: &str, use_color: bool) -> String {
    input
        .split(" | ")
        .map(|segment| {
            if segment.ends_with(" success") || segment.ends_with(" successes") {
                style(segment, &[AnsiStyle::Green, AnsiStyle::Bold], use_color)
            } else if segment.ends_with(" error") || segment.ends_with(" errors") {
                style(segment, &[AnsiStyle::Red, AnsiStyle::Bold], use_color)
            } else if segment.ends_with(" warning")
                || segment.ends_with(" warnings")
                || segment.ends_with(" skipped")
            {
                style(segment, &[AnsiStyle::Yellow, AnsiStyle::Bold], use_color)
            } else {
                style(segment, &[AnsiStyle::White, AnsiStyle::Bold], use_color)
            }
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn format_relation_name(
    schema: Option<&str>,
    alias: Option<&str>,
    relation_name: Option<&str>,
    node_name: Option<&str>,
    use_color: bool,
) -> String {
    if let (Some(schema), Some(alias)) = (schema, alias) {
        return format!(
            "{}{}",
            style(
                &format!("{schema}."),
                &[AnsiStyle::Cyan, AnsiStyle::Bold],
                use_color
            ),
            style(alias, &[AnsiStyle::Blue, AnsiStyle::Bold], use_color)
        );
    }

    if let Some(alias) = alias.or(node_name) {
        return style(alias, &[AnsiStyle::Blue, AnsiStyle::Bold], use_color);
    }

    relation_name.unwrap_or("unknown").to_string()
}

fn extract_bracketed_segment(line: &str) -> Option<&str> {
    let start = line.find('[')?;
    let end = line[start..].find(']')?;
    Some(&line[start..=start + end])
}

fn extract_quoted_segments(line: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let mut remainder = line;

    while let Some(start) = remainder.find('\'') {
        let tail = &remainder[start + 1..];
        let Some(end) = tail.find('\'') else {
            break;
        };
        segments.push(&tail[..end]);
        remainder = &tail[end + 1..];
    }

    segments
}

fn detect_status_from_msg(line: &str) -> Option<(&'static str, &'static [AnsiStyle])> {
    for (prefix, styles) in [
        ("Succeeded", &[AnsiStyle::Green, AnsiStyle::Bold][..]),
        ("Failed", &[AnsiStyle::Red, AnsiStyle::Bold][..]),
        ("Warned", &[AnsiStyle::Yellow, AnsiStyle::Bold][..]),
        ("Skipped", &[AnsiStyle::Yellow, AnsiStyle::Bold][..]),
        ("PASS", &[AnsiStyle::Green, AnsiStyle::Bold][..]),
        ("ERROR", &[AnsiStyle::Red, AnsiStyle::Bold][..]),
    ] {
        if line.trim_start().starts_with(prefix) {
            return Some((prefix, styles));
        }
    }
    None
}

fn result_status_label_and_style(status: &str) -> (&'static str, &'static [AnsiStyle]) {
    match status {
        "success" | "pass" => ("Succeeded", &[AnsiStyle::Green, AnsiStyle::Bold]),
        "error" | "fail" | "failed" => ("Failed", &[AnsiStyle::Red, AnsiStyle::Bold]),
        "warn" | "warning" => ("Warned", &[AnsiStyle::Yellow, AnsiStyle::Bold]),
        "skipped" => ("Skipped", &[AnsiStyle::Yellow, AnsiStyle::Bold]),
        _ => ("Succeeded", &[AnsiStyle::Green, AnsiStyle::Bold]),
    }
}

fn style(input: &str, styles: &[AnsiStyle], use_color: bool) -> String {
    if !use_color {
        return input.to_string();
    }
    let codes = styles
        .iter()
        .map(|style| style.code())
        .collect::<Vec<_>>()
        .join(";");
    format!("\x1b[{codes}m{input}\x1b[0m")
}

#[derive(Clone, Copy)]
enum AnsiStyle {
    Red,
    Green,
    Yellow,
    Cyan,
    Blue,
    White,
    Bold,
    Dim,
}

impl AnsiStyle {
    fn code(self) -> &'static str {
        match self {
            Self::Red => "31",
            Self::Green => "32",
            Self::Yellow => "33",
            Self::Cyan => "36",
            Self::Blue => "34",
            Self::White => "37",
            Self::Bold => "1",
            Self::Dim => "2",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AnsiStyle, LogEvent, render_result_line, style};

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

    #[test]
    fn renders_text_line_for_loading_profiles() {
        let raw = r#"{
          "info":{"name":"Generic","msg":"Loading profiles.yml"},
          "data":{"msg":"Loading profiles.yml"}
        }"#;

        let event = LogEvent::parse(raw).expect("event should parse");
        assert_eq!(
            event.render_text_line_with_color(false).as_deref(),
            Some("   Loading profiles.yml")
        );
    }

    #[test]
    fn renders_summary_message() {
        let raw = r#"{
          "info":{"name":"CommandCompleted","msg":"\n==================== Execution Summary =====================\nFinished 'run' successfully for target 'dev' [404ms]\nProcessed: 1 model\nSummary: 1 total | 1 success"},
          "data":{"completed_at":"2026-03-22T17:28:51.094023Z","elapsed":0.40479835867881775,"success":true}
        }"#;

        let event = LogEvent::parse(raw).expect("event should parse");
        assert!(
            event
                .render_text_line_with_color(false)
                .expect("summary should render")
                .contains("Finished 'run' successfully")
        );
    }

    #[test]
    fn suppresses_debug_events() {
        let raw = r#"{
          "info":{"name":"NodeStart","msg":"Began running node model.pkg.orders"},
          "data":{"node_info":{"unique_id":"model.pkg.orders"}}
        }"#;

        let event = LogEvent::parse(raw).expect("event should parse");
        assert!(event.render_text_line_with_color(false).is_none());
    }

    #[test]
    fn colorizes_result_status_when_enabled() {
        let raw = r#"{
          "info":{"name":"LogModelResult","msg":" Succeeded [  0.07s] model main.stg_customers (view)"},
          "data":{"execution_time":0.07,"status":"success","node_info":{"unique_id":"model.pkg.stg_customers","resource_type":"model","node_name":"stg_customers","materialized":"view","node_relation":{"schema":"main","alias":"stg_customers"}}}
        }"#;
        let event = LogEvent::parse(raw).expect("event should parse");
        let rendered = render_result_line(&event, true);
        assert!(rendered.contains("\u{1b}[32;1mSucceeded\u{1b}[0m"));
        assert!(rendered.contains("\u{1b}[36;1mmain.\u{1b}[0m"));
        assert!(rendered.contains("\u{1b}[34;1mstg_customers\u{1b}[0m"));
    }

    #[test]
    fn color_helper_is_noop_when_disabled() {
        assert_eq!(
            style("Succeeded", &[AnsiStyle::Green, AnsiStyle::Bold], false),
            "Succeeded"
        );
    }

    #[test]
    fn parses_selected_resources_marker() {
        let raw = r#"{
          "info":{"name":"Generic","msg":"DBTX_SELECTED_RESOURCES::{\"selected_resources\":[\"model.pkg.orders\",\"seed.pkg.customers\"]}"},
          "data":{}
        }"#;

        let event = LogEvent::parse(raw).expect("event should parse");
        let selected = event.selected_resources().expect("selected resources marker");
        assert_eq!(selected, vec!["model.pkg.orders", "seed.pkg.customers"]);
    }

    #[test]
    fn render_text_line_generic_version() {
        let raw = r#"{"info":{"name":"Generic","msg":"dbt-fusion 2.0.0"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert!(rendered.contains("dbt-fusion"));
    }

    #[test]
    fn render_text_line_generic_error() {
        let raw = r#"{"info":{"name":"Generic","msg":"error: something went wrong"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert!(rendered.contains("error:"));
    }

    #[test]
    fn render_text_line_generic_loading_profiles() {
        let raw = r#"{"info":{"name":"Generic","msg":"Loading profiles.yml"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert!(rendered.contains("Loading"));
        assert!(rendered.contains("profiles.yml"));
    }

    #[test]
    fn render_text_line_generic_indented() {
        let raw = r#"{"info":{"name":"Generic","msg":" some indented text"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert_eq!(rendered, " some indented text");
    }

    #[test]
    fn render_text_line_generic_plain() {
        let raw = r#"{"info":{"name":"Generic","msg":"plain message"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert_eq!(rendered, "   plain message");
    }

    #[test]
    fn render_text_line_empty_generic_returns_none() {
        let raw = r#"{"info":{"name":"Generic","msg":""},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        assert!(event.render_text_line_with_color(false).is_none());
    }

    #[test]
    fn render_text_line_command_completed() {
        let raw = r#"{"info":{"name":"CommandCompleted","msg":"Finished 'run' successfully"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        let rendered = event.render_text_line_with_color(false).unwrap();
        assert!(rendered.contains("Finished"));
    }

    #[test]
    fn render_text_line_unknown_event_returns_none() {
        let raw = r#"{"info":{"name":"SomeUnknownEvent","msg":"hello"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        assert!(event.render_text_line_with_color(false).is_none());
    }

    #[test]
    fn parse_returns_none_for_invalid_json() {
        assert!(LogEvent::parse("not json").is_none());
        assert!(LogEvent::parse("").is_none());
    }

    #[test]
    fn selected_resources_returns_none_for_non_marker() {
        let raw = r#"{"info":{"name":"Generic","msg":"just a message"},"data":{}}"#;
        let event = LogEvent::parse(raw).unwrap();
        assert!(event.selected_resources().is_none());
    }

    #[test]
    fn parse_timestamp_handles_valid_and_invalid() {
        use super::parse_timestamp;
        assert!(parse_timestamp(Some("2025-01-01T00:00:00Z")).is_some());
        assert!(parse_timestamp(Some("not-a-date")).is_none());
        assert!(parse_timestamp(Some("")).is_none());
        assert!(parse_timestamp(None).is_none());
    }
}
