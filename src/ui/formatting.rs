//! Pure formatting and rendering helpers for the operator UI.
//!
//! These functions have no dependency on axum, AppState, or view structs.
//! They are independently unit-testable.

use crate::api::{InvocationExecutionModeApi, InvocationStatusResponse};
use crate::db::PlanStatus;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet, VecDeque};

pub(super) fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            let _ = chars.next();
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            output.push(ch);
        }
    }
    output
}

pub(super) fn escape_html(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&#39;"),
            _ => output.push(ch),
        }
    }
    output
}

fn consume_identifier(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_alphanumeric() && byte != b'_' {
            break;
        }
        index += 1;
    }
    index
}

pub(super) fn style_relation_tokens(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::new();
    let mut index = 0;
    while index < bytes.len() {
        let start = index;
        let left_end = consume_identifier(bytes, index);
        if left_end > start && left_end < bytes.len() && bytes[left_end] == b'.' {
            let right_start = left_end + 1;
            let right_end = consume_identifier(bytes, right_start);
            if right_end > right_start {
                output.push_str("<span class=\"font-semibold text-cyan-700\">");
                output.push_str(&escape_html(&input[start..left_end]));
                output.push_str(".</span><span class=\"font-semibold text-blue-700\">");
                output.push_str(&escape_html(&input[right_start..right_end]));
                output.push_str("</span>");
                index = right_end;
                continue;
            }
        }
        let ch = input[index..].chars().next().expect("char boundary");
        output.push_str(&escape_html(&ch.to_string()));
        index += ch.len_utf8();
    }
    output
}

pub(super) fn style_bracket_segments(input: &str) -> String {
    let mut output = String::new();
    let mut remaining = input;
    while let Some(start) = remaining.find('[') {
        let (before, tail) = remaining.split_at(start);
        output.push_str(&style_relation_tokens(before));
        if let Some(end) = tail.find(']') {
            let (segment, rest) = tail.split_at(end + 1);
            output.push_str("<span class=\"text-slate-400\">");
            output.push_str(&escape_html(segment));
            output.push_str("</span>");
            remaining = rest;
        } else {
            output.push_str(&style_relation_tokens(tail));
            remaining = "";
        }
    }
    output.push_str(&style_relation_tokens(remaining));
    output
}

pub(super) fn render_cli_like_log_html(text: &str) -> String {
    let text = strip_ansi(text);
    let trimmed = text.trim_end();
    if trimmed.is_empty() {
        return String::new();
    }

    if let Some(rest) = trimmed.strip_prefix("dbt-fusion ") {
        return format!(
            "<span class=\"font-semibold text-emerald-700\">dbt-fusion</span> {}",
            style_bracket_segments(rest)
        );
    }

    for (prefix, class_name) in [
        ("Succeeded", "font-semibold text-emerald-700"),
        ("Failed", "font-semibold text-rose-700"),
        ("Warned", "font-semibold text-amber-700"),
        ("Skipped", "font-semibold text-amber-700"),
        ("PASS", "font-semibold text-emerald-700"),
        ("ERROR", "font-semibold text-rose-700"),
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            return format!(
                "<span class=\"{class_name}\">{}</span>{}",
                escape_html(prefix),
                style_bracket_segments(rest)
            );
        }
    }

    style_bracket_segments(trimmed)
}

pub(super) fn render_invocation_log_html(event: &crate::api::InvocationEvent) -> Option<String> {
    let text = event.text.as_deref()?;
    if text.trim().is_empty() {
        return None;
    }
    let rendered = match event.event_type.as_str() {
        "dbt.log" => render_cli_like_log_html(text),
        _ => escape_html(&strip_ansi(text)),
    };
    if rendered.is_empty() {
        None
    } else {
        Some(rendered)
    }
}

pub(super) fn fmt_ts(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

pub(super) fn fmt_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value.map(fmt_ts).unwrap_or_default()
}

pub(super) fn plan_status_class(status: PlanStatus) -> &'static str {
    match status {
        PlanStatus::Planned => "bg-amber-100 text-amber-800",
        PlanStatus::Blocked => "bg-orange-100 text-orange-800",
        PlanStatus::Admitted => "bg-sky-100 text-sky-800",
        PlanStatus::Completed => "bg-emerald-100 text-emerald-800",
        PlanStatus::Failed | PlanStatus::Canceled => "bg-rose-100 text-rose-800",
        PlanStatus::Superseded => "bg-slate-100 text-slate-700",
    }
}

pub(super) fn invocation_display_status(invocation: &InvocationStatusResponse) -> &'static str {
    match invocation.status {
        crate::api::InvocationLifecycleStatus::Running if invocation.claimed_by.is_none() => {
            "queued"
        }
        crate::api::InvocationLifecycleStatus::Running
            if !matches!(
                invocation.cancel_state,
                crate::api::InvocationCancelStateApi::None
            ) =>
        {
            "cancelling"
        }
        crate::api::InvocationLifecycleStatus::Running => "running",
        crate::api::InvocationLifecycleStatus::Succeeded => "succeeded",
        crate::api::InvocationLifecycleStatus::Failed => "failed",
        crate::api::InvocationLifecycleStatus::Canceled => "canceled",
    }
}

pub(super) fn invocation_mode_value(value: &InvocationExecutionModeApi) -> &'static str {
    match value {
        InvocationExecutionModeApi::Server => "server",
        InvocationExecutionModeApi::Local => "local",
    }
}

pub(super) fn status_badge_class(status: &str) -> &'static str {
    match status {
        "queued" => "bg-amber-100 text-amber-800",
        "running" => "bg-sky-100 text-sky-800",
        "cancelling" => "bg-orange-100 text-orange-800",
        "succeeded" => "bg-emerald-100 text-emerald-800",
        "failed" => "bg-rose-100 text-rose-800",
        "canceled" => "bg-slate-200 text-slate-700",
        "claimed" => "bg-sky-100 text-sky-800",
        "idle" => "bg-slate-100 text-slate-700",
        "stale" => "bg-orange-100 text-orange-800",
        _ => "bg-slate-100 text-slate-700",
    }
}

pub(super) fn topo_sort_resources(
    resource_ids: &[&str],
    edges: &[(String, String)],
) -> Vec<String> {
    let id_set: HashSet<&str> = resource_ids.iter().copied().collect();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for uid in &id_set {
        in_degree.entry(uid).or_insert(0);
    }

    for (parent, child) in edges {
        if id_set.contains(parent.as_str()) && id_set.contains(child.as_str()) {
            adj.entry(parent.as_str()).or_default().push(child.as_str());
            *in_degree.entry(child.as_str()).or_insert(0) += 1;
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&id, _)| id)
        .collect();
    let mut initial: Vec<&str> = queue.drain(..).collect();
    initial.sort();
    queue.extend(initial);

    let mut result = Vec::new();
    while let Some(node) = queue.pop_front() {
        result.push(node.to_string());
        if let Some(children) = adj.get(node) {
            let mut next = Vec::new();
            for &child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        next.push(child);
                    }
                }
            }
            next.sort();
            queue.extend(next);
        }
    }

    for uid in resource_ids {
        if !result.iter().any(|r| r == uid) {
            result.push(uid.to_string());
        }
    }

    result
}

pub(super) fn highlight_sql(code: &str) -> String {
    use syntect::highlighting::ThemeSet;
    use syntect::html::highlighted_html_for_string;
    use syntect::parsing::SyntaxSet;

    let ss = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let syntax = ss
        .find_syntax_by_extension("sql")
        .unwrap_or_else(|| ss.find_syntax_plain_text());
    let theme = &ts.themes["InspiredGitHub"];
    let inner = match highlighted_html_for_string(code, &ss, syntax, theme) {
        Ok(html) => html
            .strip_prefix("<pre style=\"")
            .and_then(|s| s.find("\">").map(|i| &s[i + 2..]))
            .and_then(|s| {
                s.strip_suffix("</pre>\n")
                    .or_else(|| s.strip_suffix("</pre>"))
            })
            .unwrap_or(&html)
            .to_string(),
        Err(_) => code
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;"),
    };
    let lines: Vec<&str> = inner.split('\n').collect();
    let count = if lines.last() == Some(&"") {
        lines.len() - 1
    } else {
        lines.len()
    };
    let mut out = String::from("<table class=\"w-full border-collapse\"><tbody>");
    for (i, line) in lines.iter().enumerate().take(count) {
        let num = i + 1;
        out.push_str(&format!(
            "<tr><td class=\"select-none pr-4 text-right align-top text-slate-300\" style=\"width:1%;white-space:nowrap;\">{num}</td><td><pre class=\"m-0 p-0\" style=\"background:transparent;\">{line}</pre></td></tr>"
        ));
    }
    out.push_str("</tbody></table>");
    out
}

pub(super) struct DiffLineView {
    pub kind: String,
    pub text: String,
}

pub(super) fn compute_diff(old: &str, new: &str) -> Vec<DiffLineView> {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();
    let mut result = Vec::new();
    let max = old_lines.len().max(new_lines.len());
    let mut oi = 0;
    let mut ni = 0;
    while oi < old_lines.len() || ni < new_lines.len() {
        if oi < old_lines.len() && ni < new_lines.len() && old_lines[oi] == new_lines[ni] {
            result.push(DiffLineView {
                kind: "same".into(),
                text: old_lines[oi].to_string(),
            });
            oi += 1;
            ni += 1;
        } else if oi < old_lines.len()
            && (ni >= new_lines.len()
                || (ni + 1 < new_lines.len() && new_lines[ni + 1..].contains(&old_lines[oi])))
        {
            if ni < new_lines.len() && !old_lines[oi..].contains(&new_lines[ni]) {
                result.push(DiffLineView {
                    kind: "add".into(),
                    text: new_lines[ni].to_string(),
                });
                ni += 1;
            } else {
                result.push(DiffLineView {
                    kind: "remove".into(),
                    text: old_lines[oi].to_string(),
                });
                oi += 1;
            }
        } else if ni < new_lines.len() {
            result.push(DiffLineView {
                kind: "add".into(),
                text: new_lines[ni].to_string(),
            });
            ni += 1;
        } else {
            result.push(DiffLineView {
                kind: "remove".into(),
                text: old_lines[oi].to_string(),
            });
            oi += 1;
        }
        if result.len() > max + 100 {
            break;
        }
    }
    let _ = max;
    result
}

pub(super) fn fmt_duration(seconds: Option<f64>) -> String {
    match seconds {
        Some(s) if s >= 60.0 => format!("{:.0}m {:.0}s", s / 60.0, s % 60.0),
        Some(s) => format!("{:.1}s", s),
        None => String::new(),
    }
}

pub(super) fn fmt_opt_time(t: Option<chrono::DateTime<Utc>>) -> String {
    t.map(|t| t.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default()
}

pub(super) fn short_hash(s: &str) -> String {
    if s.len() > 8 {
        s[..8].to_string()
    } else {
        s.to_string()
    }
}

pub(super) fn short_commit_sha(sha: &str) -> &str {
    &sha[..sha.len().min(8)]
}

pub(super) fn git_commit_url(repo_url: Option<&str>, sha: Option<&str>) -> String {
    match (repo_url, sha) {
        (Some(url), Some(sha)) if !url.is_empty() && !sha.is_empty() => {
            let base = url.trim_end_matches(".git");
            format!("{base}/commit/{sha}")
        }
        _ => String::new(),
    }
}

pub(super) fn resource_type_from_unique_id(unique_id: &str) -> &str {
    if unique_id.starts_with("source.") {
        "source"
    } else if unique_id.starts_with("seed.") {
        "seed"
    } else if unique_id.starts_with("test.") {
        "test"
    } else if unique_id.starts_with("snapshot.") {
        "snapshot"
    } else {
        "model"
    }
}

pub(super) fn model_status_class(status: &str) -> &'static str {
    use crate::db::NodeExecutionStatus;
    match NodeExecutionStatus::parse(status) {
        Some(NodeExecutionStatus::Success | NodeExecutionStatus::Pass) => {
            "bg-emerald-100 text-emerald-800"
        }
        Some(NodeExecutionStatus::Error | NodeExecutionStatus::Fail) => "bg-rose-100 text-rose-800",
        _ => "bg-slate-100 text-slate-600",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_ansi_removes_escape_sequences() {
        assert_eq!(strip_ansi("\x1b[32mhello\x1b[0m"), "hello");
        assert_eq!(strip_ansi("no escapes"), "no escapes");
        assert_eq!(strip_ansi("\x1b[1;31mred\x1b[0m plain"), "red plain");
    }

    #[test]
    fn escape_html_escapes_special_chars() {
        assert_eq!(escape_html("<b>hi</b>"), "&lt;b&gt;hi&lt;/b&gt;");
        assert_eq!(escape_html("a & b"), "a &amp; b");
        assert_eq!(escape_html("\"quoted\""), "&quot;quoted&quot;");
    }

    #[test]
    fn topo_sort_orders_parents_before_children() {
        let ids = vec!["model.pkg.a", "model.pkg.b", "model.pkg.c"];
        let edges = vec![
            ("model.pkg.a".to_string(), "model.pkg.b".to_string()),
            ("model.pkg.b".to_string(), "model.pkg.c".to_string()),
        ];
        let sorted = topo_sort_resources(&ids, &edges);
        assert_eq!(sorted, vec!["model.pkg.a", "model.pkg.b", "model.pkg.c"]);
    }

    #[test]
    fn topo_sort_handles_no_edges() {
        let ids = vec!["model.pkg.b", "model.pkg.a"];
        let edges: Vec<(String, String)> = vec![];
        let sorted = topo_sort_resources(&ids, &edges);
        assert_eq!(sorted, vec!["model.pkg.a", "model.pkg.b"]);
    }

    #[test]
    fn topo_sort_ignores_edges_outside_resource_set() {
        let ids = vec!["model.pkg.b", "model.pkg.c"];
        let edges = vec![
            ("model.pkg.a".to_string(), "model.pkg.b".to_string()),
            ("model.pkg.b".to_string(), "model.pkg.c".to_string()),
        ];
        let sorted = topo_sort_resources(&ids, &edges);
        assert_eq!(sorted, vec!["model.pkg.b", "model.pkg.c"]);
    }

    #[test]
    fn resource_type_from_unique_id_maps_all_types() {
        assert_eq!(resource_type_from_unique_id("model.pkg.orders"), "model");
        assert_eq!(resource_type_from_unique_id("source.pkg.raw"), "source");
        assert_eq!(resource_type_from_unique_id("seed.pkg.data"), "seed");
        assert_eq!(resource_type_from_unique_id("test.pkg.not_null"), "test");
        assert_eq!(resource_type_from_unique_id("snapshot.pkg.snap"), "snapshot");
        assert_eq!(resource_type_from_unique_id("unknown.pkg.x"), "model");
        assert_eq!(resource_type_from_unique_id(""), "model");
    }

    #[test]
    fn fmt_duration_formats_correctly() {
        assert_eq!(fmt_duration(Some(0.5)), "0.5s");
        assert_eq!(fmt_duration(Some(5.0)), "5.0s");
        assert_eq!(fmt_duration(Some(90.0)), "2m 30s");
        assert_eq!(fmt_duration(None), "");
    }

    #[test]
    fn compute_diff_detects_additions_and_removals() {
        let diff = compute_diff("a\nb\nc", "a\nx\nb\nc");
        let kinds: Vec<&str> = diff.iter().map(|d| d.kind.as_str()).collect();
        assert!(kinds.contains(&"add"));
        assert!(kinds.contains(&"same"));
    }

    #[test]
    fn render_cli_like_log_html_styles_dbt_fusion_prefix() {
        let html = render_cli_like_log_html("dbt-fusion [info] running model");
        assert!(html.contains("text-emerald-700"));
        assert!(html.contains("dbt-fusion"));
    }

    #[test]
    fn render_cli_like_log_html_styles_status_prefixes() {
        let html = render_cli_like_log_html("Succeeded [model.pkg.orders]");
        assert!(html.contains("text-emerald-700"));

        let html = render_cli_like_log_html("Failed [model.pkg.orders]");
        assert!(html.contains("text-rose-700"));
    }

    #[test]
    fn invocation_display_status_maps_correctly() {
        use crate::api::{
            InvocationCancelStateApi, InvocationLifecycleStatus, InvocationWorkerHealthApi,
        };

        let base = InvocationStatusResponse {
            invocation_id: uuid::Uuid::nil(),
            status: InvocationLifecycleStatus::Running,
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            worker_health: InvocationWorkerHealthApi::Unclaimed,
            cancel_state: InvocationCancelStateApi::None,
            claimed_at: None,
            claimed_by: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
            started_at: chrono::Utc::now(),
            completed_at: None,
            cancel_requested: false,
            exit_code: None,
            error: None,
        };

        assert_eq!(invocation_display_status(&base), "queued");

        let mut running = base.clone();
        running.claimed_by = Some("w1".to_string());
        assert_eq!(invocation_display_status(&running), "running");

        let mut cancelling = running.clone();
        cancelling.cancel_state = InvocationCancelStateApi::Requested;
        assert_eq!(invocation_display_status(&cancelling), "cancelling");
    }
}
