use axum::response::IntoResponse;

pub(super) const LINEAGE_JS: &str = include_str!("../../lineage-ui/dist/lineage.js");
pub(super) const LINEAGE_CSS: &str = include_str!("../../lineage-ui/dist/lineage.css");
pub(super) const TIMELINE_JS: &str = include_str!("../../timeline-ui/dist/timeline.js");
pub(super) const TIMELINE_CSS: &str = include_str!("../../timeline-ui/dist/timeline.css");

pub(super) async fn lineage_js_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        LINEAGE_JS,
    )
}

pub(super) async fn lineage_css_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css")],
        LINEAGE_CSS,
    )
}

pub(super) async fn timeline_js_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        TIMELINE_JS,
    )
}

pub(super) async fn timeline_css_asset() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css")],
        TIMELINE_CSS,
    )
}
