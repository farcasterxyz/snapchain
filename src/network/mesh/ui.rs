//! The mesh dashboard: a single self-contained HTML page (inline CSS + JS, no
//! CDN) served at `/v1/mesh/ui`. It fetches the admin-gated mesh JSON endpoints
//! (`/v1/mesh?format=json` and `?crawl=true`) using credentials entered in the
//! page, renders the local view and network topology with human-readable node
//! names, and best-effort fetches each discovered node's `/v1/info` directly
//! from the browser.

/// The dashboard page, embedded at build time.
pub const UI_HTML: &str = include_str!("ui.html");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_html_is_self_contained() {
        assert!(!UI_HTML.is_empty());
        assert!(UI_HTML.contains("<html"));
        // No external resources: a strict reading of "self-contained" — no
        // remote scripts, styles, or images that a locked-down node couldn't
        // load. (The runtime `fetch()` calls to node APIs are same-origin or
        // to operator-controlled nodes, not asset loads.)
        assert!(
            !UI_HTML.contains("src=\"http"),
            "UI must not load external scripts/images"
        );
        assert!(
            !UI_HTML.contains("href=\"http"),
            "UI must not load external stylesheets"
        );
    }
}
