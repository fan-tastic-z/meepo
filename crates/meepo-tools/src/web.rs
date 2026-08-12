//! Web tools: fetch URLs and search the web.

use async_trait::async_trait;
use regex::Regex;
use serde_json::{json, Value};

use crate::{Tool, ToolError};

const MAX_FETCH_BYTES: usize = 64_000;
const FETCH_TIMEOUT_SECS: u64 = 15;

/// `web_fetch(url)` — fetch a URL and return its text content.
/// HTML pages are converted to plain text (tags stripped, entities decoded).
pub struct WebFetch;

#[async_trait]
impl Tool for WebFetch {
    fn name(&self) -> &str {
        "web_fetch"
    }

    fn description(&self) -> &str {
        "Fetch a URL and return its text content. HTML pages are converted to readable text. Useful for reading documentation, APIs, articles."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The URL to fetch (must include http:// or https://)." }
            },
            "required": ["url"]
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let url = args.get("url").and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'url'".into()))?;
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(ToolError::BadArgs("url must start with http:// or https://".into()));
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(FETCH_TIMEOUT_SECS))
            .redirect(reqwest::redirect::Policy::limited(5))
            .user_agent("Meepo/0.1")
            .build()
            .map_err(|e| ToolError::Other(format!("http client: {e}")))?;

        let resp = client.get(url).send().await
            .map_err(|e| ToolError::Other(format!("fetch failed: {e}")))?;

        if !resp.status().is_success() {
            let st = resp.status();
            return Err(ToolError::Other(format!("HTTP {st}")));
        }

        let content_type = resp.headers().get("content-type")
            .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let body = resp.bytes().await
            .map_err(|e| ToolError::Other(format!("read body: {e}")))?;
        let body_str = String::from_utf8_lossy(&body[..body.len().min(MAX_FETCH_BYTES)]);
        let text = if content_type.contains("text/html") {
            html_to_text(&body_str)
        } else {
            body_str.into_owned()
        };

        let char_count = text.chars().count();
        if char_count > 8000 {
            let preview: String = text.chars().take(8000).collect();
            Ok(format!("{preview}\n\n…[{char_count} chars total]"))
        } else {
            Ok(text)
        }
    }
}

/// `web_search(query)` — search the web and return result titles + URLs + snippets.
/// Uses DuckDuckGo's HTML endpoint (no API key required).
pub struct WebSearch;

#[async_trait]
impl Tool for WebSearch {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web and return result titles, URLs, and snippets. Uses DuckDuckGo (no API key needed). Useful for finding documentation, APIs, examples, or answers."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "The search query." },
                "max_results": { "type": "integer", "description": "Maximum results to return (default 5)." }
            },
            "required": ["query"]
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let query = args.get("query").and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'query'".into()))?;
        let max_results = args.get("max_results").and_then(|v| v.as_u64()).unwrap_or(5) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(FETCH_TIMEOUT_SECS))
            .redirect(reqwest::redirect::Policy::limited(5))
            .user_agent("Meepo/0.1")
            .build()
            .map_err(|e| ToolError::Other(format!("http client: {e}")))?;

        let search_url = format!("https://html.duckduckgo.com/html/?q={}", urlencoding(query));
        let resp = client.get(&search_url).send().await
            .map_err(|e| ToolError::Other(format!("search failed: {e}")))?;

        if !resp.status().is_success() {
            let st = resp.status();
            return Err(ToolError::Other(format!("HTTP {st}")));
        }

        let html = resp.text().await
            .map_err(|e| ToolError::Other(format!("read body: {e}")))?;

        let results = parse_ddg_results(&html, max_results);
        if results.is_empty() {
            return Ok("No results found.".into());
        }

        let formatted: Vec<String> = results.iter().enumerate().map(|(i, r)| {
            format!("{}. {}\n   {}\n   {}", i + 1, r.title, r.url, r.snippet)
        }).collect();
        Ok(formatted.join("\n\n"))
    }
}

// ── helpers ──

struct SearchResult {
    title: String,
    url: String,
    snippet: String,
}

/// Convert HTML to readable plain text.
fn html_to_text(html: &str) -> String {
    // Remove script/style blocks (non-greedy, case-insensitive, dotall).
    let script_re = Regex::new("(?is)<script[^>]*>.*?</script>").unwrap();
    let style_re = Regex::new("(?is)<style[^>]*>.*?</style>").unwrap();
    let no_scripts = script_re.replace_all(html, "");
    let no_style = style_re.replace_all(&no_scripts, "");

    let block_re = Regex::new(r"(?i)</?(p|div|br|h[1-6]|li|tr|hr)[^>]*>").unwrap();
    let with_newlines = block_re.replace_all(&no_style, "\n");

    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    let no_tags = tag_re.replace_all(&with_newlines, "");

    let decoded = no_tags
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    decoded.lines().map(|l| l.trim()).filter(|l| !l.is_empty())
        .collect::<Vec<_>>().join("\n")
}

/// Parse DuckDuckGo HTML search results.
fn parse_ddg_results(html: &str, max: usize) -> Vec<SearchResult> {
    let link_re = Regex::new(r#"class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>"#).unwrap();
    let snippet_re = Regex::new(r#"class="result__snippet"[^>]*>(.*?)</a>"#).unwrap();
    let mut results: Vec<SearchResult> = Vec::new();

    for cap in link_re.captures_iter(html) {
        if results.len() >= max {
            break;
        }
        let raw_url = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_title = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        let url = extract_ddg_url(raw_url);
        let title = strip_tags(raw_title).trim().to_string();
        if !title.is_empty() && !url.is_empty() {
            results.push(SearchResult { title, url, snippet: String::new() });
        }
    }

    for (i, cap) in snippet_re.captures_iter(html).enumerate() {
        if i >= results.len() {
            break;
        }
        results[i].snippet = strip_tags(cap.get(1).map(|m| m.as_str()).unwrap_or("")).trim().to_string();
    }

    results
}

/// Extract the actual URL from DDG's redirect wrapper.
fn extract_ddg_url(raw: &str) -> String {
    if let Some(start) = raw.find("uddg=") {
        let rest = &raw[start + 5..];
        let end = rest.find('&').unwrap_or(rest.len());
        return url_decode(&rest[..end]);
    }
    if raw.starts_with("http") {
        raw.to_string()
    } else {
        String::new()
    }
}

fn strip_tags(s: &str) -> String {
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    tag_re.replace_all(s, "").to_string()
}

fn urlencoding(s: &str) -> String {
    s.chars().map(|c| match c {
        ' ' => '+'.to_string(),
        c if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' => c.to_string(),
        c => format!("%{:02X}", c as u8),
    }).collect()
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            let h1 = chars.next();
            let h2 = chars.next();
            if let (Some(a), Some(b)) = (h1, h2) {
                let hex = format!("{a}{b}");
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            result.push(c);
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_to_text_strips_tags() {
        let html = r#"<html><body><h1>Title</h1><p>Hello <b>world</b></p><script>alert(1)</script></body></html>"#;
        let text = html_to_text(html);
        assert!(text.contains("Title"));
        assert!(text.contains("Hello world"));
        assert!(!text.contains("<"));
        assert!(!text.contains("alert"));
    }

    #[test]
    fn html_to_text_decodes_entities() {
        let html = "<p>a &amp; b &lt; c</p>";
        let text = html_to_text(html);
        assert_eq!(text.trim(), "a & b < c");
    }

    #[test]
    fn urlencoding_encodes_spaces() {
        assert_eq!(urlencoding("hello world"), "hello+world");
    }

    #[test]
    fn extract_ddg_url_from_redirect() {
        let raw = "//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com&rut=abc";
        assert_eq!(extract_ddg_url(raw), "https://example.com");
    }

    #[test]
    fn extract_ddg_url_passthrough() {
        assert_eq!(extract_ddg_url("https://example.com"), "https://example.com");
    }
}
