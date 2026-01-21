# Web Fetch Strategy

Common web fetching strategy for anti-crawler handling.

## Site Classification

| Type | Examples | Characteristics |
|------|----------|-----------------|
| Anti-crawler | Reddit, Twitter/X, LinkedIn | Need login or browser fingerprint |
| Regular | blog.rust-lang.org, docs.rs | No anti-crawler, direct fetch |

## Fetch Priority

```
Anti-crawler sites: Local Chrome → crawl4ai MCP → give up and mark
Regular sites: WebFetch → crawl4ai MCP
```

## Tools

### 1. Local Chrome (for anti-crawler)

User's real browser with login and normal fingerprint.

**macOS:**
```bash
# Open URL
osascript -e 'tell application "Google Chrome" to open location "URL"'

# Get page HTML
osascript -e 'tell application "Google Chrome" to execute front window'\''s active tab javascript "document.documentElement.outerHTML"'
```

### 2. crawl4ai MCP (fallback)

Strong anti-crawler bypass, needs Docker.

```
mcp__crawl4ai__scrape(url: "URL")
```

### 3. WebFetch (regular sites)

Built-in tool, simple and fast, no anti-crawler capability.

## Site Routing

| Domain | Tool | Reason |
|--------|------|--------|
| reddit.com | Local Chrome | Strict anti-crawler |
| twitter.com / x.com | Local Chrome | Needs login |
| linkedin.com | Local Chrome | Strict anti-crawler |
| *.rust-lang.org | WebFetch | No anti-crawler |
| docs.rs | WebFetch | No anti-crawler |
| crates.io | WebFetch | No anti-crawler |
| this-week-in-rust.org | WebFetch | No anti-crawler |
| rustfoundation.org | WebFetch | No anti-crawler |
| github.com | WebFetch | Light rate limit |

## Failure Handling

1. Local Chrome fails → try crawl4ai
2. crawl4ai fails → try WebFetch
3. All fail → mark "Fetch failed: {reason}"

## Validation

After fetch, check:
- Content is not empty
- Not an error page (403, 429, "blocked")
- Contains expected data
