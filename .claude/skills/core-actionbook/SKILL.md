---
name: core-actionbook
# Internal tool - no description to prevent auto-triggering
# Used by: rust-learner agents for pre-computed selectors
---

# Actionbook

Pre-computed action manuals for browser automation. Agents receive structured page information instead of parsing entire HTML.

## Workflow

1. **search_actions** - Search by keyword, returns URL-based action IDs with content previews
2. **get_action_by_id** - Get full action manual with page details, DOM structure, and element selectors
3. **Execute** - Use returned selectors with your browser automation tool

## MCP Tools

- `search_actions` - Search by keyword. Returns: URL-based action IDs, content previews, relevance scores
- `get_action_by_id` - Get full action details. Returns: action content, page element selectors (CSS/XPath), element types, allowed methods (click, type, extract), document metadata

### Parameters

**search_actions**:
- `query` (required): Search keyword (e.g., "airbnb search", "google login")
- `type`: `vector` | `fulltext` | `hybrid` (default)
- `limit`: Max results (default: 5)
- `sourceIds`: Filter by source IDs (comma-separated)
- `minScore`: Minimum relevance score (0-1)

**get_action_by_id**:
- `id` (required): URL-based action ID (e.g., `example.com/page`)

## Example Response

```json
{
  "title": "Airbnb Search",
  "url": "www.airbnb.com/search",
  "elements": [
    {
      "name": "location_input",
      "selector": "input[data-testid='structured-search-input-field-query']",
      "type": "textbox",
      "methods": ["type", "fill"]
    }
  ]
}
```
