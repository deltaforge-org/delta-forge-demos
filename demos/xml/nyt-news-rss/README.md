# XML NYT News — RSS Feed Analysis

Demonstrates how DeltaForge handles real-world namespaced XML with repeating elements, self-closing elements, and multi-file reading. Seven NYT RSS feed exports from different regions are read into two external tables showing different repeating element strategies.

## Data Story

The New York Times publishes RSS feeds for world news by region. Each XML file is a standard RSS 2.0 feed using four XML namespaces:

| Namespace | Prefix | Purpose |
|-----------|--------|---------|
| Dublin Core | `dc:` | Author (`dc:creator`) |
| Media RSS | `media:` | Thumbnails, credits, descriptions |
| Atom | `atom:` | Self-referencing links |
| NYT | `nyt:` | NYT-specific extensions |

| File | Region | Items | Categories |
|------|--------|-------|------------|
| `Africa.xml` | Africa | 20 | 173 |
| `Americas.xml` | Americas | 31 | 334 |
| `AsiaPacific.xml` | Asia-Pacific | 20 | 134 |
| `Europe.xml` | Europe | 20 | 186 |
| `MiddleEast.xml` | Middle East | 26 | 241 |
| `World.xml` | World (Mar 2025) | 57 | 494 |
| `news.xml` | World (Feb 2025) | 57 | 461 |
| **Total** | | **231** | **2023** |

## Tables

### `news_articles` — One row per article (231 rows)

Repeating `<category>` elements joined as comma-separated string. Namespace-prefixed columns mapped to friendly names.

| Column | Source | Notes |
|--------|--------|-------|
| `title` | `<title>` | Article headline |
| `link` | `<link>` | Article URL |
| `description` | `<description>` | Summary text |
| `author` | `<dc:creator>` | Column mapping from `creator` |
| `pubDate` | `<pubDate>` | Publication timestamp |
| `category` | `<category>` (x0–27) | Comma-joined via `JoinComma` |
| `thumbnail_url` | `<media:content @url>` | From self-closing element attribute |
| `thumbnail_height` | `<media:content @height>` | Image dimensions |
| `thumbnail_width` | `<media:content @width>` | Image dimensions |
| `media_type` | `<media:content @medium>` | Always "image" |
| `media_credit` | `<media:credit>` | Photographer/agency |

### `news_categories` — One row per category (~2023 rows)

Each `<category>` element exploded into its own row. The `@domain` attribute distinguishes keyword types for analytics.

| Column | Source | Notes |
|--------|--------|-------|
| `title` | `<title>` | Article headline (duplicated per category) |
| `author` | `<dc:creator>` | Column mapping |
| `pubDate` | `<pubDate>` | Publication timestamp |
| `category` | `<category>` text | The keyword value |
| `category_type` | `<category @domain>` | Keyword namespace URI |

Category types (from `@domain`):
- `.../keywords/des` — Topic descriptors (e.g., "War and Armed Conflicts")
- `.../keywords/nyt_per` — People (e.g., "Trump, Donald J")
- `.../keywords/nyt_geo` — Places (e.g., "Ukraine")
- `.../keywords/nyt_org` — Organizations (e.g., "Hamas")

## How to Verify

Run the **Summary** query (#12) to see PASS/FAIL for each check:

```sql
SELECT 'total_articles' AS check_name,
       CASE WHEN COUNT(*) = 231 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.xml.news_articles
UNION ALL ...
ORDER BY check_name;
```

## What This Tests

1. **Namespace handling** — 4 XML namespaces stripped via `strip_namespace_prefixes` + `namespaces` map
2. **Repeating element JoinComma** — Multiple `<category>` elements joined into one comma string
3. **Repeating element Explode** — One row per `<category>` in the categories table
4. **Self-closing element attributes** — `<media:content url="..." height="..." />` attributes extracted
5. **Column mappings** — `creator` → `author`, `credit` → `media_credit`, `content/@url` → `thumbnail_url`
6. **Exclude paths** — Channel-level metadata (`/rss/channel/image`, etc.) excluded
7. **Multi-file reading** — All 7 XML files discovered and read from one directory
8. **Optional elements → NULL** — Items missing `media:description` produce NULL
9. **HTML entity handling** — `&gt;`, `&#39;` decoded in text content
