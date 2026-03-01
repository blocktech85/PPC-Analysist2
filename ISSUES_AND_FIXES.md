# Issues and Fixes Document

Date Created: 2026-02-28 03:08:53 (UTC)  
Last Updated: 2026-03-01

## Resolved Issues

### SerpAPI / Run target
- **Run produced no snapshots / 400 Bad Request:** SerpAPI expects city-level location, not bare ZIP. Added `_location_for_serpapi()` to normalize ZIPs (e.g. 85001 → Phoenix, Arizona, United States). Run (desktop+mobile) now succeeds.
- **API key visible in errors:** All user-facing error messages (flash, JSON API, ATC/crawl responses) now pass through `redact_api_keys()` so keys are never shown. SerpAPI error response body is redacted before raising.

### Ads Transparency Center (ATC)
- **Unsupported `US` region parameter:** SerpAPI ATC requires numeric region codes. Added `_atc_region_code()` so "US" → "2840" (United States). Both `atc_list` and `atc_details` use it.

### Configuration
- **.env not loaded when run from different cwd:** `config.py` now loads `.env` explicitly from project root (`BASE_DIR / ".env"`) so keys load regardless of current working directory.
- **API keys:** Loaded from `config.py` via `python-dotenv` and env vars. Place `.env` in project root (same folder as `app.py`).

### Data & API
- **Auction insights “No snapshot data”:** Date filter now uses Python UTC cutoff instead of SQLite `date('now', …)` so the window matches stored snapshot timestamps.
- **add_targets return value:** Now returns actual count of inserted rows instead of `len(keywords)`.

### Budget tracking
- **400 on scheduled budget tracking for ZIP targets:** Budget exhaustion service now uses `_location_for_serpapi()` so SerpAPI requests succeed for ZIP-based targets.

### Security / XSS
- **API keys in error messages:** Redaction applied everywhere errors are shown or returned (see above).
- **Unescaped API data in HTML:** Added `esc()` in research, auction_insights, competitor, and geo templates. All dynamic content (advertiser names, error messages, diff lists, presence data) is escaped before being set via `innerHTML`.

### UX
- **Trends:** Real data or error only—no placeholder/fake data. On failure the API returns `series: []` and an `error` message; the UI shows the error and no chart.
- **Auction insights empty state:** Message updated to “No snapshot data for the last X days. Run targets on this job first, or try a longer window.”

### Production readiness (2026-03-01)
- **init_db under WSGI:** `init_db()` is now called when the app module is loaded (after creating the Flask app), so the database is initialized when using gunicorn/uwsgi, not only when running `python app.py`.
- **Config and run docs:** Added `.env.example`, `README.md` (run locally, production, config reference), and docstring in `config.py` for where config is read from.

> Update this document as new issues and fixes are identified.
