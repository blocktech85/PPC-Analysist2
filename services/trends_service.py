"""Google Trends via SerpAPI. Real data or error only—no placeholder data."""
import logging
from datetime import datetime, timezone

import requests

from config import SERPAPI_API_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://serpapi.com/search"


def _search(params: dict) -> dict:
    if not SERPAPI_API_KEY:
        return None
    params = {**params, "api_key": SERPAPI_API_KEY}
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_trends(keywords: str, geo: str = "US", timeframe: str = "today 12-m"):
    """Fetch trend series for keywords. Returns dict with series (list of {keyword, points: [{t, v}]}), synced_at_utc, error."""
    kw_list = [k.strip() for k in keywords.replace("\n", ",").split(",") if k.strip()][:5]
    if not kw_list:
        return {"series": [], "synced_at_utc": None, "error": "No keywords provided"}
    data = None
    try:
        if len(kw_list) == 1:
            data = _search({"engine": "google_trends", "q": kw_list[0], "geo": geo, "date": timeframe})
        else:
            data = _search({"engine": "google_trends", "q": kw_list, "geo": geo, "date": timeframe})
    except Exception as e:
        logger.warning("Trends API failed: %s", e)
        return {"series": [], "synced_at_utc": None, "error": "Trends API unavailable."}
    if not data:
        return {"series": [], "synced_at_utc": None, "error": "Trends API unavailable."}
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    series = []
    timeline = data.get("interest_over_time", {}).get("timeline_data") or data.get("timeline_data") or []
    if timeline:
        for kw_idx, kw in enumerate(kw_list):
            points = []
            for d in timeline:
                if not isinstance(d, dict):
                    continue
                t = d.get("date") or d.get("timestamp")
                vals = d.get("values") or []
                # SerpAPI returns values in same order as queries; or each item has "query" and "value"/"extracted_value"
                if len(kw_list) == 1:
                    item = vals[0] if vals else {}
                else:
                    item = None
                    for v in vals:
                        if isinstance(v, dict) and (v.get("query") or "").strip().lower() == kw.lower():
                            item = v
                            break
                    if item is None and kw_idx < len(vals):
                        item = vals[kw_idx] if isinstance(vals[kw_idx], dict) else {}
                    else:
                        item = item or {}
                v = item.get("extracted_value") or item.get("value") or 0
                try:
                    v = int(v) if not isinstance(v, (int, float)) else v
                except (TypeError, ValueError):
                    v = 0
                if t:
                    points.append({"t": t[:10] if len(str(t)) >= 10 else str(t), "v": v})
            series.append({"keyword": kw, "points": points})
    if not series:
        return {"series": [], "synced_at_utc": None, "error": "No trend data in response."}
    return {"series": series, "synced_at_utc": now, "error": None}

