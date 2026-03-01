"""Google Ads Transparency Center via SerpAPI."""
import json
import logging

import requests

from config import SERPAPI_API_KEY
from db import cursor, utc_now
from utils import redact_api_keys

logger = logging.getLogger(__name__)

BASE_URL = "https://serpapi.com/search"

# SerpAPI ATC expects numeric region codes (e.g. 2840 for US), not "US".
def _atc_region_code(region: str) -> str:
    r = str(region or "").strip().upper()
    if r == "US" or not r:
        return "2840"
    if r.isdigit():
        return r
    return str(region)


def _search(params: dict) -> dict:
    if not SERPAPI_API_KEY:
        raise ValueError("SERPAPI_API_KEY is not set")
    params = {**params, "api_key": SERPAPI_API_KEY}
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def atc_list(advertiser: str, region: str = "US", page: int = 1):
    """Fetch ATC creatives list for advertiser (text search or advertiser_id). Returns dict with creatives, synced_at_utc, raw_keys."""
    advertiser = (advertiser or "").strip()
    if not advertiser:
        return {"creatives": [], "error": "Enter an advertiser name or domain", "raw_keys": [], "synced_at_utc": None}

    # SerpAPI ATC requires numeric region code (e.g. 2840 for United States), not "US".
    region_param = _atc_region_code(region)

    # First try: text search (may return matching advertisers or direct ads depending on SerpAPI)
    params = {
        "engine": "google_ads_transparency_center",
        "text": advertiser,
        "region": region_param,
        "num": 100,
    }
    try:
        data = _search(params)
    except requests.exceptions.HTTPError as e:
        err_body = ""
        if e.response is not None:
            try:
                err_body = e.response.text[:500]
            except Exception:
                pass
        logger.exception("ATC list HTTP error: %s %s", e, err_body)
        err_msg = redact_api_keys(str(e) + (" " + err_body if err_body else ""))
        return {"creatives": [], "error": err_msg, "raw_keys": [], "synced_at_utc": None}
    except Exception as e:
        logger.exception("ATC list failed: %s", e)
        return {"creatives": [], "error": redact_api_keys(str(e)), "raw_keys": [], "synced_at_utc": None}

    raw_keys = list(data.keys()) if isinstance(data, dict) else []
    # SerpAPI may return 200 with an error message in body
    if isinstance(data, dict) and data.get("error"):
        return {"creatives": [], "error": redact_api_keys(str(data.get("error"))), "raw_keys": raw_keys, "synced_at_utc": None}

    now = utc_now()
    creatives = []
    # Direct list of ads (top-level keys)
    for key in ("ads", "creatives", "results", "advertiser_ads", "ads_by_advertiser"):
        val = data.get(key) if isinstance(data, dict) else None
        if isinstance(val, list):
            for c in val:
                if isinstance(c, dict):
                    creatives.append(_normalize_creative(c, region))
            break
        # advertiser_ads might be dict of id -> list of ads
        if isinstance(val, dict):
            for v in val.values():
                if isinstance(v, list):
                    for c in v:
                        if isinstance(c, dict):
                            creatives.append(_normalize_creative(c, region))
            if creatives:
                break
    # Nested: advertiser_results / advertisers -> first hit then its ads
    if not creatives and isinstance(data, dict):
        for wrap_key in ("advertiser_results", "advertisers", "search_results"):
            wrap = data.get(wrap_key)
            if not isinstance(wrap, list):
                continue
            for item in wrap:
                if not isinstance(item, dict):
                    continue
                # Item might be { advertiser_id, name, ads: [...] } or { ads: [...] }
                ad_list = item.get("ads") or item.get("creatives") or item.get("results")
                if isinstance(ad_list, list):
                    for c in ad_list:
                        if isinstance(c, dict):
                            creatives.append(_normalize_creative(c, region))
                # Or we need to do a follow-up by advertiser_id
                adv_id = item.get("advertiser_id") or item.get("advertiserId") or item.get("id")
                if not creatives and adv_id:
                    try:
                        sub = _search({
                            "engine": "google_ads_transparency_center",
                            "advertiser_id": str(adv_id),
                            "region": region_param,
                            "num": 100,
                        })
                        for k in ("ads", "creatives", "results"):
                            if isinstance(sub.get(k), list):
                                for c in sub[k]:
                                    if isinstance(c, dict):
                                        creatives.append(_normalize_creative(c, region))
                                break
                        if creatives:
                            break
                    except Exception as e:
                        logger.debug("ATC follow-up by advertiser_id failed: %s", e)
            if creatives:
                break
    # Fallback: recursively find any list of dicts that look like ads (have creative_id, ad_id, or title)
    if not creatives and isinstance(data, dict):
        creatives = _extract_ads_from_any_key(data, region)

    with cursor() as cur:
        cur.execute(
            """INSERT INTO atc_snapshots (advertiser, region, raw_json, synced_at_utc) VALUES (?,?,?,?)""",
            (advertiser, region, json.dumps(data)[:100000] if data else None, now),
        )
    out = {"creatives": creatives, "synced_at_utc": now, "raw_keys": raw_keys}
    if not creatives and isinstance(data, dict):
        out["response_structure"] = _describe_structure(data)
    return out


def _describe_structure(obj, max_depth=2):
    """Return a short description of response structure for debugging."""
    if max_depth <= 0:
        return "..."
    if obj is None:
        return "null"
    if isinstance(obj, dict):
        parts = []
        for k, v in list(obj.items())[:20]:
            t = type(v).__name__
            if isinstance(v, list):
                parts.append(f"{k}(list[{len(v)}])")
                if v and isinstance(v[0], dict) and max_depth > 1:
                    parts.append(f"  first keys: {list(v[0].keys())[:10]}")
            elif isinstance(v, dict):
                parts.append(f"{k}(dict)")
            else:
                parts.append(f"{k}({t})")
        return " ".join(parts)
    if isinstance(obj, list):
        return f"list[{len(obj)}]"
    return type(obj).__name__


def _extract_ads_from_any_key(obj, region="", seen=None):
    """Recursively find lists of dicts that look like ad creatives."""
    if seen is None:
        seen = set()
    if id(obj) in seen:
        return []
    if isinstance(obj, dict):
        seen.add(id(obj))
        for key, val in obj.items():
            if isinstance(val, list) and val and isinstance(val[0], dict):
                # Check if items look like ads
                first = val[0]
                if any(first.get(k) for k in ("ad_id", "creative_id", "id", "title", "headline")):
                    out = []
                    for c in val:
                        if isinstance(c, dict):
                            out.append(_normalize_creative(c, region))
                    if out:
                        return out
            out = _extract_ads_from_any_key(val, region, seen)
            if out:
                return out
        return []
    if isinstance(obj, list):
        for item in obj:
            out = _extract_ads_from_any_key(item, region, seen)
            if out:
                return out
    return []


def _normalize_creative(c: dict, region: str) -> dict:
    return {
        "ad_id": c.get("ad_id") or c.get("creative_id") or c.get("id"),
        "title": c.get("title") or c.get("headline") or "",
        "format": c.get("format") or c.get("creative_format") or "unknown",
        "first_seen": c.get("first_seen") or c.get("start_date"),
        "last_seen": c.get("last_seen") or c.get("end_date"),
        "preview_url": c.get("preview_url") or c.get("preview_link"),
        "final_url": c.get("final_url") or c.get("destination_url"),
    }


def atc_details(ad_id: str, region: str):
    """Fetch ATC ad details by creative ID. Returns dict with normalized, synced_at_utc."""
    region_param = _atc_region_code(region)
    params = {
        "engine": "google_ads_transparency_center_ad_details",
        "ad_id": ad_id,
        "region": region_param,
    }
    try:
        data = _search(params)
    except Exception as e:
        logger.exception("ATC details failed: %s", e)
        return {"normalized": {}, "error": redact_api_keys(str(e)), "synced_at_utc": None}
    now = utc_now()
    normalized = _normalize_atc_details(data)
    return {"normalized": normalized, "synced_at_utc": now}


def _normalize_atc_details(data: dict) -> dict:
    """Extract headlines, descriptions, callouts, sitelinks from ATC details response."""
    out = {"format": "unknown", "headlines": [], "descriptions": [], "callouts": [], "sitelinks": [], "raw": data}
    if not data:
        return out
    # Common SerpAPI ATC detail keys
    for key in ("headlines", "descriptions", "callouts", "sitelinks"):
        val = data.get(key)
        if isinstance(val, list):
            out[key] = [str(x) if not isinstance(x, dict) else x.get("text") or x.get("title") or "" for x in val]
        elif val:
            out[key] = [str(val)]
    if data.get("format"):
        out["format"] = data["format"]
    # Sitelinks as list of {text, url}
    sl = data.get("sitelinks")
    if isinstance(sl, list):
        out["sitelinks"] = [{"text": x.get("text") or x.get("title"), "url": x.get("url") or x.get("link")} for x in sl if isinstance(x, dict)]
    return out
