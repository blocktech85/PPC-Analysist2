"""SerpAPI service: run Google Search and store snapshots + ads."""
import json
import logging

import requests

from config import SERPAPI_API_KEY
from db import cursor, utc_now
from utils import redact_api_keys

logger = logging.getLogger(__name__)

BASE_URL = "https://serpapi.com/search"


def _search(params: dict) -> dict:
    if not SERPAPI_API_KEY:
        raise ValueError("SERPAPI_API_KEY is not set")
    params = {**params, "api_key": SERPAPI_API_KEY}
    r = requests.get(BASE_URL, params=params, timeout=60)
    if not r.ok:
        err_msg = str(r.status_code)
        try:
            body = r.json()
            if isinstance(body, dict) and body.get("error"):
                err_msg = f"{r.status_code}: {body['error']}"
        except Exception:
            if r.text:
                err_msg = f"{r.status_code}: {r.text[:200]}"
        raise ValueError(redact_api_keys(err_msg))
    return r.json()


def _location_for_serpapi(location: str, gl: str) -> str:
    """Return a location string SerpAPI accepts. SerpAPI expects city-level (e.g. 'Phoenix, Arizona, United States'); bare ZIP can cause 400."""
    loc = str(location or "").strip()
    if not loc:
        return "United States" if (gl or "us").lower() == "us" else loc
    # If it's a bare US ZIP (5 digits or 5+4), use a format that often works: "ZIP, State, United States" or fallback to US.
    if loc.isdigit() and len(loc) in (5, 9):
        # Common ZIPs -> city, state for better SerpAPI acceptance
        zip_to_place = {
            "85001": "Phoenix, Arizona, United States",
            "10001": "New York, New York, United States",
            "90210": "Beverly Hills, California, United States",
            "60601": "Chicago, Illinois, United States",
            "75201": "Dallas, Texas, United States",
            "33101": "Miami, Florida, United States",
        }
        if loc in zip_to_place:
            return zip_to_place[loc]
        # Unknown ZIP: use "United States" so request succeeds; gl=us keeps country.
        return "United States"
    return loc


def _extract_ads_from_serp(result: dict):
    """Extract paid ads from SerpAPI Google result. Handles both 'ads' and 'paid' keys."""
    ads = result.get("ads") or result.get("paid") or []
    out = []
    for i, ad in enumerate(ads):
        if not isinstance(ad, dict):
            continue
        # Normalize common field names
        headline = ad.get("title") or ad.get("headline") or ""
        desc = ad.get("description") or ad.get("snippet") or ""
        link = ad.get("link") or ad.get("destination_link") or ""
        displayed = ad.get("displayed_link") or ad.get("display_link") or ""
        block = ad.get("block") or ("top" if i < 4 else "bottom")
        extensions = ad.get("extensions") or ad.get("sitelinks") or []
        if isinstance(extensions, list):
            ext_list = extensions
        else:
            ext_list = [extensions] if extensions else []
        ad_id = ad.get("ad_id") or ad.get("position") or str(i)
        out.append({
            "ad_id": str(ad_id),
            "headline": headline,
            "description": desc,
            "destination_link": link,
            "displayed_link": displayed,
            "block": str(block),
            "position": ad.get("position", i + 1),
            "extensions": ext_list,
            "offers": ad.get("offers") or [],
        })
    return out


def run_target(target_id: int, job_id: int, keyword: str, location: str, gl: str, hl: str):
    """Run SERP for a target (desktop + mobile), store snapshots and ads. Returns snapshot count."""
    now = utc_now()
    location = _location_for_serpapi(location, gl)
    devices = ["desktop", "mobile"]
    snapshot_count = 0
    for device in devices:
        params = {
            "q": keyword,
            "engine": "google",
            "location": location,
            "gl": gl,
            "hl": hl,
            "device": device,
        }
        try:
            result_dict = _search(params)
        except Exception as e:
            logger.exception("SerpAPI search failed for target %s device %s: %s", target_id, device, e)
            raise
        with cursor() as cur:
            cur.execute(
                """INSERT INTO serp_snapshots (target_id, device, captured_at_utc, raw_json) VALUES (?,?,?,?)""",
                (target_id, device, now, json.dumps(result_dict)[:50000] if result_dict else None),
            )
            snap_id = cur.lastrowid
        snapshot_count += 1
        ads = _extract_ads_from_serp(result_dict)
        with cursor() as cur:
            for ad in ads[:100]:
                cur.execute(
                    """INSERT INTO ads (snapshot_id, job_id, advertiser, ad_id, device, block, headline, description,
                    displayed_link, destination_link, position, created_at_utc, extensions_json, offers_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        snap_id,
                        job_id,
                        _domain_from_link(ad.get("displayed_link") or ad.get("destination_link") or "unknown"),
                        ad.get("ad_id"),
                        device,
                        ad.get("block"),
                        ad.get("headline"),
                        ad.get("description"),
                        ad.get("displayed_link"),
                        ad.get("destination_link"),
                        ad.get("position"),
                        now,
                        json.dumps(ad.get("extensions") or []),
                        json.dumps(ad.get("offers") or []),
                    ),
                )
    return snapshot_count


def _domain_from_link(link: str) -> str:
    """Extract domain from URL for use as advertiser name."""
    if not link:
        return "unknown"
    link = link.strip().lower()
    for prefix in ("https://", "http://", "www."):
        if link.startswith(prefix):
            link = link[len(prefix) :]
    return link.split("/")[0].split(":")[0] or "unknown"
