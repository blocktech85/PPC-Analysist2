import os
import re
import json
import csv
import sqlite3
from io import StringIO
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, redirect, url_for, render_template, flash, jsonify, Response

APP = Flask(__name__)
APP.secret_key = os.environ.get("FLASK_SECRET", "dev-only-change-me")

DB_PATH = os.environ.get("DB_PATH", "ppc_competitors.sqlite3")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")  # required
PAGESPEED_API_KEY = os.environ.get("PAGESPEED_API_KEY")  # optional

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"


# -----------------------------
# Utility: timestamps & picking fields
# -----------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def fmt_sync_mm(iso_ts: str) -> str:
    """
    UI-friendly sync stamp. Format: MM/DD HH:MM UTC
    """
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%m/%d %H:%M") + " UTC"

def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return default


# -----------------------------
# Offer / message intelligence (PPC-oriented)
# -----------------------------
OFFER_PATTERNS = {
    "discount_percent": re.compile(r"\b(\d{1,3})\s*%(\s*off)?\b", re.I),
    "discount_dollar": re.compile(r"\$\s?(\d{1,6})\b", re.I),
    "financing": re.compile(r"\b(financing|0%\s*apr|apr|low\s*monthly|per\s*month|\/mo)\b", re.I),
    "free": re.compile(r"\bfree\b", re.I),
    "same_day": re.compile(r"\b(same\s*day|today)\b", re.I),
    "warranty": re.compile(r"\b(warranty|guarantee)\b", re.I),
    "quote_estimate": re.compile(r"\b(quote|estimate|free estimate)\b", re.I),
    "licensed_insured": re.compile(r"\b(licensed|insured|bonded)\b", re.I),
    "consultation": re.compile(r"\b(consultation|free consultation)\b", re.I),
    "financing_terms": re.compile(r"\b(no money down|deferred|prequalify|soft check)\b", re.I),
}

def detect_offers(text: str) -> List[str]:
    hits = []
    t = text or ""
    for label, pat in OFFER_PATTERNS.items():
        if pat.search(t):
            hits.append(label)
    return hits


# -----------------------------
# DB schema
# -----------------------------
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS research_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  job_id INTEGER NOT NULL,
  keyword TEXT NOT NULL,
  location_input TEXT NOT NULL,         -- user entry (zip/county/string)
  serp_location TEXT NOT NULL,          -- normalized for SerpAPI
  gl TEXT NOT NULL DEFAULT 'us',
  hl TEXT NOT NULL DEFAULT 'en',
  FOREIGN KEY(job_id) REFERENCES research_jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_targets_job ON targets(job_id);

CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  device TEXT NOT NULL,                 -- desktop/mobile
  serpapi_search_id TEXT,
  raw_json TEXT,
  FOREIGN KEY(target_id) REFERENCES targets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_target ON runs(target_id);

CREATE TABLE IF NOT EXISTS ads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  position INTEGER,
  block TEXT,                           -- top_ads/bottom_ads/ads
  advertiser TEXT,
  displayed_link TEXT,
  destination_link TEXT,
  headline TEXT,
  description TEXT,
  offers_json TEXT,
  extensions_json TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ads_run ON ads(run_id);
CREATE INDEX IF NOT EXISTS idx_ads_adv ON ads(advertiser);

CREATE TABLE IF NOT EXISTS landing_pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ad_id INTEGER NOT NULL UNIQUE,
  fetched_at_utc TEXT NOT NULL,
  final_url TEXT,
  http_status INTEGER,
  title TEXT,
  h1 TEXT,
  h2s_json TEXT,
  has_form INTEGER,
  pricing_mentions INTEGER,
  financing_mentions INTEGER,
  offers_json TEXT,
  pagespeed_performance REAL,
  pagespeed_accessibility REAL,
  pagespeed_best_practices REAL,
  pagespeed_seo REAL,
  FOREIGN KEY(ad_id) REFERENCES ads(id) ON DELETE CASCADE
);

-- Optional cache for ATC responses (raw)
CREATE TABLE IF NOT EXISTS atc_cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at_utc TEXT NOT NULL,
  advertiser_query TEXT NOT NULL,
  region TEXT NOT NULL,
  endpoint TEXT NOT NULL, -- list/details
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_atc_cache ON atc_cache(advertiser_query, region, endpoint, fetched_at_utc);
"""

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = db()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


# -----------------------------
# Location normalization (ZIP / county / passthrough)
# -----------------------------
US_STATE_MAP = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas",
    "UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia"
}

def normalize_location(location_input: str) -> str:
    s = (location_input or "").strip()
    if not s:
        return "United States"

    if re.fullmatch(r"\d{5}", s) or re.fullmatch(r"\d{5}-\d{4}", s):
        return f"{s}, United States"

    m = re.fullmatch(r"(.+?)\s+County,\s*([A-Za-z]{2})", s)
    if m:
        county = m.group(1).strip()
        st = m.group(2).upper()
        state = US_STATE_MAP.get(st, st)
        return f"{county} County, {state}, United States"

    return s


# -----------------------------
# SerpAPI: Google search snapshot (ads)
# -----------------------------
def serpapi_search_google(keyword: str, serp_location: str, device: str, gl: str="us", hl: str="en") -> Dict[str, Any]:
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY is missing (set env var).")
    params = {
        "engine": "google",
        "q": keyword,
        "location": serp_location,
        "gl": gl,
        "hl": hl,
        "device": device,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def extract_domain(url_or_domain: Optional[str]) -> Optional[str]:
    if not url_or_domain:
        return None
    s = url_or_domain.strip()
    if "://" not in s and "/" not in s and "." in s:
        return s.lower().replace("www.", "")
    m = re.search(r"^(?:https?://)?([^/]+)", s, flags=re.I)
    if not m:
        return None
    return m.group(1).lower().replace("www.", "")

def infer_advertiser(ad: Dict[str, Any], displayed_link: Optional[str], destination_link: Optional[str], headline: Optional[str]) -> str:
    for k in ["advertiser", "source", "tracking_source"]:
        if isinstance(ad.get(k), str) and ad[k].strip():
            return ad[k].strip()
    domain = extract_domain(displayed_link) or extract_domain(destination_link)
    if domain:
        return domain
    if headline:
        return headline.split("|")[0].split("-")[0].strip()[:80]
    return "unknown"

def normalize_ad(ad: Dict[str, Any], block_key: str, fallback_pos: int) -> Dict[str, Any]:
    headline = ad.get("title") or ad.get("headline") or ad.get("name")
    description = ad.get("snippet") or ad.get("description") or ad.get("text")
    displayed_link = ad.get("displayed_link") or ad.get("display_link") or ad.get("displayLink")
    destination_link = ad.get("link") or ad.get("destination") or ad.get("url")

    extensions = {
        "extensions": ad.get("extensions"),
        "sitelinks": ad.get("sitelinks"),
        "callouts": ad.get("callouts"),
        "structured_snippets": ad.get("structured_snippets"),
        "price_extensions": ad.get("price_extensions"),
        "promotion_extensions": ad.get("promotion_extensions"),
    }
    extensions = {k: v for k, v in extensions.items() if v}

    advertiser = infer_advertiser(ad, displayed_link, destination_link, headline)
    text_blob = f"{headline or ''} {description or ''}"
    offers = detect_offers(text_blob)

    return {
        "position": ad.get("position", fallback_pos),
        "block": block_key,  # top_ads/bottom_ads/ads
        "advertiser": advertiser,
        "displayed_link": displayed_link,
        "destination_link": destination_link,
        "headline": headline,
        "description": description,
        "offers": offers,
        "extensions": extensions if extensions else None,
    }

def extract_ads(serp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    def pull(block_key: str) -> None:
        items = serp_json.get(block_key) or []
        if not isinstance(items, list):
            return
        for idx, ad in enumerate(items, start=1):
            out.append(normalize_ad(ad, block_key, idx))
    for key in ["top_ads", "bottom_ads", "ads"]:
        pull(key)
    return out


# -----------------------------
# SerpAPI: Ads Transparency Center (ATC)
# -----------------------------
def serpapi_atc_list_ads(advertiser_query: str, region: str="US", page: int=1) -> Dict[str, Any]:
    """
    SerpAPI docs: engine=google_ads_transparency_center
    """
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY is missing.")
    params = {
        "engine": "google_ads_transparency_center",
        "advertiser": advertiser_query,
        "region": region,
        "page": page,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def serpapi_atc_ad_details(ad_id: str, region: str="US") -> Dict[str, Any]:
    """
    SerpAPI docs: engine=google_ads_transparency_center_ad_details
    """
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY is missing.")
    params = {
        "engine": "google_ads_transparency_center_ad_details",
        "ad_id": ad_id,
        "region": region,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_atc_list_item(item: Dict[str, Any]) -> Dict[str, Any]:
    ad_id = pick(item, "ad_id", "id", "creative_id", "adId", "creativeId")
    title = pick(item, "ad_title", "title", "headline", "name", "text", "creative_text")
    fmt = pick(item, "format", "ad_format", "type", "ad_type", default="unknown")
    preview = pick(item, "preview", "preview_url", "image", "image_url", "thumbnail", "video_url")
    final_url = pick(item, "final_url", "landing_page", "landing_page_url", "url", "link")
    first_seen = pick(item, "first_seen", "firstShown", "start_date", "startDate")
    last_seen = pick(item, "last_seen", "lastShown", "end_date", "endDate")
    regions = pick(item, "regions", "countries", "served_regions", "geo", default=[])

    if isinstance(regions, dict):
        regions = list(regions.values())
    if isinstance(regions, str):
        regions = [regions]

    return {
        "ad_id": ad_id,
        "title": title,
        "format": fmt,
        "preview_url": preview,
        "final_url": final_url,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "regions": regions if isinstance(regions, list) else [],
        "raw": item,
    }

def normalize_atc_details(details: Dict[str, Any]) -> Dict[str, Any]:
    advertiser = pick(details, "advertiser", "advertiser_name", "advertiserName", "advertiser_domain")
    ad_id = pick(details, "ad_id", "id", "creative_id")
    fmt = pick(details, "format", "ad_format", "type", "ad_type", default="unknown")

    headlines: List[str] = []
    descriptions: List[str] = []
    callouts: List[str] = []
    sitelinks: List[Dict[str, Any]] = []
    final_urls: List[str] = []
    media: List[Dict[str, Any]] = []

    containers: List[Dict[str, Any]] = []
    for k in ["ad", "creative", "ad_creative", "details", "result", "ad_details"]:
        if isinstance(details.get(k), dict):
            containers.append(details[k])
    containers.append(details)

    def extend_str_list(dst: List[str], val: Any):
        if isinstance(val, list):
            for x in val:
                if isinstance(x, str) and x.strip():
                    dst.append(x.strip())

    for c in containers:
        extend_str_list(headlines, pick(c, "headlines", "headline_variants", "headlineVariants", default=[]))
        extend_str_list(descriptions, pick(c, "descriptions", "description_variants", "descriptionVariants", default=[]))
        extend_str_list(callouts, pick(c, "callouts", "callout_extensions", "calloutExtensions", default=[]))

        sl = pick(c, "sitelinks", "site_links", "sitelink_extensions", "sitelinkExtensions", default=[])
        if isinstance(sl, list):
            for s in sl:
                if isinstance(s, dict):
                    txt = pick(s, "text", "title", "label")
                    url = pick(s, "url", "link", "final_url")
                    sitelinks.append({"text": txt, "url": url})
                elif isinstance(s, str):
                    sitelinks.append({"text": s, "url": None})

        fu = pick(c, "final_urls", "finalUrls", "landing_pages", "landingPages", "urls", default=[])
        if isinstance(fu, list):
            for u in fu:
                if isinstance(u, str) and u.strip():
                    final_urls.append(u.strip())
                elif isinstance(u, dict):
                    uu = pick(u, "url", "link")
                    if isinstance(uu, str) and uu.strip():
                        final_urls.append(uu.strip())

        m = pick(c, "media", "assets", "images", "videos", default=[])
        if isinstance(m, list):
            for a in m:
                if isinstance(a, dict):
                    media.append({
                        "type": pick(a, "type", "asset_type", default="unknown"),
                        "url": pick(a, "url", "link", "src"),
                        "preview": pick(a, "preview", "thumbnail"),
                    })
                elif isinstance(a, str) and a.strip():
                    media.append({"type": "unknown", "url": a.strip(), "preview": None})

    def dedupe(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        "ad_id": ad_id,
        "advertiser": advertiser,
        "format": fmt,
        "headlines": dedupe(headlines),
        "descriptions": dedupe(descriptions),
        "callouts": dedupe(callouts),
        "sitelinks": sitelinks,
        "final_urls": [u for u in final_urls if u],
        "media": media,
        "raw": details,
    }


# -----------------------------
# Landing page crawler + PageSpeed Insights
# -----------------------------
def fetch_pagespeed(url: str, strategy: str="mobile") -> Dict[str, Optional[float]]:
    if not PAGESPEED_API_KEY:
        return {"performance": None, "accessibility": None, "best_practices": None, "seo": None}
    endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {"url": url, "strategy": strategy, "key": PAGESPEED_API_KEY}
    r = requests.get(endpoint, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    cats = data.get("lighthouseResult", {}).get("categories", {})
    def score(name: str) -> Optional[float]:
        v = cats.get(name, {}).get("score")
        return float(v) * 100.0 if isinstance(v, (int, float)) else None
    return {
        "performance": score("performance"),
        "accessibility": score("accessibility"),
        "best_practices": score("best-practices"),
        "seo": score("seo"),
    }

def crawl_landing_page(url: str) -> Dict[str, Any]:
    try:
        r = requests.get(
            url,
            timeout=25,
            allow_redirects=True,
            headers={"User-Agent":"Mozilla/5.0 PPC-Competitor-Research"}
        )
        status = r.status_code
        final_url = r.url
        html = r.text if "text/html" in (r.headers.get("Content-Type") or "") else ""
    except Exception:
        return {
            "final_url": None, "http_status": None, "title": None,
            "h1": None, "h2s": [], "has_form": 0,
            "pricing_mentions": 0, "financing_mentions": 0,
            "offers": []
        }

    soup = BeautifulSoup(html, "lxml") if html else None
    title = soup.title.get_text(strip=True) if soup and soup.title else None
    h1 = soup.find("h1").get_text(" ", strip=True) if soup and soup.find("h1") else None
    h2s = [h.get_text(" ", strip=True) for h in (soup.find_all("h2") if soup else [])][:10]
    has_form = 1 if soup and soup.find("form") else 0

    text = soup.get_text(" ", strip=True)[:200000] if soup else ""
    pricing_mentions = 1 if re.search(r"\$\s?\d|pricing|price", text, flags=re.I) else 0
    financing_mentions = 1 if re.search(r"financ|apr|\/mo|monthly", text, flags=re.I) else 0
    offers = detect_offers(text)

    return {
        "final_url": final_url,
        "http_status": status,
        "title": title,
        "h1": h1,
        "h2s": h2s,
        "has_form": has_form,
        "pricing_mentions": pricing_mentions,
        "financing_mentions": financing_mentions,
        "offers": offers,
    }


# -----------------------------
# Google Trends (optional; pytrends)
# -----------------------------
def trends_interest_over_time(keywords: List[str], geo: str="US", timeframe: str="today 12-m") -> Dict[str, Any]:
    try:
        from pytrends.request import TrendReq
    except Exception:
        return {"error": "pytrends not installed. Run: pip install pytrends"}

    pytrends = TrendReq(hl="en-US", tz=360)
    kw = [k for k in keywords if k.strip()][:5]  # pytrends limit
    if not kw:
        return {"error": "No keywords"}
    pytrends.build_payload(kw, cat=0, timeframe=timeframe, geo=geo, gprop="")
    df = pytrends.interest_over_time()
    if df is None or df.empty:
        return {"series": []}
    df = df.drop(columns=[c for c in df.columns if c == "isPartial"], errors="ignore")
    series = []
    for col in df.columns:
        series.append({"keyword": col, "points": [{"t": idx.isoformat(), "v": int(v)} for idx, v in df[col].items()]})
    return {"series": series, "geo": geo, "timeframe": timeframe}


# -----------------------------
# Inserts / queries
# -----------------------------
def create_job(name: str) -> int:
    conn = db()
    now = now_utc_iso()
    cur = conn.cursor()
    cur.execute("INSERT INTO research_jobs (created_at_utc, name) VALUES (?, ?)", (now, name))
    conn.commit()
    jid = cur.lastrowid
    conn.close()
    return int(jid)

def list_jobs() -> List[sqlite3.Row]:
    conn = db()
    rows = conn.execute("SELECT * FROM research_jobs ORDER BY id DESC LIMIT 100").fetchall()
    conn.close()
    return rows

def get_job(job_id: int) -> sqlite3.Row:
    conn = db()
    row = conn.execute("SELECT * FROM research_jobs WHERE id=?", (job_id,)).fetchone()
    conn.close()
    if not row:
        raise KeyError("Job not found")
    return row

def add_targets(job_id: int, keywords: List[str], location_input: str, gl: str="us", hl: str="en") -> int:
    serp_loc = normalize_location(location_input)
    conn = db()
    now = now_utc_iso()
    added = 0
    for kw in keywords:
        kw2 = kw.strip()
        if not kw2:
            continue
        conn.execute(
            "INSERT INTO targets (created_at_utc, job_id, keyword, location_input, serp_location, gl, hl) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now, job_id, kw2, location_input, serp_loc, gl, hl)
        )
        added += 1
    conn.commit()
    conn.close()
    return added

def list_targets(job_id: int) -> List[sqlite3.Row]:
    conn = db()
    rows = conn.execute(
        "SELECT * FROM targets WHERE job_id=? ORDER BY id DESC LIMIT 500",
        (job_id,)
    ).fetchall()
    conn.close()
    return rows

def get_target(target_id: int) -> sqlite3.Row:
    conn = db()
    row = conn.execute("SELECT * FROM targets WHERE id=?", (target_id,)).fetchone()
    conn.close()
    if not row:
        raise KeyError("Target not found")
    return row

def insert_run(target_id: int, device: str, serp_json: Dict[str, Any]) -> int:
    conn = db()
    now = now_utc_iso()
    serpapi_search_id = None
    if isinstance(serp_json.get("search_metadata"), dict):
        serpapi_search_id = serp_json["search_metadata"].get("id")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO runs (created_at_utc, target_id, device, serpapi_search_id, raw_json) VALUES (?, ?, ?, ?, ?)",
        (now, target_id, device, serpapi_search_id, json.dumps(serp_json))
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return int(rid)

def insert_ads(run_id: int, ads: List[Dict[str, Any]]) -> None:
    conn = db()
    for ad in ads:
        conn.execute(
            """
            INSERT INTO ads (run_id, position, block, advertiser, displayed_link, destination_link, headline, description, offers_json, extensions_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                ad.get("position"),
                ad.get("block"),
                ad.get("advertiser"),
                ad.get("displayed_link"),
                ad.get("destination_link"),
                ad.get("headline"),
                ad.get("description"),
                json.dumps(ad.get("offers") or []),
                json.dumps(ad.get("extensions")) if ad.get("extensions") else None,
            )
        )
    conn.commit()
    conn.close()


# -----------------------------
# Competitor aggregates (SOV proxies)
# -----------------------------
def aggregate_competitors(job_id: int, days: int=7, device: str="all") -> List[Dict[str, Any]]:
    conn = db()
    params: List[Any] = [job_id, f"-{int(days)} days"]
    device_clause = ""
    if device in ("desktop", "mobile"):
        device_clause = "AND runs.device=?"
        params.append(device)

    rows = conn.execute(
        f"""
        SELECT ads.advertiser AS advertiser,
               COUNT(*) AS appearances,
               SUM(CASE WHEN ads.block='top_ads' THEN 1 ELSE 0 END) AS top_appearances,
               SUM(CASE WHEN ads.block='bottom_ads' THEN 1 ELSE 0 END) AS bottom_appearances
        FROM ads
        JOIN runs ON ads.run_id = runs.id
        JOIN targets ON runs.target_id = targets.id
        WHERE targets.job_id=?
          AND datetime(runs.created_at_utc) >= datetime('now', ?)
          {device_clause}
        GROUP BY ads.advertiser
        ORDER BY appearances DESC
        LIMIT 300
        """,
        params
    ).fetchall()
    conn.close()

    out = []
    for r in rows:
        appearances = int(r["appearances"])
        top = int(r["top_appearances"] or 0)
        bottom = int(r["bottom_appearances"] or 0)
        out.append({
            "advertiser": r["advertiser"],
            "appearances": appearances,
            "top_ads_share": (top / appearances) if appearances else 0.0,
            "bottom_ads_share": (bottom / appearances) if appearances else 0.0,
        })
    return out

def competitor_time_series(job_id: int, advertiser: str, days: int=30, device: str="all") -> Dict[str, Any]:
    conn = db()
    params: List[Any] = [job_id, advertiser, f"-{int(days)} days"]
    device_clause = ""
    if device in ("desktop", "mobile"):
        device_clause = "AND runs.device=?"
        params.append(device)

    rows = conn.execute(
        f"""
        SELECT substr(runs.created_at_utc, 1, 10) AS d,
               COUNT(*) AS appearances,
               SUM(CASE WHEN ads.block='top_ads' THEN 1 ELSE 0 END) AS top_count,
               SUM(CASE WHEN ads.block='bottom_ads' THEN 1 ELSE 0 END) AS bottom_count
        FROM ads
        JOIN runs ON ads.run_id = runs.id
        JOIN targets ON runs.target_id = targets.id
        WHERE targets.job_id=?
          AND ads.advertiser=?
          AND datetime(runs.created_at_utc) >= datetime('now', ?)
          {device_clause}
        GROUP BY d
        ORDER BY d ASC
        """,
        params
    ).fetchall()
    conn.close()

    points = []
    for r in rows:
        points.append({
            "date": r["d"],
            "appearances": int(r["appearances"]),
            "top": int(r["top_count"] or 0),
            "bottom": int(r["bottom_count"] or 0),
        })
    return {"advertiser": advertiser, "days": days, "device": device, "points": points}

def competitor_today_week_counts(job_id: int, advertiser: str) -> Dict[str, int]:
    today = date.today().isoformat()
    week_start = (date.today() - timedelta(days=7)).isoformat()

    conn = db()
    today_count = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM ads
        JOIN runs ON ads.run_id=runs.id
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
          AND ads.advertiser=?
          AND substr(runs.created_at_utc,1,10)=?
        """,
        (job_id, advertiser, today)
    ).fetchone()["c"]

    week_count = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM ads
        JOIN runs ON ads.run_id=runs.id
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
          AND ads.advertiser=?
          AND substr(runs.created_at_utc,1,10) >= ?
        """,
        (job_id, advertiser, week_start)
    ).fetchone()["c"]

    conn.close()
    return {"today": int(today_count), "this_week": int(week_count)}

def competitor_diff(job_id: int) -> Dict[str, Any]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    this_week_start = today - timedelta(days=7)
    last_week_start = today - timedelta(days=14)

    conn = db()

    def advertisers_in_range(start: date, end: date) -> set:
        rows = conn.execute(
            """
            SELECT DISTINCT ads.advertiser
            FROM ads
            JOIN runs ON ads.run_id=runs.id
            JOIN targets ON runs.target_id=targets.id
            WHERE targets.job_id=?
              AND substr(runs.created_at_utc, 1, 10) >= ?
              AND substr(runs.created_at_utc, 1, 10) <= ?
            """,
            (job_id, start.isoformat(), end.isoformat())
        ).fetchall()
        return {r[0] for r in rows}

    todays = advertisers_in_range(today, today)
    ydays = advertisers_in_range(yesterday, yesterday)
    new_today = sorted(list(todays - ydays))

    def counts_in_range(start: date, end: date) -> Dict[str, int]:
        rows = conn.execute(
            """
            SELECT ads.advertiser, COUNT(*) AS c
            FROM ads
            JOIN runs ON ads.run_id=runs.id
            JOIN targets ON runs.target_id=targets.id
            WHERE targets.job_id=?
              AND substr(runs.created_at_utc, 1, 10) >= ?
              AND substr(runs.created_at_utc, 1, 10) <= ?
            GROUP BY ads.advertiser
            """,
            (job_id, start.isoformat(), end.isoformat())
        ).fetchall()
        return {r["advertiser"]: int(r["c"]) for r in rows}

    this_week = counts_in_range(this_week_start, today)
    last_week = counts_in_range(last_week_start, yesterday)

    increased = []
    for adv, c in this_week.items():
        prev = last_week.get(adv, 0)
        if c > prev and (c - prev) >= 3:
            increased.append({"advertiser": adv, "this_week": c, "last_week": prev, "delta": c - prev})
    increased.sort(key=lambda x: x["delta"], reverse=True)

    conn.close()
    return {
        "new_today": new_today[:80],
        "increased_this_week": increased[:80],
        "today": today.isoformat(),
        "yesterday": yesterday.isoformat(),
    }

def rough_monthly_spend_estimate(appearances: int, cpc_assumption: float=8.0, clicks_per_appearance: float=0.25) -> float:
    return round(float(appearances) * float(clicks_per_appearance) * float(cpc_assumption), 2)


# -----------------------------
# Ad search API for competitor drilldown
# -----------------------------
def competitor_ads(job_id: int, advertiser: str, days: int=30, device: str="all", offer: str="") -> List[Dict[str, Any]]:
    conn = db()
    params: List[Any] = [job_id, advertiser, f"-{int(days)} days"]
    device_clause = ""
    if device in ("desktop", "mobile"):
        device_clause = "AND runs.device=?"
        params.append(device)

    offer_clause = ""
    if offer:
        offer_clause = "AND ads.offers_json LIKE ?"
        params.append(f"%{offer}%")

    rows = conn.execute(
        f"""
        SELECT ads.*,
               runs.created_at_utc,
               runs.device
        FROM ads
        JOIN runs ON ads.run_id=runs.id
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
          AND ads.advertiser=?
          AND datetime(runs.created_at_utc) >= datetime('now', ?)
          {device_clause}
          {offer_clause}
        ORDER BY ads.id DESC
        LIMIT 500
        """,
        params
    ).fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append({
            "ad_id": r["id"],
            "created_at_utc": r["created_at_utc"],
            "device": r["device"],
            "block": r["block"],
            "position": r["position"],
            "headline": r["headline"],
            "description": r["description"],
            "displayed_link": r["displayed_link"],
            "destination_link": r["destination_link"],
            "offers": json.loads(r["offers_json"] or "[]"),
            "extensions": json.loads(r["extensions_json"] or "null"),
        })
    return out


# -----------------------------
# Geo heatmap: appearance-rate per location
# -----------------------------
def geo_heatmap(job_id: int, days: int = 30, device: str = "all", block: str = "all", advertiser: str = "") -> Dict[str, Any]:
    conn = db()
    synced_at = now_utc_iso()

    device_clause = ""
    block_clause = ""
    adv_clause = ""
    params_runs: List[Any] = [job_id, f"-{int(days)} days"]
    params_ads: List[Any] = [job_id, f"-{int(days)} days"]

    if device in ("desktop", "mobile"):
        device_clause = "AND runs.device=?"
        params_runs.append(device)
        params_ads.append(device)

    if block in ("top_ads", "bottom_ads"):
        block_clause = "AND ads.block=?"
        params_ads.append(block)

    if advertiser.strip():
        adv_clause = "AND ads.advertiser=?"
        params_ads.append(advertiser.strip())

    run_rows = conn.execute(
        f"""
        SELECT targets.location_input AS loc, COUNT(*) AS run_count
        FROM runs
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
          AND datetime(runs.created_at_utc) >= datetime('now', ?)
          {device_clause}
        GROUP BY loc
        ORDER BY loc ASC
        """,
        params_runs
    ).fetchall()
    run_count_by_loc = {r["loc"]: int(r["run_count"]) for r in run_rows}

    ads_rows = conn.execute(
        f"""
        SELECT targets.location_input AS loc,
               ads.advertiser AS advertiser,
               COUNT(*) AS appearances,
               SUM(CASE WHEN ads.block='top_ads' THEN 1 ELSE 0 END) AS top_count,
               SUM(CASE WHEN ads.block='bottom_ads' THEN 1 ELSE 0 END) AS bottom_count
        FROM ads
        JOIN runs ON ads.run_id=runs.id
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
          AND datetime(runs.created_at_utc) >= datetime('now', ?)
          {device_clause}
          {block_clause}
          {adv_clause}
        GROUP BY loc, advertiser
        """,
        params_ads
    ).fetchall()

    loc_map: Dict[str, List[Dict[str, Any]]] = {loc: [] for loc in run_count_by_loc.keys()}
    for r in ads_rows:
        loc = r["loc"]
        denom = run_count_by_loc.get(loc, 0)
        appearances = int(r["appearances"])
        rate = (appearances / denom) if denom else 0.0
        loc_map.setdefault(loc, []).append({
            "advertiser": r["advertiser"],
            "appearances": appearances,
            "run_count": denom,
            "appearance_rate": rate,
            "top_count": int(r["top_count"] or 0),
            "bottom_count": int(r["bottom_count"] or 0),
        })

    TOP_N = 10 if not advertiser.strip() else 1
    for loc in list(loc_map.keys()):
        loc_map[loc].sort(key=lambda x: x["appearance_rate"], reverse=True)
        loc_map[loc] = loc_map[loc][:TOP_N]

    conn.close()
    return {
        "synced_at_utc": synced_at,
        "job_id": job_id,
        "days": days,
        "device": device,
        "block": block,
        "advertiser_filter": advertiser.strip() or None,
        "locations": [{"loc": loc, "run_count": run_count_by_loc.get(loc, 0), "rows": loc_map.get(loc, [])} for loc in sorted(run_count_by_loc.keys())]
    }


# -----------------------------
# Routes: pages
# -----------------------------
@APP.get("/")
def index():
    jobs = list_jobs()
    return render_template("index.html", jobs=jobs)

@APP.post("/jobs")
def create_job_route():
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Job name is required.")
        return redirect(url_for("index"))
    jid = create_job(name)
    flash(f"Created job. Sync: {fmt_sync_mm(now_utc_iso())}")
    return redirect(url_for("research", job_id=jid))

@APP.get("/research/<int:job_id>")
def research(job_id: int):
    job = get_job(job_id)
    targets = list_targets(job_id)
    return render_template("research.html", job=job, targets=targets)

@APP.get("/research/<int:job_id>/geo")
def geo_page(job_id: int):
    job = get_job(job_id)
    return render_template("geo.html", job=job)

@APP.get("/research/<int:job_id>/competitor/<path:advertiser>")
def competitor_view(job_id: int, advertiser: str):
    days = int(request.args.get("days", "30"))
    device = request.args.get("device", "all")

    series = competitor_time_series(job_id, advertiser, days=days, device=device)
    total = sum(p["appearances"] for p in series["points"])
    top = sum(p["top"] for p in series["points"])
    bottom = sum(p["bottom"] for p in series["points"])

    spend_scenario = {
        "cpc_assumption": float(request.args.get("cpc", "8.0")),
        "clicks_per_appearance": float(request.args.get("cpa", "0.25")),
    }
    monthly_spend = rough_monthly_spend_estimate(total, spend_scenario["cpc_assumption"], spend_scenario["clicks_per_appearance"])

    dayweek = competitor_today_week_counts(job_id, advertiser)

    return render_template(
        "competitor.html",
        job=get_job(job_id),
        advertiser=advertiser,
        days=days,
        device=device,
        series=series,
        total=total,
        top=top,
        bottom=bottom,
        top_share=(top/total if total else 0.0),
        bottom_share=(bottom/total if total else 0.0),
        spend_scenario=spend_scenario,
        monthly_spend=monthly_spend,
        dayweek=dayweek,
        offer_tags=list(OFFER_PATTERNS.keys()),
        synced_at_utc=now_utc_iso(),
    )

@APP.post("/research/<int:job_id>/targets")
def add_targets_route(job_id: int):
    kw_blob = (request.form.get("keywords") or "").strip()
    location_input = (request.form.get("location") or "").strip()
    gl = (request.form.get("gl") or "us").strip()
    hl = (request.form.get("hl") or "en").strip()
    if not kw_blob or not location_input:
        flash("Keywords and location are required.")
        return redirect(url_for("research", job_id=job_id))

    keywords = [k.strip() for k in re.split(r"[\n,]+", kw_blob) if k.strip()]
    added = add_targets(job_id, keywords, location_input, gl=gl, hl=hl)
    flash(f"Added {added} target(s). Sync: {fmt_sync_mm(now_utc_iso())}")
    return redirect(url_for("research", job_id=job_id))

@APP.post("/targets/<int:target_id>/run")
def run_target(target_id: int):
    t = get_target(target_id)
    try:
        for device in ["desktop", "mobile"]:
            j = serpapi_search_google(t["keyword"], t["serp_location"], device=device, gl=t["gl"], hl=t["hl"])
            ads = extract_ads(j)
            rid = insert_run(target_id, device=device, serp_json=j)
            insert_ads(rid, ads)
        flash(f"Snapshot saved (desktop + mobile). Sync: {fmt_sync_mm(now_utc_iso())}")
    except Exception as e:
        flash(f"Run failed: {e}")
    return redirect(url_for("research", job_id=t["job_id"]))

@APP.get("/research/<int:job_id>/export.csv")
def export_job_csv(job_id: int):
    conn = db()
    rows = conn.execute(
        """
        SELECT targets.keyword, targets.serp_location, runs.created_at_utc, runs.device,
               ads.block, ads.position, ads.advertiser, ads.headline, ads.description,
               ads.displayed_link, ads.destination_link, ads.offers_json, ads.extensions_json
        FROM ads
        JOIN runs ON ads.run_id=runs.id
        JOIN targets ON runs.target_id=targets.id
        WHERE targets.job_id=?
        ORDER BY runs.created_at_utc DESC
        """,
        (job_id,)
    ).fetchall()
    conn.close()

    out = StringIO()
    w = csv.writer(out)
    w.writerow([
        "keyword","serp_location","run_time_utc","device","block","position",
        "advertiser","headline","description","displayed_link","destination_link","offers_json","extensions_json"
    ])
    for r in rows:
        w.writerow([r[c] for c in r.keys()])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=ppc_job_{job_id}_ads.csv"}
    )


# -----------------------------
# Routes: JSON APIs (with synced_at_utc)
# -----------------------------
@APP.get("/api/research/<int:job_id>/competitors")
def competitors_api(job_id: int):
    synced_at = now_utc_iso()
    days = int(request.args.get("days", "7"))
    device = request.args.get("device", "all")
    comp = aggregate_competitors(job_id, days=days, device=device)
    diffs = competitor_diff(job_id)
    return jsonify({
        "synced_at_utc": synced_at,
        "job_id": job_id,
        "days": days,
        "device": device,
        "competitors": comp,
        "diffs": diffs,
    })

@APP.get("/api/research/<int:job_id>/geo")
def geo_api(job_id: int):
    days = int(request.args.get("days", "30"))
    device = (request.args.get("device", "all")).strip()
    block = (request.args.get("block", "all")).strip()
    advertiser = (request.args.get("advertiser", "")).strip()
    return jsonify(geo_heatmap(job_id, days=days, device=device, block=block, advertiser=advertiser))

@APP.get("/api/research/<int:job_id>/competitor/<path:advertiser>/ads")
def competitor_ads_api(job_id: int, advertiser: str):
    synced_at = now_utc_iso()
    days = int(request.args.get("days", "30"))
    device = request.args.get("device", "all")
    offer = (request.args.get("offer") or "").strip()
    ads = competitor_ads(job_id, advertiser, days=days, device=device, offer=offer)
    return jsonify({"synced_at_utc": synced_at, "advertiser": advertiser, "ads": ads})

@APP.post("/api/ad/<int:ad_id>/crawl")
def crawl_ad_route(ad_id: int):
    synced_at = now_utc_iso()
    conn = db()
    ad = conn.execute(
        "SELECT id, destination_link FROM ads WHERE id=?",
        (ad_id,)
    ).fetchone()
    if not ad:
        conn.close()
        return jsonify({"synced_at_utc": synced_at, "error": "Ad not found"}), 404

    url = ad["destination_link"]
    if not url:
        conn.close()
        return jsonify({"synced_at_utc": synced_at, "error": "No destination_link for this ad"}), 400

    lp = crawl_landing_page(url)
    psi_mobile = fetch_pagespeed(url, strategy="mobile")

    conn.execute(
        """
        INSERT OR REPLACE INTO landing_pages
          (ad_id, fetched_at_utc, final_url, http_status, title, h1, h2s_json, has_form,
           pricing_mentions, financing_mentions, offers_json,
           pagespeed_performance, pagespeed_accessibility, pagespeed_best_practices, pagespeed_seo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ad_id, synced_at,
            lp.get("final_url"), lp.get("http_status"),
            lp.get("title"), lp.get("h1"), json.dumps(lp.get("h2s") or []),
            int(lp.get("has_form") or 0),
            int(lp.get("pricing_mentions") or 0),
            int(lp.get("financing_mentions") or 0),
            json.dumps(lp.get("offers") or []),
            psi_mobile.get("performance"),
            psi_mobile.get("accessibility"),
            psi_mobile.get("best_practices"),
            psi_mobile.get("seo"),
        )
    )
    conn.commit()
    row = conn.execute("SELECT * FROM landing_pages WHERE ad_id=?", (ad_id,)).fetchone()
    conn.close()
    out = dict(row)
    out["synced_at_utc"] = synced_at
    return jsonify(out)

@APP.get("/api/atc/list")
def atc_list_api():
    synced_at = now_utc_iso()
    advertiser = (request.args.get("advertiser") or "").strip()
    region = (request.args.get("region") or "US").strip()
    page = int(request.args.get("page", "1"))
    if not advertiser:
        return jsonify({"synced_at_utc": synced_at, "error": "advertiser is required"}), 400

    raw = serpapi_atc_list_ads(advertiser, region=region, page=page)

    conn = db()
    conn.execute(
        "INSERT INTO atc_cache (fetched_at_utc, advertiser_query, region, endpoint, raw_json) VALUES (?, ?, ?, ?, ?)",
        (synced_at, advertiser, region, "list", json.dumps(raw))
    )
    conn.commit()
    conn.close()

    raw_list = None
    for k in ["ads", "ad_creatives", "creatives", "results", "items"]:
        if isinstance(raw.get(k), list):
            raw_list = raw.get(k)
            break
    if raw_list is None:
        raw_list = []

    creatives = [normalize_atc_list_item(x) for x in raw_list if isinstance(x, dict)]

    return jsonify({
        "synced_at_utc": synced_at,
        "advertiser_query": advertiser,
        "region": region,
        "page": page,
        "count": len(creatives),
        "creatives": creatives,
        "raw_keys": list(raw.keys()),
    })

@APP.get("/api/atc/details")
def atc_details_api():
    synced_at = now_utc_iso()
    ad_id = (request.args.get("ad_id") or "").strip()
    region = (request.args.get("region") or "US").strip()
    if not ad_id:
        return jsonify({"synced_at_utc": synced_at, "error": "ad_id is required"}), 400

    raw = serpapi_atc_ad_details(ad_id, region=region)

    conn = db()
    conn.execute(
        "INSERT INTO atc_cache (fetched_at_utc, advertiser_query, region, endpoint, raw_json) VALUES (?, ?, ?, ?, ?)",
        (synced_at, ad_id, region, "details", json.dumps(raw))
    )
    conn.commit()
    conn.close()

    normalized = normalize_atc_details(raw)

    return jsonify({
        "synced_at_utc": synced_at,
        "region": region,
        "normalized": normalized,
    })

@APP.get("/api/trends")
def trends_api():
    synced_at = now_utc_iso()
    kw_blob = (request.args.get("keywords") or "").strip()
    geo = (request.args.get("geo") or "US").strip()
    timeframe = (request.args.get("timeframe") or "today 12-m").strip()
    keywords = [k.strip() for k in re.split(r"[\n,]+", kw_blob) if k.strip()]
    data = trends_interest_over_time(keywords, geo=geo, timeframe=timeframe)
    data["synced_at_utc"] = synced_at
    return jsonify(data)


def main():
    init_db()
    APP.run(debug=True, port=int(os.environ.get("PORT", "5000")))

if __name__ == "__main__":
    main()