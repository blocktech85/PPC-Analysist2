"""Crawl landing page and PageSpeed Insights."""
import json
import logging
import re

import requests
from bs4 import BeautifulSoup

from config import PAGESPEED_API_KEY
from db import cursor, utc_now

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
OFFER_PATTERNS = re.compile(
    r"\b(free\s*trial|%\s*off|discount|guarantee|financing|0%\s*apr|no\s*payment|save\s*\d+|offer|limited\s*time)\b",
    re.I,
)
PRICING_PATTERNS = re.compile(r"\b(\$\d+|price|pricing|cost|affordable)\b", re.I)
FINANCING_PATTERNS = re.compile(r"\b(financing|0%\s*apr|monthly\s*payment|lease|loan)\b", re.I)


def fetch_page(url: str, timeout: int = 15):
    """Fetch URL and return (final_url, status_code, text)."""
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            allow_redirects=True,
        )
        return r.url, r.status_code, r.text
    except Exception as e:
        logger.exception("Fetch failed for %s: %s", url, e)
        return url, 0, ""


def extract_landing_fields(html: str, base_url: str):
    """Extract title, h1, h2s, form, pricing, financing, offers from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()[:500]
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag and h1_tag.get_text():
        h1 = h1_tag.get_text(strip=True)[:500]
    h2s = [h.get_text(strip=True)[:200] for h in soup.find_all("h2") if h.get_text(strip=True)][:20]
    body_text = soup.get_text(separator=" ", strip=True)[:50000]
    has_form = bool(soup.find("form"))
    pricing_mentions = bool(PRICING_PATTERNS.search(body_text))
    financing_mentions = bool(FINANCING_PATTERNS.search(body_text))
    offers = list(set(OFFER_PATTERNS.findall(body_text)))[:15]
    return {
        "title": title,
        "h1": h1,
        "h2s": h2s,
        "has_form": has_form,
        "pricing_mentions": pricing_mentions,
        "financing_mentions": financing_mentions,
        "offers": offers,
    }


def pagespeed_insights(url: str):
    """Call PageSpeed Insights API. Returns dict with performance, accessibility, best_practices, seo."""
    if not PAGESPEED_API_KEY:
        return {}
    try:
        r = requests.get(
            "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
            params={"url": url, "key": PAGESPEED_API_KEY, "strategy": "mobile"},
            timeout=30,
        )
        if r.status_code != 200:
            return {}
        data = r.json()
        cats = data.get("lighthouseResult", {}).get("categories", {})
        return {
            "performance": cats.get("performance", {}).get("score"),
            "accessibility": cats.get("accessibility", {}).get("score"),
            "best_practices": cats.get("best-practices", {}).get("score"),
            "seo": cats.get("seo", {}).get("score"),
        }
    except Exception as e:
        logger.exception("PageSpeed API failed for %s: %s", url, e)
        return {}


def crawl_and_save(ad_id: int, destination_url: str):
    """Crawl destination URL, run PageSpeed if key set, save to crawls. Returns crawl row dict."""
    final_url, status, html = fetch_page(destination_url)
    fields = extract_landing_fields(html, final_url) if html else {}
    psi = pagespeed_insights(final_url) if final_url.startswith("http") else {}
    now = utc_now()
    with cursor() as cur:
        cur.execute(
            """INSERT INTO crawls (ad_id, destination_url, final_url, http_status, title, h1, h2s_json,
            has_form, pricing_mentions, financing_mentions, offers_json, pagespeed_performance, pagespeed_accessibility,
            pagespeed_best_practices, pagespeed_seo, synced_at_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ad_id,
                destination_url,
                final_url,
                status,
                fields.get("title"),
                fields.get("h1"),
                json.dumps(fields.get("h2s", [])),
                1 if fields.get("has_form") else 0,
                1 if fields.get("pricing_mentions") else 0,
                1 if fields.get("financing_mentions") else 0,
                json.dumps(fields.get("offers", [])),
                psi.get("performance"),
                psi.get("accessibility"),
                psi.get("best_practices"),
                psi.get("seo"),
                now,
            ),
        )
        crawl_id = cur.lastrowid
    return {
        "id": crawl_id,
        "final_url": final_url,
        "http_status": status,
        "title": fields.get("title"),
        "h1": fields.get("h1"),
        "h2s_json": json.dumps(fields.get("h2s", [])),
        "has_form": fields.get("has_form"),
        "pricing_mentions": fields.get("pricing_mentions"),
        "financing_mentions": fields.get("financing_mentions"),
        "offers_json": json.dumps(fields.get("offers", [])),
        "pagespeed_performance": psi.get("performance"),
        "pagespeed_accessibility": psi.get("accessibility"),
        "pagespeed_best_practices": psi.get("best_practices"),
        "pagespeed_seo": psi.get("seo"),
        "synced_at_utc": now,
    }
