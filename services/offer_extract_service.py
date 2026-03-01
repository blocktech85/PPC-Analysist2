"""Agentic ad copy extraction: LLM extracts structured offers from headlines/descriptions."""
import json
import logging
from datetime import datetime, timezone

from db import cursor, utc_now

logger = logging.getLogger(__name__)


def _gemini_extract_offers(texts: list) -> list:
    """Call Gemini to extract offer entities. Returns list of dicts per ad."""
    try:
        import google.generativeai as genai
        from config import GEMINI_API_KEY

        if not GEMINI_API_KEY:
            return []
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        results = []
        for text in texts:
            prompt = """Extract structured offer entities from this ad copy. Respond with JSON only.
Keys: financing_rate (e.g. "0% APR"), guarantee_text, free_trial_days (number or null), discount_type (e.g. "20% off"), other_promotion (string or null).
If not present use null.

Ad copy:
""" + text[:2000]
            try:
                resp = model.generate_content(prompt)
                raw = resp.text.strip()
                if "```" in raw:
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                obj = json.loads(raw)
                results.append(obj)
            except Exception as e:
                logger.warning("Gemini offer parse failed: %s", e)
                results.append({})
        return results
    except ImportError:
        return []
    except Exception as e:
        logger.exception("Gemini offer extraction failed: %s", e)
        return []


def extract_offers_for_ads(ad_ids: list, source: str = "serp"):
    """Extract offers for given ad ids and store in extracted_offers."""
    if not ad_ids:
        return 0
    placeholders = ",".join("?" * len(ad_ids))
    with cursor() as cur:
        cur.execute(
            f"SELECT id, headline, description FROM ads WHERE id IN ({placeholders})",
            ad_ids,
        )
        ads = cur.fetchall()
    texts = [f"Headline: {a['headline'] or ''}\nDescription: {a['description'] or ''}" for a in ads]
    extracted = _gemini_extract_offers(texts)
    if len(extracted) != len(ads):
        return 0
    now = utc_now()
    with cursor() as cur:
        for i, (ad, ex) in enumerate(zip(ads, extracted)):
            cur.execute(
                """INSERT OR REPLACE INTO extracted_offers (ad_id, source, financing_rate, guarantee_text, free_trial_days, discount_type, other_promotion_json, raw_snippet, model_used, synced_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    ad["id"],
                    source,
                    ex.get("financing_rate"),
                    ex.get("guarantee_text"),
                    ex.get("free_trial_days"),
                    ex.get("discount_type"),
                    json.dumps(ex.get("other_promotion")) if ex.get("other_promotion") else None,
                    texts[i][:500] if i < len(texts) else "",
                    "gemini",
                    now,
                ),
            )
    return len(ads)
