"""NLP psychological trigger extraction via Gemini."""
import json
import logging
from datetime import datetime, timezone

from db import cursor, utc_now

logger = logging.getLogger(__name__)

TRIGGERS = [
    "Scarcity",
    "Urgency",
    "Social Proof",
    "Price Anchoring",
    "Quality",
    "Guarantee",
    "Free Trial",
    "Discount",
]


def _gemini_classify(headlines: list, descriptions: list) -> list:
    """Call Gemini to classify ad copy into triggers. Returns list of dicts {trigger_name: score} per ad."""
    try:
        import google.generativeai as genai
        from config import GEMINI_API_KEY

        if not GEMINI_API_KEY:
            return []
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        results = []
        for i, (h, d) in enumerate(zip(headlines, descriptions)):
            text = f"Headline: {h}\nDescription: {d}"
            prompt = f"""Classify the following ad copy into these psychological triggers. For each trigger, respond with 0 or 1 (1 if present).
Triggers: {', '.join(TRIGGERS)}
Respond with a JSON object only, keys are trigger names, values are 0 or 1. Example: {{"Scarcity": 0, "Urgency": 1, ...}}

Ad copy:
{text}"""
            try:
                resp = model.generate_content(prompt)
                raw = resp.text.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                obj = json.loads(raw)
                results.append(obj)
            except Exception as e:
                logger.warning("Gemini parse failed for ad %s: %s", i, e)
                results.append({t: 0 for t in TRIGGERS})
        return results
    except ImportError:
        return []
    except Exception as e:
        logger.exception("Gemini trigger extraction failed: %s", e)
        return []


def extract_triggers_for_ads(ad_ids: list):
    """Fetch ad copy for ad_ids, call Gemini, store in ad_trigger_scores."""
    if not ad_ids:
        return 0
    placeholders = ",".join("?" * len(ad_ids))
    with cursor() as cur:
        cur.execute(
            f"SELECT id, headline, description FROM ads WHERE id IN ({placeholders})",
            ad_ids,
        )
        ads = cur.fetchall()
    headlines = [a["headline"] or "" for a in ads]
    descriptions = [a["description"] or "" for a in ads]
    classifications = _gemini_classify(headlines, descriptions)
    if len(classifications) != len(ads):
        return 0
    now = utc_now()
    with cursor() as cur:
        for ad, scores in zip(ads, classifications):
            for trigger_name, score in scores.items():
                if trigger_name not in TRIGGERS:
                    continue
                cur.execute(
                    """INSERT OR REPLACE INTO ad_trigger_scores (ad_id, trigger_name, score, model_used, synced_at)
                    VALUES (?,?,?,?,?)""",
                    (ad["id"], trigger_name, float(score) if isinstance(score, (int, float)) else 0, "gemini", now),
                )
    return len(ads)


def get_trigger_scores_for_ad(ad_id: int) -> dict:
    """Return {trigger_name: score} for one ad."""
    with cursor() as cur:
        cur.execute("SELECT trigger_name, score FROM ad_trigger_scores WHERE ad_id = ?", (ad_id,))
        return {r["trigger_name"]: r["score"] for r in cur.fetchall()}
