"""Brand hijacking and trademark monitoring: scan ads for brand terms, record violations, generate complaint doc."""
import json
import logging
import re
from datetime import datetime, timezone

from db import cursor, utc_now

logger = logging.getLogger(__name__)


def list_brand_assets(job_id: int = None):
    """List brand assets (job_id or global if job_id None)."""
    with cursor() as cur:
        if job_id is not None:
            cur.execute("SELECT id, job_id, term, pattern_type, regex_pattern FROM brand_assets WHERE job_id = ? ORDER BY id", (job_id,))
        else:
            cur.execute("SELECT id, job_id, term, pattern_type, regex_pattern FROM brand_assets WHERE job_id IS NULL ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


def add_brand_asset(job_id=None, term: str = "", pattern_type: str = "literal", regex_pattern: str = None):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO brand_assets (job_id, term, pattern_type, regex_pattern) VALUES (?,?,?,?)",
            (job_id, term, pattern_type, regex_pattern or ""),
        )
        return cur.lastrowid


def _match_asset(text: str, asset: dict) -> list:
    """Return list of (matched_asset, snippet) for text."""
    if not text:
        return []
    text_lower = text.lower()
    term = (asset.get("term") or "").strip()
    pattern_type = asset.get("pattern_type") or "literal"
    regex_pattern = asset.get("regex_pattern") or ""
    out = []
    if pattern_type == "regex" and regex_pattern:
        try:
            for m in re.finditer(regex_pattern, text, re.I):
                out.append((term or regex_pattern[:50], m.group(0)[:200]))
        except re.error:
            pass
    else:
        if term and term.lower() in text_lower:
            start = text_lower.find(term.lower())
            snippet = text[start : start + len(term) + 40]
            out.append((term, snippet))
    return out


def scan_ads_for_brand(job_id: int, since_utc: str = None):
    """Scan all ads for job (since optional) against brand_assets. Insert into trademark_violations."""
    assets = list_brand_assets(job_id) + list_brand_assets(None)
    if not assets:
        return 0
    with cursor() as cur:
        if since_utc:
            cur.execute(
                """SELECT id, advertiser, headline, description, displayed_link, created_at_utc FROM ads
                WHERE job_id = ? AND created_at_utc >= ?""",
                (job_id, since_utc),
            )
        else:
            cur.execute(
                """SELECT id, advertiser, headline, description, displayed_link, created_at_utc FROM ads WHERE job_id = ?""",
                (job_id,),
            )
        ads = cur.fetchall()
    count = 0
    for ad in ads:
        texts = [
            ad["headline"] or "",
            ad["description"] or "",
            ad["displayed_link"] or "",
        ]
        combined = " ".join(texts)
        for asset in assets:
            matches = _match_asset(combined, asset)
            for matched_asset, snippet in matches:
                with cursor() as cur:
                    cur.execute(
                        """SELECT id FROM trademark_violations
                        WHERE job_id = ? AND ad_id = ? AND matched_asset = ? LIMIT 1""",
                        (job_id, ad["id"], matched_asset),
                    )
                    if cur.fetchone():
                        continue
                    cur.execute(
                        """INSERT INTO trademark_violations (job_id, ad_id, advertiser, source, matched_asset, matched_text_snippet, captured_at, status)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        (job_id, ad["id"], ad["advertiser"], "serp", matched_asset, snippet, ad["created_at_utc"], "new"),
                    )
                    count += 1
    return count


def list_violations(job_id: int = None, status: str = None):
    with cursor() as cur:
        if job_id is not None and status:
            cur.execute(
                """SELECT id, job_id, ad_id, advertiser, source, matched_asset, matched_text_snippet, captured_at, reviewed_at, status
                FROM trademark_violations WHERE job_id = ? AND status = ? ORDER BY captured_at DESC""",
                (job_id, status),
            )
        elif job_id is not None:
            cur.execute(
                """SELECT id, job_id, ad_id, advertiser, source, matched_asset, matched_text_snippet, captured_at, reviewed_at, status
                FROM trademark_violations WHERE job_id = ? ORDER BY captured_at DESC""",
                (job_id,),
            )
        else:
            cur.execute(
                """SELECT id, job_id, ad_id, advertiser, source, matched_asset, matched_text_snippet, captured_at, reviewed_at, status
                FROM trademark_violations ORDER BY captured_at DESC LIMIT 500"""
            )
        return [dict(r) for r in cur.fetchall()]


def generate_complaint_doc(violation_ids: list) -> str:
    """Generate Google Ads Trademark Complaint style text."""
    if not violation_ids:
        return "No violations selected."
    placeholders = ",".join("?" * len(violation_ids))
    with cursor() as cur:
        cur.execute(
            f"""SELECT id, job_id, ad_id, advertiser, source, matched_asset, matched_text_snippet, captured_at
            FROM trademark_violations WHERE id IN ({placeholders}) ORDER BY captured_at DESC""",
            violation_ids,
        )
        rows = cur.fetchall()
    lines = [
        "GOOGLE ADS TRADEMARK COMPLAINT – EVIDENCE PACK",
        "Generated: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "",
    ]
    for r in rows:
        lines.append(f"--- Violation #{r['id']} ---")
        lines.append(f"Advertiser: {r['advertiser']}")
        lines.append(f"Matched term: {r['matched_asset']}")
        lines.append(f"Snippet: {r['matched_text_snippet']}")
        lines.append(f"Date captured: {r['captured_at']}")
        lines.append("")
    lines.append("Use this document to support your complaint at https://support.google.com/legal/answer/3110420")
    return "\n".join(lines)


def update_violation_status(violation_id: int, job_id: int, new_status: str) -> bool:
    """Update a violation's status (e.g. 'reviewed', 'dismissed'). Returns True if updated."""
    allowed = {"new", "reviewed", "dismissed", "escalated"}
    if (new_status or "").strip().lower() not in allowed:
        return False
    new_status = new_status.strip().lower()
    with cursor() as cur:
        cur.execute(
            "UPDATE trademark_violations SET status = ?, reviewed_at = datetime('now') WHERE id = ? AND job_id = ?",
            (new_status, violation_id, job_id),
        )
        return cur.rowcount > 0
