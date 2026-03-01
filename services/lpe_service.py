"""Automated LPE: batch PageSpeed for destination URLs, store in metrics time-series."""
import hashlib
import logging
from datetime import datetime, timedelta, timezone

from db import cursor, utc_now
from services.crawl_service import pagespeed_insights

logger = logging.getLogger(__name__)


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def run_lpe_batch_for_job(job_id: int, days: int = 7, throttle_per_url_per_day: bool = True):
    """Collect unique destination URLs from ads in job (last N days), run PageSpeed, store in crawls + metrics."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    with cursor() as cur:
        cur.execute(
            """SELECT DISTINCT a.id, a.destination_link FROM ads a
            WHERE a.job_id = ? AND a.created_at_utc >= ? AND a.destination_link IS NOT NULL AND a.destination_link != '' AND a.destination_link LIKE 'http%'""",
            (job_id, cutoff),
        )
        rows = cur.fetchall()
    seen_url = set()
    urls_to_run = []
    for r in rows:
        url = (r["destination_link"] or "").strip()
        if not url or url in seen_url:
            continue
        seen_url.add(url)
        urls_to_run.append((r["id"], url))
    count = 0
    for ad_id, url in urls_to_run[:50]:
        psi = pagespeed_insights(url)
        if not psi:
            continue
        now = utc_now()
        with cursor() as cur:
            cur.execute(
                """INSERT INTO metrics (timestamp_utc, entity_type, entity_id, metric_name, value)
                VALUES (?,?,?,?,?)""",
                (now, "lpe_url", _url_hash(url), "performance", psi.get("performance")),
            )
            cur.execute(
                """INSERT INTO metrics (timestamp_utc, entity_type, entity_id, metric_name, value)
                VALUES (?,?,?,?,?)""",
                (now, "lpe_url", _url_hash(url), "accessibility", psi.get("accessibility")),
            )
            cur.execute(
                """INSERT INTO metrics (timestamp_utc, entity_type, entity_id, metric_name, value)
                VALUES (?,?,?,?,?)""",
                (now, "lpe_url", _url_hash(url), "best_practices", psi.get("best_practices")),
            )
            cur.execute(
                """INSERT INTO metrics (timestamp_utc, entity_type, entity_id, metric_name, value)
                VALUES (?,?,?,?,?)""",
                (now, "lpe_url", _url_hash(url), "seo", psi.get("seo")),
            )
        count += 1
    return count
