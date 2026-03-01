"""Creative threat detection: poll ATC for watchlist, diff vs previous snapshot, create alerts."""
import json
import logging
from datetime import datetime, timezone

from db import cursor, utc_now
from services.atc_service import atc_list

logger = logging.getLogger(__name__)


def list_watchlist(job_id: int = None):
    with cursor() as cur:
        if job_id is not None:
            cur.execute(
                "SELECT id, job_id, advertiser_domain, region, last_atc_snapshot_id, last_poll_at FROM competitor_watchlist WHERE job_id = ?",
                (job_id,),
            )
        else:
            cur.execute("SELECT id, job_id, advertiser_domain, region, last_atc_snapshot_id, last_poll_at FROM competitor_watchlist ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


def add_to_watchlist(job_id: int, advertiser_domain: str, region: str = "US"):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO competitor_watchlist (job_id, advertiser_domain, region) VALUES (?,?,?)",
            (job_id, advertiser_domain, region),
        )
        return cur.lastrowid


def poll_watchlist_and_alert():
    """For each watchlist entry, fetch ATC, diff with last snapshot, insert creative_alerts if new/removed."""
    now = utc_now()
    for w in list_watchlist():
        try:
            out = atc_list(w["advertiser_domain"], w["region"], 1)
            creatives = out.get("creatives") or []
            new_snap_id = None
            with cursor() as cur:
                cur.execute(
                    "SELECT id, raw_json FROM atc_snapshots WHERE advertiser = ? AND region = ? ORDER BY synced_at_utc DESC LIMIT 1",
                    (w["advertiser_domain"], w["region"]),
                )
                r = cur.fetchone()
            if r:
                new_snap_id = r["id"]
            prev_snap_id = w.get("last_atc_snapshot_id")
            if prev_snap_id and new_snap_id:
                with cursor() as cur:
                    cur.execute("SELECT raw_json FROM atc_snapshots WHERE id = ?", (prev_snap_id,))
                    prev_r = cur.fetchone()
                prev_data = json.loads(prev_r["raw_json"]) if prev_r and prev_r["raw_json"] else {}
                prev_creatives = set()
                for key in ("ads", "creatives", "results"):
                    if isinstance(prev_data.get(key), list):
                        for c in prev_data[key]:
                            if isinstance(c, dict):
                                cid = c.get("ad_id") or c.get("creative_id") or c.get("id")
                                if cid:
                                    prev_creatives.add(str(cid))
                        break
                new_ids = {str(c.get("ad_id") or c.get("creative_id") or c.get("id")) for c in creatives if c.get("ad_id") or c.get("creative_id") or c.get("id")}
                added = new_ids - prev_creatives
                removed = prev_creatives - new_ids
                if added:
                    with cursor() as cur:
                        cur.execute(
                            "INSERT INTO creative_alerts (watchlist_id, type, previous_snapshot_id, new_snapshot_id, diff_summary_json, created_at) VALUES (?,?,?,?,?,?)",
                            (w["id"], "new_creative", prev_snap_id, new_snap_id, json.dumps({"added": list(added)[:20]}), now),
                        )
                if removed:
                    with cursor() as cur:
                        cur.execute(
                            "INSERT INTO creative_alerts (watchlist_id, type, previous_snapshot_id, new_snapshot_id, diff_summary_json, created_at) VALUES (?,?,?,?,?,?)",
                            (w["id"], "removed_creative", prev_snap_id, new_snap_id, json.dumps({"removed": list(removed)[:20]}), now),
                        )
            with cursor() as cur:
                cur.execute(
                    "UPDATE competitor_watchlist SET last_atc_snapshot_id = ?, last_poll_at = ? WHERE id = ?",
                    (new_snap_id, now, w["id"]),
                )
        except Exception as e:
            logger.exception("Creative threat poll failed for watchlist %s: %s", w["id"], e)
