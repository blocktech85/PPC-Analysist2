"""Competitor budget exhaustion: hourly presence tracking for high-value targets."""
import logging
from datetime import datetime, timezone

from db import cursor, utc_now

logger = logging.getLogger(__name__)


def record_presence(target_id: int, advertiser: str, appeared: bool):
    now = utc_now()
    with cursor() as cur:
        cur.execute(
            "INSERT INTO presence_snapshots (target_id, advertiser, timestamp_utc, appeared) VALUES (?,?,?,?)",
            (target_id, advertiser, now, 1 if appeared else 0),
        )


def run_budget_tracking_for_target(target_id: int) -> bool:
    """Run one presence snapshot for a single target. Target must have budget_tracking_enabled=1. Returns True if run, False if skipped."""
    with cursor() as cur:
        cur.execute(
            "SELECT id, job_id, keyword, location_input, serp_location, gl, hl FROM targets WHERE id = ? AND budget_tracking_enabled = 1",
            (target_id,),
        )
        t = cur.fetchone()
    if not t:
        return False
    t = dict(t)
    try:
        from services.serpapi_service import _search, _location_for_serpapi
        from config import SERPAPI_API_KEY
        if not SERPAPI_API_KEY:
            return False
        loc = t["serp_location"] or t["location_input"]
        loc = _location_for_serpapi(loc, t.get("gl") or "us")
        result = _search({
            "q": t["keyword"],
            "engine": "google",
            "location": loc,
            "gl": t["gl"] or "us",
            "hl": t["hl"] or "en",
            "device": "desktop",
        })
        ads = result.get("ads") or result.get("paid") or []
        advertisers = set()
        for ad in ads:
            if isinstance(ad, dict):
                link = ad.get("displayed_link") or ad.get("link") or ad.get("destination_link") or ""
                if link:
                    from services.serpapi_service import _domain_from_link
                    advertisers.add(_domain_from_link(link))
        for adv in advertisers:
            record_presence(t["id"], adv, True)
        return True
    except Exception as e:
        logger.exception("Budget tracking failed for target %s: %s", t["id"], e)
        return False


def run_budget_tracking_cycle():
    """For each target with budget_tracking_enabled=1, run SERP, record which advertisers appeared."""
    with cursor() as cur:
        cur.execute("SELECT id, job_id, keyword, location_input, serp_location, gl, hl FROM targets WHERE budget_tracking_enabled = 1")
        targets = [dict(r) for r in cur.fetchall()]
    for t in targets:
        try:
            from services.serpapi_service import _search, _location_for_serpapi
            from config import SERPAPI_API_KEY
            if not SERPAPI_API_KEY:
                continue
            loc = t["serp_location"] or t["location_input"]
            loc = _location_for_serpapi(loc, t.get("gl") or "us")
            result = _search({
                "q": t["keyword"],
                "engine": "google",
                "location": loc,
                "gl": t["gl"] or "us",
                "hl": t["hl"] or "en",
                "device": "desktop",
            })
            ads = result.get("ads") or result.get("paid") or []
            advertisers = set()
            for ad in ads:
                if isinstance(ad, dict):
                    link = ad.get("displayed_link") or ad.get("link") or ad.get("destination_link") or ""
                    if link:
                        from services.serpapi_service import _domain_from_link
                        advertisers.add(_domain_from_link(link))
            for adv in advertisers:
                record_presence(t["id"], adv, True)
        except Exception as e:
            logger.exception("Budget tracking failed for target %s: %s", t["id"], e)


def get_presence_24h(target_id: int) -> list:
    """Return list of {advertiser, hours_present, first_hour, last_hour} for last 24h."""
    with cursor() as cur:
        cur.execute(
            """SELECT advertiser, strftime('%H', timestamp_utc) AS hour, MAX(appeared) AS appeared
            FROM presence_snapshots
            WHERE target_id = ? AND timestamp_utc >= datetime('now', '-1 day')
            GROUP BY advertiser, strftime('%H', timestamp_utc)"""
            ,
            (target_id,),
        )
        rows = cur.fetchall()
    by_adv = {}
    for r in rows:
        adv = r["advertiser"]
        if adv not in by_adv:
            by_adv[adv] = set()
        if r["appeared"]:
            by_adv[adv].add(int(r["hour"]))
    result = []
    for adv, hours in by_adv.items():
        h_list = sorted(hours)
        result.append({
            "advertiser": adv,
            "hours_present": len(h_list),
            "first_hour": h_list[0] if h_list else None,
            "last_hour": h_list[-1] if h_list else None,
        })
    return result
