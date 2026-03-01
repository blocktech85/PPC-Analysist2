"""Data access helpers for jobs, targets, ads, aggregates."""
import json
import logging
from datetime import datetime, timedelta, timezone

from db import cursor, utc_now

logger = logging.getLogger(__name__)


def list_jobs():
    with cursor() as cur:
        cur.execute("SELECT id, name, created_at FROM jobs ORDER BY id DESC")
        return [dict(r) for r in cur.fetchall()]


def get_job(job_id: int):
    with cursor() as cur:
        cur.execute("SELECT id, name, created_at FROM jobs WHERE id = ?", (job_id,))
        r = cur.fetchone()
        return dict(r) if r else None


def create_job(name: str):
    with cursor() as cur:
        cur.execute("INSERT INTO jobs (name) VALUES (?)", (name,))
        return cur.lastrowid


def delete_job(job_id: int) -> bool:
    """Delete a job and all related data (targets, snapshots, ads, etc. via FK CASCADE). Returns True if deleted."""
    with cursor() as cur:
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        return cur.rowcount > 0


def list_targets(job_id: int):
    with cursor() as cur:
        cur.execute(
            """SELECT id, job_id, keyword, location_input, serp_location, gl, hl, created_at, budget_tracking_enabled
            FROM targets WHERE job_id = ? ORDER BY id""",
            (job_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_targets_with_last_run(job_id: int):
    """List targets with last_snapshot_utc (max captured_at_utc for that target)."""
    targets = list_targets(job_id)
    with cursor() as cur:
        cur.execute(
            """SELECT t.id, MAX(s.captured_at_utc) AS last_snapshot_utc
            FROM targets t LEFT JOIN serp_snapshots s ON s.target_id = t.id WHERE t.job_id = ?
            GROUP BY t.id""",
            (job_id,),
        )
        by_id = {r["id"]: r["last_snapshot_utc"] for r in cur.fetchall()}
    for t in targets:
        t["last_snapshot_utc"] = by_id.get(t["id"])
    return targets


def get_target(target_id: int):
    with cursor() as cur:
        cur.execute("SELECT * FROM targets WHERE id = ?", (target_id,))
        r = cur.fetchone()
        return dict(r) if r else None


def add_targets(job_id: int, keywords: list, location_input: str, gl: str, hl: str):
    """Insert multiple targets. serp_location set same as location_input for now. Returns count inserted."""
    inserted = 0
    with cursor() as cur:
        for kw in keywords:
            kw = (kw or "").strip()
            if not kw:
                continue
            cur.execute(
                """INSERT INTO targets (job_id, keyword, location_input, serp_location, gl, hl)
                VALUES (?,?,?,?,?,?)""",
                (job_id, kw, location_input, location_input, gl or "us", hl or "en"),
            )
            inserted += 1
    return inserted


def _parse_cutoff(days: int):
    return (datetime.now(timezone.utc) - timedelta(days=int(days))).strftime("%Y-%m-%d")


def get_competitors(job_id: int, days: int, device: str):
    """Return list of {advertiser, appearances, top_ads_share, bottom_ads_share} and synced_at_utc, diffs."""
    cutoff = _parse_cutoff(days)
    with cursor() as cur:
        if device == "all":
            cur.execute(
                """SELECT advertiser,
                COUNT(*) AS appearances,
                SUM(CASE WHEN block = 'top' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS top_ads_share,
                SUM(CASE WHEN block = 'bottom' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS bottom_ads_share
                FROM ads WHERE job_id = ? AND created_at_utc >= ?
                GROUP BY advertiser ORDER BY appearances DESC""",
                (job_id, cutoff),
            )
        else:
            cur.execute(
                """SELECT advertiser,
                COUNT(*) AS appearances,
                SUM(CASE WHEN block = 'top' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS top_ads_share,
                SUM(CASE WHEN block = 'bottom' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS bottom_ads_share
                FROM ads WHERE job_id = ? AND created_at_utc >= ? AND device = ?
                GROUP BY advertiser ORDER BY appearances DESC""",
                (job_id, cutoff, device),
            )
        rows = cur.fetchall()
    competitors = [dict(r) for r in rows]
    # Latest snapshot time as synced_at
    with cursor() as cur:
        cur.execute(
            """SELECT MAX(captured_at_utc) FROM serp_snapshots s
            JOIN targets t ON s.target_id = t.id WHERE t.job_id = ?""",
            (job_id,),
        )
        r = cur.fetchone()
    synced = r[0] if r and r[0] else None
    # Simple diffs: new today vs yesterday, increased this week (placeholder)
    today = datetime.now(timezone.utc).date().isoformat()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    with cursor() as cur:
        cur.execute(
            """SELECT advertiser FROM ads WHERE job_id = ? AND date(created_at_utc) = ?
            GROUP BY advertiser""",
            (job_id, today),
        )
        today_advertisers = {row[0] for row in cur.fetchall()}
        cur.execute(
            """SELECT advertiser FROM ads WHERE job_id = ? AND date(created_at_utc) = ?
            GROUP BY advertiser""",
            (job_id, yesterday),
        )
        yesterday_advertisers = {row[0] for row in cur.fetchall()}
    new_today = list(today_advertisers - yesterday_advertisers)[:12]
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    with cursor() as cur:
        cur.execute(
            """SELECT advertiser, COUNT(*) AS cnt FROM ads
            WHERE job_id = ? AND created_at_utc >= ? GROUP BY advertiser""",
            (job_id, week_ago),
        )
        this_week = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute(
            """SELECT advertiser, COUNT(*) AS cnt FROM ads
            WHERE job_id = ? AND created_at_utc < ? AND created_at_utc >= ?
            GROUP BY advertiser""",
            (job_id, week_ago, (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")),
        )
        last_week = {row[0]: row[1] for row in cur.fetchall()}
    increased = [{"advertiser": a, "delta": this_week.get(a, 0) - last_week.get(a, 0)} for a in this_week if this_week.get(a, 0) > last_week.get(a, 0)]
    increased.sort(key=lambda x: -x["delta"])
    diffs = {"new_today": new_today, "increased_this_week": increased[:8]}
    return {"competitors": competitors, "synced_at_utc": synced, "diffs": diffs}


def get_competitor_ads(job_id: int, advertiser: str, days: int, device: str, offer_tag: str = None):
    """Return ads list and synced_at for competitor drilldown."""
    cutoff = _parse_cutoff(days)
    with cursor() as cur:
        if device == "all":
            if offer_tag:
                cur.execute(
                    """SELECT id, advertiser, ad_id, device, block, headline, description, displayed_link,
                    destination_link, position, created_at_utc, extensions_json, offers_json, offer_tag
                    FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND (offer_tag = ? OR offer_tag IS NULL)
                    ORDER BY created_at_utc DESC LIMIT 500""",
                    (job_id, advertiser, cutoff, offer_tag),
                )
            else:
                cur.execute(
                    """SELECT id, advertiser, ad_id, device, block, headline, description, displayed_link,
                    destination_link, position, created_at_utc, extensions_json, offers_json, offer_tag
                    FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ?
                    ORDER BY created_at_utc DESC LIMIT 500""",
                    (job_id, advertiser, cutoff),
                )
        else:
            if offer_tag:
                cur.execute(
                    """SELECT id, advertiser, ad_id, device, block, headline, description, displayed_link,
                    destination_link, position, created_at_utc, extensions_json, offers_json, offer_tag
                    FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND device = ? AND (offer_tag = ? OR offer_tag IS NULL)
                    ORDER BY created_at_utc DESC LIMIT 500""",
                    (job_id, advertiser, cutoff, device, offer_tag),
                )
            else:
                cur.execute(
                    """SELECT id, advertiser, ad_id, device, block, headline, description, displayed_link,
                    destination_link, position, created_at_utc, extensions_json, offers_json, offer_tag
                    FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND device = ?
                    ORDER BY created_at_utc DESC LIMIT 500""",
                    (job_id, advertiser, cutoff, device),
                )
        rows = cur.fetchall()
    ads_list = []
    for r in rows:
        d = dict(r)
        d["offers"] = json.loads(r["offers_json"]) if r["offers_json"] else []
        d["extensions"] = json.loads(r["extensions_json"]) if r["extensions_json"] else None
        ads_list.append(d)
    with cursor() as cur:
        cur.execute(
            """SELECT MAX(captured_at_utc) FROM serp_snapshots s
            JOIN targets t ON s.target_id = t.id WHERE t.job_id = ?""",
            (job_id,),
        )
        row = cur.fetchone()
        synced = row[0] if row and row[0] else None
    return {"ads": ads_list, "synced_at_utc": synced}


def get_competitor_aggregates(job_id: int, advertiser: str, days: int, device: str):
    """Total, top_share, bottom_share, dayweek (today, this_week), monthly_spend, spend_scenario, series points, offer_tags."""
    cutoff = _parse_cutoff(days)
    with cursor() as cur:
        if device == "all":
            cur.execute(
                """SELECT COUNT(*) AS total,
                SUM(CASE WHEN block = 'top' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS top_share,
                SUM(CASE WHEN block = 'bottom' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS bottom_share
                FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ?""",
                (job_id, advertiser, cutoff),
            )
        else:
            cur.execute(
                """SELECT COUNT(*) AS total,
                SUM(CASE WHEN block = 'top' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS top_share,
                SUM(CASE WHEN block = 'bottom' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS bottom_share
                FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND device = ?""",
                (job_id, advertiser, cutoff, device),
            )
        row = cur.fetchone()
    total = row["total"] or 0
    top_share = row["top_share"] or 0.0
    bottom_share = row["bottom_share"] or 0.0
    today_str = datetime.now(timezone.utc).date().isoformat()
    week_start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    with cursor() as cur:
        cur.execute(
            """SELECT COUNT(*) FROM ads WHERE job_id = ? AND advertiser = ? AND date(created_at_utc) = ?""",
            (job_id, advertiser, today_str),
        )
        dayweek_today = cur.fetchone()[0] or 0
        cur.execute(
            """SELECT COUNT(*) FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ?""",
            (job_id, advertiser, week_start),
        )
        dayweek_week = cur.fetchone()[0] or 0
    dayweek = {"today": dayweek_today, "this_week": dayweek_week}
    cpc = 5.0
    clicks_per = 0.5
    monthly_spend = int(total * cpc * clicks_per * 30 / max(days, 1))
    spend_scenario = {"cpc_assumption": cpc, "clicks_per_appearance": clicks_per}
    # Series by date
    with cursor() as cur:
        if device == "all":
            cur.execute(
                """SELECT date(created_at_utc) AS d,
                COUNT(*) AS appearances,
                SUM(CASE WHEN block = 'top' THEN 1 ELSE 0 END) AS top,
                SUM(CASE WHEN block = 'bottom' THEN 1 ELSE 0 END) AS bottom
                FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ?
                GROUP BY date(created_at_utc) ORDER BY d""",
                (job_id, advertiser, cutoff),
            )
        else:
            cur.execute(
                """SELECT date(created_at_utc) AS d,
                COUNT(*) AS appearances,
                SUM(CASE WHEN block = 'top' THEN 1 ELSE 0 END) AS top,
                SUM(CASE WHEN block = 'bottom' THEN 1 ELSE 0 END) AS bottom
                FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND device = ?
                GROUP BY date(created_at_utc) ORDER BY d""",
                (job_id, advertiser, cutoff, device),
            )
        rows = cur.fetchall()
    series_points = [{"date": row["d"], "appearances": row["appearances"], "top": row["top"] or 0, "bottom": row["bottom"] or 0} for row in rows]
    if not series_points:
        series_points = [{"date": cutoff, "appearances": 0, "top": 0, "bottom": 0}]
    # Offer tags from same ads
    with cursor() as cur:
        cur.execute(
            """SELECT DISTINCT offer_tag FROM ads WHERE job_id = ? AND advertiser = ? AND created_at_utc >= ? AND offer_tag IS NOT NULL AND offer_tag != ''""",
            (job_id, advertiser, cutoff),
        )
        offer_tags = [r[0] for r in cur.fetchall()]
    synced_at = None
    with cursor() as cur:
        cur.execute(
            """SELECT MAX(captured_at_utc) FROM serp_snapshots s JOIN targets t ON s.target_id = t.id WHERE t.job_id = ?""",
            (job_id,),
        )
        r = cur.fetchone()
        if r and r[0]:
            synced_at = r[0]
    return {
        "total": total,
        "top_share": top_share,
        "bottom_share": bottom_share,
        "dayweek": dayweek,
        "monthly_spend": monthly_spend,
        "spend_scenario": spend_scenario,
        "series": {"points": series_points},
        "offer_tags": offer_tags or [],
        "synced_at_utc": synced_at,
    }


def get_ad_by_id(ad_id: int):
    """Get single ad row by id for crawl."""
    with cursor() as cur:
        cur.execute("SELECT id, destination_link FROM ads WHERE id = ?", (ad_id,))
        r = cur.fetchone()
        return dict(r) if r else None


def get_ad_by_id_or_external(ad_id_or_pk):
    """Resolve ad by primary key (int) or external ad_id (str). Returns row with id, destination_link."""
    try:
        pk = int(ad_id_or_pk)
        with cursor() as cur:
            cur.execute("SELECT id, destination_link FROM ads WHERE id = ?", (pk,))
            r = cur.fetchone()
            return dict(r) if r else None
    except (ValueError, TypeError):
        pass
    with cursor() as cur:
        cur.execute(
            "SELECT id, destination_link FROM ads WHERE ad_id = ? AND destination_link IS NOT NULL AND destination_link != '' ORDER BY created_at_utc DESC LIMIT 1",
            (str(ad_id_or_pk),),
        )
        r = cur.fetchone()
        if r:
            return dict(r)
        cur.execute("SELECT id, destination_link FROM ads WHERE ad_id = ? ORDER BY created_at_utc DESC LIMIT 1", (str(ad_id_or_pk),))
        r = cur.fetchone()
        return dict(r) if r else None


def get_latest_crawl(ad_id: int):
    """Get latest crawl for an ad (by ads.id)."""
    with cursor() as cur:
        cur.execute(
            """SELECT final_url, http_status, title, h1, h2s_json, has_form, pricing_mentions, financing_mentions,
            offers_json, pagespeed_performance, pagespeed_accessibility, pagespeed_best_practices, pagespeed_seo, synced_at_utc
            FROM crawls WHERE ad_id = ? ORDER BY synced_at_utc DESC LIMIT 1""",
            (ad_id,),
        )
        r = cur.fetchone()
        return dict(r) if r else None
