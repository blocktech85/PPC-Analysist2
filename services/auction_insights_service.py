"""Proxy auction insights: overlap rate and outranking share from longitudinal SERP data."""
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from db import cursor

logger = logging.getLogger(__name__)


def compute_auction_insights(job_id: int, window_days: int, device: str) -> list:
    """Compute pairwise overlap_rate and outranking_share for all advertisers in job. Returns list of dicts."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(window_days))).strftime("%Y-%m-%d")
    with cursor() as cur:
        cur.execute(
            """SELECT s.id AS snapshot_id, a.advertiser, a.block, a.position
            FROM serp_snapshots s
            JOIN ads a ON a.snapshot_id = s.id
            JOIN targets t ON s.target_id = t.id
            WHERE t.job_id = ? AND s.captured_at_utc >= ?
            AND (? = 'all' OR a.device = ?)""",
            (job_id, cutoff, device, device),
        )
        rows = cur.fetchall()
    # snapshot_id -> list of (advertiser, block, position)
    snap_advertisers = defaultdict(list)
    for r in rows:
        snap_advertisers[r["snapshot_id"]].append((r["advertiser"], r["block"] or "bottom", r["position"] or 999))
    snapshot_ids = list(snap_advertisers.keys())
    if not snapshot_ids:
        return []
    advertisers = set()
    for v in snap_advertisers.values():
        for adv, _, _ in v:
            advertisers.add(adv)
    advertisers = sorted(advertisers)
    n_snaps = len(snapshot_ids)
    # overlap: both appear in same snapshot
    overlap = defaultdict(lambda: defaultdict(int))
    outrank_ab = defaultdict(lambda: defaultdict(int))
    outrank_ba = defaultdict(lambda: defaultdict(int))
    for snap_id, adv_list in snap_advertisers.items():
        adv_set = set(a[0] for a in adv_list)
        positions = {a[0]: (a[1], a[2]) for a in adv_list}
        for a in advertisers:
            for b in advertisers:
                if a >= b:
                    continue
                if a in adv_set and b in adv_set:
                    overlap[a][b] += 1
                    pos_a = positions[a]
                    pos_b = positions[b]
                    if pos_a[1] < pos_b[1] or (pos_a[1] == pos_b[1] and pos_a[0] == "top" and pos_b[0] != "top"):
                        outrank_ab[a][b] += 1
                    else:
                        outrank_ba[a][b] += 1
    result = []
    for a in advertisers:
        for b in advertisers:
            if a >= b:
                continue
            o = overlap[a][b]
            oab = outrank_ab[a][b]
            oba = outrank_ba[a][b]
            result.append({
                "advertiser_a": a,
                "advertiser_b": b,
                "overlap_rate": round(o / n_snaps, 4) if n_snaps else 0,
                "outranking_share_ab": round(oab / n_snaps, 4) if n_snaps else 0,
                "outranking_share_ba": round(oba / n_snaps, 4) if n_snaps else 0,
                "snapshot_count": n_snaps,
            })
    return result
