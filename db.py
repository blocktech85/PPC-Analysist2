"""Database layer: SQLite schema and connection helpers."""
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH

logger = logging.getLogger(__name__)

# Ensure directory exists
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)


def get_connection():
    """Return a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def cursor():
    conn = get_connection()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they do not exist."""
    with cursor() as cur:
        # Phase 1: Core
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                keyword TEXT NOT NULL,
                location_input TEXT NOT NULL,
                serp_location TEXT,
                gl TEXT DEFAULT 'us',
                hl TEXT DEFAULT 'en',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                budget_tracking_enabled INTEGER NOT NULL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS serp_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
                device TEXT NOT NULL,
                captured_at_utc TEXT NOT NULL,
                raw_json TEXT,
                grid_cell_id INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER REFERENCES serp_snapshots(id) ON DELETE SET NULL,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                advertiser TEXT NOT NULL,
                ad_id TEXT,
                device TEXT,
                block TEXT,
                headline TEXT,
                description TEXT,
                displayed_link TEXT,
                destination_link TEXT,
                position INTEGER,
                created_at_utc TEXT NOT NULL,
                extensions_json TEXT,
                offers_json TEXT,
                offer_tag TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ads_job_advertiser ON ads(job_id, advertiser)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ads_created_at ON ads(created_at_utc)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crawls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER REFERENCES ads(id) ON DELETE SET NULL,
                destination_url TEXT,
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
                synced_at_utc TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS atc_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                advertiser TEXT NOT NULL,
                region TEXT NOT NULL,
                raw_json TEXT,
                synced_at_utc TEXT NOT NULL
            )
        """)
        # Time-series for LPE and presence (Phase 4, 8)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                value REAL,
                value_text TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_entity ON metrics(entity_type, entity_id, metric_name, timestamp_utc)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS presence_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
                advertiser TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                appeared INTEGER NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_presence_target_time ON presence_snapshots(target_id, timestamp_utc)")

        # Phase 2: Brand / trademark
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brand_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
                term TEXT NOT NULL,
                pattern_type TEXT NOT NULL DEFAULT 'literal',
                regex_pattern TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trademark_violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                ad_id INTEGER REFERENCES ads(id),
                advertiser TEXT NOT NULL,
                source TEXT NOT NULL,
                matched_asset TEXT NOT NULL,
                matched_text_snippet TEXT,
                captured_at TEXT NOT NULL,
                reviewed_at TEXT,
                status TEXT DEFAULT 'new'
            )
        """)

        # Phase 3: NLP triggers
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ad_trigger_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
                trigger_name TEXT NOT NULL,
                score REAL,
                model_used TEXT,
                synced_at TEXT NOT NULL,
                UNIQUE(ad_id, trigger_name)
            )
        """)

        # Phase 4: Auction insights (computed on demand or cached)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS auction_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                window_days INTEGER NOT NULL,
                device TEXT NOT NULL,
                advertiser_a TEXT NOT NULL,
                advertiser_b TEXT NOT NULL,
                overlap_rate REAL NOT NULL,
                outranking_share_ab REAL NOT NULL,
                outranking_share_ba REAL NOT NULL,
                snapshot_count INTEGER NOT NULL,
                computed_at TEXT NOT NULL
            )
        """)

        # Phase 6: Geo grid
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geo_grids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                definition_json TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grid_cells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                geo_grid_id INTEGER NOT NULL REFERENCES geo_grids(id) ON DELETE CASCADE,
                label TEXT NOT NULL,
                zip_code TEXT,
                lat REAL,
                lon REAL
            )
        """)

        # Phase 7: Creative threat
        cur.execute("""
            CREATE TABLE IF NOT EXISTS competitor_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                advertiser_domain TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT 'US',
                last_atc_snapshot_id INTEGER REFERENCES atc_snapshots(id),
                last_poll_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS creative_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watchlist_id INTEGER NOT NULL REFERENCES competitor_watchlist(id) ON DELETE CASCADE,
                type TEXT NOT NULL,
                previous_snapshot_id INTEGER,
                new_snapshot_id INTEGER,
                diff_summary_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        # Phase 8: Extracted offers
        cur.execute("""
            CREATE TABLE IF NOT EXISTS extracted_offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
                source TEXT NOT NULL,
                financing_rate TEXT,
                guarantee_text TEXT,
                free_trial_days INTEGER,
                discount_type TEXT,
                other_promotion_json TEXT,
                raw_snippet TEXT,
                model_used TEXT,
                synced_at TEXT NOT NULL
            )
        """)

    logger.info("Database initialized at %s", DB_PATH)


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
