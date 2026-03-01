# PPC Analyst - Flask application
"""Flask app: routes, APIs, and scheduler."""
import csv
import io
import json
import logging
from datetime import datetime, timezone

from flask import Flask, flash, redirect, render_template, request, url_for
from apscheduler.schedulers.background import BackgroundScheduler

import logger as app_logger
from config import DEBUG, SECRET_KEY
from db import init_db, cursor, utc_now
from data import (
    list_jobs,
    get_job,
    create_job,
    delete_job,
    list_targets,
    get_targets_with_last_run,
    get_target,
    add_targets,
    get_competitors,
    get_competitor_ads,
    get_competitor_aggregates,
    get_ad_by_id_or_external,
)
from services.serpapi_service import run_target as serp_run_target
from services.crawl_service import crawl_and_save
from services.atc_service import atc_list, atc_details
from services.trends_service import fetch_trends
from services.brand_service import list_brand_assets, add_brand_asset, scan_ads_for_brand, list_violations, generate_complaint_doc, update_violation_status
from services.auction_insights_service import compute_auction_insights
from services.lpe_service import run_lpe_batch_for_job
from services.creative_threat_service import list_watchlist, add_to_watchlist, poll_watchlist_and_alert
from services.budget_exhaustion_service import run_budget_tracking_cycle, run_budget_tracking_for_target, get_presence_24h
from utils import redact_api_keys

app_logger  # ensure logging is configured

app = Flask(__name__, template_folder="Templates")
app.config["SECRET_KEY"] = SECRET_KEY
app.config["DEBUG"] = DEBUG

logger = logging.getLogger(__name__)

# Ensure DB exists when app is loaded (e.g. under gunicorn/uwsgi; also when run via python app.py)
init_db()

# Scheduler (for Phase 7, 9 - creative threat and budget exhaustion)
scheduler = BackgroundScheduler()


@app.before_request
def _ensure_db():
    pass  # init_db is called at startup


# ----- Page routes -----


@app.route("/")
def index():
    jobs = list_jobs()
    return render_template("index.html", jobs=jobs, title="Projects")


@app.route("/create-job", methods=["POST"])
def create_job_route():
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("index"))
    create_job(name)
    return redirect(url_for("index"))


@app.route("/research/<int:job_id>/delete", methods=["POST"])
def delete_job_route(job_id: int):
    job = get_job(job_id)
    if not job:
        flash("Project not found.", "warning")
        return redirect(url_for("index"))
    if delete_job(job_id):
        flash(f"Project “{job['name']}” has been deleted.", "success")
    else:
        flash("Could not delete project.", "danger")
    return redirect(url_for("index"))


@app.route("/research/<int:job_id>")
def research(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    targets = get_targets_with_last_run(job_id)
    return render_template("research.html", job=job, targets=targets, title=job["name"])


@app.route("/research/<int:job_id>/add-targets", methods=["POST"])
def add_targets_route(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    keywords_raw = request.form.get("keywords") or ""
    keywords = [k.strip() for k in keywords_raw.replace("\n", ",").split(",") if k.strip()]
    location = (request.form.get("location") or "").strip()
    gl = (request.form.get("gl") or "us").strip()
    hl = (request.form.get("hl") or "en").strip()
    if keywords and location:
        add_targets(job_id, keywords, location, gl, hl)
    return redirect(url_for("research", job_id=job_id))


@app.route("/research/<int:job_id>/run-target/<int:target_id>", methods=["POST"])
def run_target(job_id: int, target_id: int):
    target = get_target(target_id)
    if not target or target["job_id"] != job_id:
        return "Target not found", 404
    try:
        count = serp_run_target(
            target_id,
            job_id,
            target["keyword"],
            target["serp_location"] or target["location_input"],
            target["gl"] or "us",
            target["hl"] or "en",
        )
        if count:
            flash(f"Run complete: {count} snapshot(s) captured.")
        else:
            flash("Run produced no snapshots (check SERPAPI_API_KEY and logs).", "warning")
    except Exception as e:
        logger.exception("Run target failed: %s", e)
        flash(f"Run failed: {redact_api_keys(str(e))[:200]}", "danger")
    return redirect(url_for("research", job_id=job_id))


@app.route("/research/<int:job_id>/competitor/<path:advertiser>")
def competitor_page(job_id: int, advertiser: str):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    days = int(request.args.get("days", 30))
    device = request.args.get("device", "all")
    ag = get_competitor_aggregates(job_id, advertiser, days, device)
    return render_template(
        "competitor.html",
        job=job,
        advertiser=advertiser,
        days=days,
        device=device,
        total=ag["total"],
        top_share=ag["top_share"],
        bottom_share=ag["bottom_share"],
        dayweek=ag["dayweek"],
        monthly_spend=ag["monthly_spend"],
        spend_scenario=ag["spend_scenario"],
        series=ag["series"],
        offer_tags=ag["offer_tags"],
        synced_at_utc=ag["synced_at_utc"],
        title=f"{advertiser} - {job['name']}",
    )


@app.route("/research/<int:job_id>/geo")
def geo_page(job_id: int):
    def _geo_context(jid):
        job = get_job(jid)
        if not job:
            return None
        advertiser = request.args.get("advertiser", "")
        days = int(request.args.get("days", 30))
        device = request.args.get("device", "all")
        comp_data = get_competitors(jid, days, device)
        competitors = [c["advertiser"] for c in (comp_data.get("competitors") or [])[:100]]
        if not advertiser:
            return {
                "job": job, "advertiser": "", "days": days, "device": device,
                "total": 0, "top_share": 0, "bottom_share": 0,
                "dayweek": {"today": 0, "this_week": 0}, "monthly_spend": 0,
                "spend_scenario": {"cpc_assumption": 5, "clicks_per_appearance": 0.5},
                "series": {"points": []}, "offer_tags": [], "synced_at_utc": None,
                "title": f"Geo - {job['name']}", "competitors": competitors,
            }
        ag = get_competitor_aggregates(jid, advertiser, days, device)
        return {**ag, "job": job, "advertiser": advertiser, "days": days, "device": device, "title": f"Geo - {job['name']}", "competitors": competitors}
    ctx = _geo_context(job_id)
    if ctx is None:
        return "Project not found", 404
    return render_template("geo.html", **ctx)


@app.route("/research/<int:job_id>/export.csv")
def export_job_csv(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    with cursor() as cur:
        cur.execute(
            """SELECT a.id, a.advertiser, a.ad_id, a.device, a.block, a.headline, a.description, a.displayed_link,
            a.destination_link, a.position, a.created_at_utc, t.keyword, t.location_input
            FROM ads a LEFT JOIN serp_snapshots s ON a.snapshot_id = s.id LEFT JOIN targets t ON s.target_id = t.id
            WHERE a.job_id = ? ORDER BY a.created_at_utc DESC""",
            (job_id,),
        )
        rows = cur.fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id", "advertiser", "ad_id", "device", "block", "headline", "description", "displayed_link", "destination_link", "position", "created_at_utc", "keyword", "location"])
    for r in rows:
        w.writerow([r["id"], r["advertiser"], r["ad_id"], r["device"], r["block"], r["headline"], r["description"], r["displayed_link"], r["destination_link"], r["position"], r["created_at_utc"], r["keyword"], r["location_input"]])
    out.seek(0)
    return out.getvalue(), 200, {"Content-Type": "text/csv", "Content-Disposition": f"attachment; filename=project_{job_id}_export.csv"}


# ----- JSON APIs -----


@app.route("/api/status")
def api_status():
    """Health/status check; returns 200 OK."""
    return app.response_class(
        response=json.dumps({"status": "ok"}),
        mimetype="application/json",
    )


@app.route("/api/research/<int:job_id>/competitors")
def api_competitors(job_id: int):
    days = int(request.args.get("days", 30))
    device = request.args.get("device", "all")
    out = get_competitors(job_id, days, device)
    return app.response_class(response=json.dumps(out), mimetype="application/json")


@app.route("/api/research/<int:job_id>/competitor/<path:advertiser>/ads")
def api_competitor_ads(job_id: int, advertiser: str):
    advertiser = (advertiser or "").strip()
    if not advertiser:
        return json.dumps({"error": "Advertiser is required", "ads": []}), 400, {"Content-Type": "application/json"}
    days = int(request.args.get("days", 30))
    device = request.args.get("device", "all")
    offer = request.args.get("offer", "").strip() or None
    out = get_competitor_ads(job_id, advertiser, days, device, offer)
    # Serialize for JSON (datetime, None)
    ads_ser = []
    for a in out["ads"]:
        ad = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in a.items()}
        ad["created_at_utc"] = a.get("created_at_utc") or ""
        ads_ser.append(ad)
    return app.response_class(response=json.dumps({"ads": ads_ser, "synced_at_utc": out["synced_at_utc"]}), mimetype="application/json")


@app.route("/api/ad/<ad_id>/crawl", methods=["POST"])
def api_ad_crawl(ad_id):
    ad = get_ad_by_id_or_external(ad_id)
    if not ad:
        return json.dumps({"error": "Ad not found"}), 404, {"Content-Type": "application/json"}
    url = ad.get("destination_link") or ""
    if not url or not url.startswith("http"):
        return json.dumps({"error": "No destination URL", "synced_at_utc": None}), 400, {"Content-Type": "application/json"}
    try:
        crawl = crawl_and_save(ad["id"], url)
        return app.response_class(response=json.dumps(crawl), mimetype="application/json")
    except Exception as e:
        logger.exception("Crawl failed: %s", e)
        return json.dumps({"error": redact_api_keys(str(e)), "synced_at_utc": None}), 500, {"Content-Type": "application/json"}


@app.route("/api/atc/list")
def api_atc_list():
    advertiser = request.args.get("advertiser", "")
    region = request.args.get("region", "US")
    page = int(request.args.get("page", 1))
    out = atc_list(advertiser, region, page)
    return app.response_class(response=json.dumps(out), mimetype="application/json")


@app.route("/api/atc/details")
def api_atc_details():
    ad_id = request.args.get("ad_id", "")
    region = request.args.get("region", "US")
    out = atc_details(ad_id, region)
    return app.response_class(response=json.dumps(out), mimetype="application/json")


@app.route("/api/trends")
def api_trends():
    keywords = request.args.get("keywords", "")
    geo = request.args.get("geo", "US")
    timeframe = request.args.get("timeframe", "today 12-m")
    out = fetch_trends(keywords, geo, timeframe)
    return app.response_class(response=json.dumps(out), mimetype="application/json")


# ----- Phase 2–9: Advanced APIs -----


@app.route("/api/research/<int:job_id>/brand-assets", methods=["GET", "POST"])
def api_brand_assets(job_id: int):
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        term = (request.form.get("term") or data.get("term") or "").strip()
        pattern_type = request.form.get("pattern_type") or data.get("pattern_type") or "literal"
        regex = request.form.get("regex_pattern") or data.get("regex_pattern")
        if term or regex:
            add_brand_asset(job_id, term, pattern_type, regex)
        if request.is_json or request.content_type and "application/json" in request.content_type:
            return app.response_class(response=json.dumps({"ok": True}), mimetype="application/json")
        return redirect(url_for("research", job_id=job_id))
    assets = list_brand_assets(job_id) + list_brand_assets(None)
    return app.response_class(response=json.dumps({"assets": assets}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/scan-brand", methods=["POST"])
def api_scan_brand(job_id: int):
    count = scan_ads_for_brand(job_id)
    if request.is_json or (request.content_type and "application/json" in (request.content_type or "")):
        return app.response_class(response=json.dumps({"scanned": count}), mimetype="application/json")
    flash(f"Scan complete: {count} new violation(s) recorded." if count else "Scan complete: no new violations.")
    return redirect(url_for("brand_monitor_page", job_id=job_id))


@app.route("/api/research/<int:job_id>/violations")
def api_violations(job_id: int):
    status = request.args.get("status")
    out = list_violations(job_id, status)
    return app.response_class(response=json.dumps({"violations": [dict(v) for v in out]}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/violations/<int:violation_id>/status", methods=["POST", "PATCH"])
def api_violation_status(job_id: int, violation_id: int):
    data = request.get_json(silent=True) or {}
    new_status = (data.get("status") or request.form.get("status") or "").strip()
    if not new_status:
        return json.dumps({"ok": False, "error": "status required"}), 400, {"Content-Type": "application/json"}
    ok = update_violation_status(violation_id, job_id, new_status)
    return app.response_class(response=json.dumps({"ok": ok}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/complaint-doc", methods=["POST"])
def api_complaint_doc(job_id: int):
    data = request.get_json(silent=True) if request.is_json else None
    if request.is_json:
        ids = (data or {}).get("violation_ids", [])
    else:
        raw = request.form.get("violation_ids")
        ids = [int(x.strip()) for x in (raw or "").split(",") if str(x).strip().isdigit()]
    doc = generate_complaint_doc(ids)
    return app.response_class(response=doc, mimetype="text/plain", headers={"Content-Disposition": "attachment; filename=trademark_complaint_evidence.txt"})


@app.route("/api/research/<int:job_id>/auction-insights")
def api_auction_insights(job_id: int):
    days = int(request.args.get("days", 30))
    device = request.args.get("device", "all")
    out = compute_auction_insights(job_id, days, device)
    return app.response_class(response=json.dumps({"matrix": out}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/lpe-batch", methods=["POST"])
def api_lpe_batch(job_id: int):
    days = int(request.args.get("days", 7))
    count = run_lpe_batch_for_job(job_id, days)
    return app.response_class(response=json.dumps({"urls_processed": count}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/watchlist", methods=["GET", "POST"])
def api_watchlist(job_id: int):
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        domain = (request.form.get("advertiser_domain") or data.get("advertiser_domain") or "").strip()
        region = request.form.get("region") or data.get("region") or "US"
        if domain:
            add_to_watchlist(job_id, domain, region)
        return app.response_class(response=json.dumps({"ok": True}), mimetype="application/json")
    out = list_watchlist(job_id)
    return app.response_class(response=json.dumps({"watchlist": [dict(w) for w in out]}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/creative-alerts")
def api_creative_alerts(job_id: int):
    with cursor() as cur:
        cur.execute(
            """SELECT ca.id, ca.watchlist_id, ca.type, ca.previous_snapshot_id, ca.new_snapshot_id, ca.diff_summary_json, ca.created_at, cw.advertiser_domain
            FROM creative_alerts ca JOIN competitor_watchlist cw ON ca.watchlist_id = cw.id WHERE cw.job_id = ? ORDER BY ca.created_at DESC LIMIT 100""",
            (job_id,),
        )
        rows = cur.fetchall()
    return app.response_class(response=json.dumps({"alerts": [dict(r) for r in rows]}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/target/<int:target_id>/budget-tracking", methods=["POST"])
def api_toggle_budget_tracking(job_id: int, target_id: int):
    target = get_target(target_id)
    if not target or target["job_id"] != job_id:
        return json.dumps({"error": "Not found"}), 404, {"Content-Type": "application/json"}
    enabled = (request.get_json(silent=True) or {}).get("enabled", True) if request.is_json else request.form.get("enabled", "1") == "1"
    with cursor() as cur:
        cur.execute("UPDATE targets SET budget_tracking_enabled = ? WHERE id = ?", (1 if enabled else 0, target_id))
    return app.response_class(response=json.dumps({"ok": True, "budget_tracking_enabled": enabled}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/target/<int:target_id>/presence")
def api_presence(job_id: int, target_id: int):
    target = get_target(target_id)
    if not target or target["job_id"] != job_id:
        return json.dumps({"error": "Not found"}), 404, {"Content-Type": "application/json"}
    out = get_presence_24h(target_id)
    return app.response_class(response=json.dumps({"presence": out}), mimetype="application/json")


@app.route("/api/research/<int:job_id>/target/<int:target_id>/presence-refresh", methods=["POST"])
def api_presence_refresh(job_id: int, target_id: int):
    """Run one presence snapshot for this target (must have budget tracking enabled), then return updated presence."""
    target = get_target(target_id)
    if not target or target["job_id"] != job_id:
        return json.dumps({"error": "Not found"}), 404, {"Content-Type": "application/json"}
    ran = run_budget_tracking_for_target(target_id)
    out = get_presence_24h(target_id)
    return app.response_class(
        response=json.dumps({"ok": True, "ran": ran, "presence": out}),
        mimetype="application/json",
    )


@app.route("/research/<int:job_id>/brand")
def brand_monitor_page(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    status_filter = request.args.get("status", "")
    assets = list_brand_assets(job_id) + list_brand_assets(None)
    violations = list_violations(job_id, status_filter if status_filter else None)
    return render_template("brand_monitor.html", job=job, assets=assets, violations=violations, status_filter=status_filter, title=f"Brand monitor - {job['name']}")


@app.route("/research/<int:job_id>/auction-insights")
def auction_insights_page(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    return render_template("auction_insights.html", job=job, title=f"Auction insights - {job['name']}")


@app.route("/research/<int:job_id>/creative-alerts")
def creative_alerts_page(job_id: int):
    job = get_job(job_id)
    if not job:
        return "Project not found", 404
    return render_template("creative_alerts.html", job=job, title=f"Creative alerts - {job['name']}")


if __name__ == "__main__":
    try:
        scheduler.add_job(poll_watchlist_and_alert, "cron", hour=2, minute=0)
        scheduler.add_job(run_budget_tracking_cycle, "interval", hours=1)
    except Exception as e:
        logger.warning("Scheduler jobs not added: %s", e)
    scheduler.start()
    app.run(debug=DEBUG, use_reloader=False)