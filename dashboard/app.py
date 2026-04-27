"""
フォーム営業 Webダッシュボード
日次・週次・月次レポート、全リード管理、定量分析を提供する。

起動: python3 dashboard/app.py
アクセス: https://133.18.122.55:8443  (ユーザー/パスワード: config.yamlのdashboard.users)
"""

import sys, os, math, yaml, secrets
from datetime import date, datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import database as db

app = Flask(__name__)
# 起動ごとにランダムなsecret_keyを生成（セッション偽造対策）
app.secret_key = secrets.token_hex(32)


def load_config():
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# ── 認証 ──────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        config = load_config()
        users = config.get("dashboard", {}).get("users", {})
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if username in users and users[username] == password:
            session["logged_in"] = True
            session["username"] = username
            return redirect(url_for("overview"))
        error = "IDまたはパスワードが違います"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))


# ── 概要（ホーム） ────────────────────────────────────
@app.route("/")
@require_login
def overview():
    config = load_config()
    daily_limit = config["rate_limit"]["max_per_day"]
    stats = db.get_stats()
    today_stats = db.get_today_processing_stats()
    yesterday_sent = db.get_yesterday_send_count()
    daily_counts = db.get_daily_counts(7)

    sent = today_stats["sent"]
    achievement = round(sent / daily_limit * 100) if daily_limit > 0 else 0
    diff = sent - yesterday_sent
    days_left = round(stats["pending"] / daily_limit, 1) if daily_limit > 0 else 0

    return render_template("overview.html",
        stats=stats, today_stats=today_stats,
        yesterday_sent=yesterday_sent, diff=diff,
        achievement=achievement, daily_limit=daily_limit,
        days_left=days_left, daily_counts=daily_counts,
        today=date.today().strftime("%Y年%m月%d日"),
    )


# ── レポート（日次・週次・月次） ──────────────────────
@app.route("/reports")
@require_login
def reports():
    period = request.args.get("period", "daily")
    config = load_config()
    daily_limit = config["rate_limit"]["max_per_day"]

    daily_counts  = db.get_daily_counts(30)
    monthly_stats = db.get_monthly_stats(12)
    weekly_data   = db.get_weekly_analytics(weeks_back=1)
    stats         = db.get_stats()

    # 週次: 直近4週の集計
    weekly_counts = []
    for w in range(4):
        wd = db.get_weekly_analytics(weeks_back=w + 1)
        t  = wd.get("totals", {})
        from datetime import datetime, timedelta
        week_end   = (datetime.now() - timedelta(weeks=w)).strftime("%m/%d")
        week_start = (datetime.now() - timedelta(weeks=w + 1)).strftime("%m/%d")
        weekly_counts.append({
            "label": f"{week_start}〜{week_end}",
            "sent": t.get("total_sent") or 0,
            "responses": t.get("total_responses") or 0,
        })
    weekly_counts.reverse()

    return render_template("reports.html",
        period=period,
        daily_counts=daily_counts,
        monthly_stats=monthly_stats,
        weekly_counts=weekly_counts,
        weekly_data=weekly_data,
        stats=stats,
        daily_limit=daily_limit,
    )


# ── リード一覧 ────────────────────────────────────────
@app.route("/leads")
@require_login
def leads():
    page           = int(request.args.get("page", 1))
    per_page       = 50
    status_filter  = request.args.get("status", "")
    industry_filter= request.args.get("industry", "")
    search         = request.args.get("search", "")

    lead_list, total = db.get_leads_paginated(
        page=page, per_page=per_page,
        status=status_filter or None,
        industry=industry_filter or None,
        search=search or None,
    )
    total_pages = math.ceil(total / per_page) if total > 0 else 1

    return render_template("leads.html",
        leads=lead_list, total=total,
        page=page, total_pages=total_pages, per_page=per_page,
        status_filter=status_filter,
        industry_filter=industry_filter,
        search=search,
    )


# ── 定量分析 ──────────────────────────────────────────
@app.route("/analytics")
@require_login
def analytics():
    stats             = db.get_stats()
    industry_breakdown= db.get_industry_breakdown()
    skip_reasons      = db.get_skip_reason_breakdown()
    weekly            = db.get_weekly_analytics(weeks_back=4)

    # 送信ファネル
    total   = stats["total"]
    pending = stats["pending"]
    sent    = stats["sent"]
    skipped = stats["skipped"]
    failed  = stats["failed"]
    responses = sum(
        (r.get("total_responses") or 0)
        for r in [db.get_weekly_analytics(weeks_back=99).get("totals", {})]
    )

    return render_template("analytics.html",
        stats=stats,
        industry_breakdown=industry_breakdown,
        skip_reasons=skip_reasons,
        weekly=weekly,
        funnel={
            "total": total, "pending": pending,
            "sent": sent, "skipped": skipped, "failed": failed,
        },
    )


# ── JSON API (Chart.js用) ─────────────────────────────
@app.route("/api/daily")
@require_login
def api_daily():
    data = db.get_daily_counts(30)
    return jsonify({"labels": [d["date"] for d in data], "values": [d["count"] for d in data]})


@app.route("/api/monthly")
@require_login
def api_monthly():
    data = db.get_monthly_stats(12)
    return jsonify({"labels": [d["month"] for d in data], "values": [d["sent"] for d in data]})


if __name__ == "__main__":
    db.init_db()
    config = load_config()
    port = config.get("dashboard", {}).get("port", 8443)

    # SSL証明書のパス
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ssl_cert = os.path.join(base_dir, "ssl", "cert.pem")
    ssl_key  = os.path.join(base_dir, "ssl", "key.pem")

    if os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        print(f"ダッシュボード起動: https://0.0.0.0:{port}  (HTTPS)")
        app.run(host="0.0.0.0", port=port, debug=False,
                ssl_context=(ssl_cert, ssl_key))
    else:
        print(f"ダッシュボード起動: http://0.0.0.0:{port}  (HTTP fallback)")
        app.run(host="0.0.0.0", port=port, debug=False)
