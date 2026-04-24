"""
送信記録・リード管理・送信量制御を行うデータベース層
SQLiteを使用（無料・ローカル完結）
"""

import sqlite3
import os
from datetime import datetime, date
from typing import Optional
from dataclasses import dataclass


DB_PATH = os.path.join(os.path.dirname(__file__), "data", "form_sales.db")


@dataclass
class Lead:
    id: Optional[int]
    company_name: str
    url: str
    industry: str
    contact_form_url: str
    status: str  # pending, researched, sent, skipped, failed
    skip_reason: str
    ai_summary: str
    personalized_message: str
    sent_at: Optional[str]
    created_at: str
    source_file: str = ""
    template_name: str = "default"


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """データベースの初期化（テーブル作成・マイグレーション）"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            industry TEXT DEFAULT '',
            contact_form_url TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            skip_reason TEXT DEFAULT '',
            ai_summary TEXT DEFAULT '',
            personalized_message TEXT DEFAULT '',
            sent_at TEXT,
            source_file TEXT DEFAULT '',
            template_name TEXT DEFAULT 'default',
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS send_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            sent_at TEXT DEFAULT (datetime('now', 'localtime')),
            success INTEGER DEFAULT 0,
            error_message TEXT DEFAULT '',
            screenshot_path TEXT DEFAULT '',
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS daily_counter (
            date TEXT PRIMARY KEY,
            count INTEGER DEFAULT 0
        );
    """)
    # 既存DBへのカラム追加（マイグレーション）
    for column, definition in [
        ("source_file", "TEXT DEFAULT ''"),
        ("template_name", "TEXT DEFAULT 'default'"),
        ("response_received", "INTEGER DEFAULT 0"),
        ("responded_at", "TEXT"),
        ("response_notes", "TEXT DEFAULT ''"),
    ]:
        try:
            conn.execute(f"ALTER TABLE leads ADD COLUMN {column} {definition}")
        except Exception:
            pass  # 既にカラムが存在する場合はスキップ
    conn.commit()
    conn.close()


def import_leads_from_csv(csv_path: str, template_name: str = "default") -> int:
    """CSVからリードを一括インポート（ソースファイルとテンプレートを記録）"""
    import csv
    source_file = os.path.basename(csv_path)
    conn = get_connection()
    imported = 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO leads (company_name, url, industry, source_file, template_name) VALUES (?, ?, ?, ?, ?)",
                    (row.get("company_name", ""), row.get("url", ""), row.get("industry", ""), source_file, template_name)
                )
                if cursor.rowcount > 0:
                    imported += 1
            except sqlite3.IntegrityError:
                pass
    conn.commit()
    conn.close()
    return imported


def get_pending_leads(limit: int = 50, template_name: str = None) -> list[dict]:
    """未送信のリードを取得（テンプレート名でフィルタ可能）"""
    conn = get_connection()
    if template_name:
        rows = conn.execute(
            "SELECT * FROM leads WHERE status = 'pending' AND template_name = ? ORDER BY id LIMIT ?",
            (template_name, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM leads WHERE status = 'pending' ORDER BY id LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sent_leads() -> list[dict]:
    """送信済みリードを取得"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, company_name, industry, source_file, template_name, sent_at FROM leads WHERE status = 'sent' ORDER BY sent_at"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reset_for_retry(statuses: list = None) -> int:
    """失敗・スキップリードをpendingに戻す"""
    if statuses is None:
        statuses = ["failed", "skipped"]
    conn = get_connection()
    placeholders = ",".join("?" * len(statuses))
    result = conn.execute(
        f"UPDATE leads SET status='pending', skip_reason='', sent_at=NULL WHERE status IN ({placeholders})",
        statuses
    )
    count = result.rowcount
    conn.commit()
    conn.close()
    return count


def get_retry_leads() -> list[dict]:
    """直近の失敗・スキップリードを取得（リトライリスト用）"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, company_name, url, industry, source_file, template_name, status, skip_reason
        FROM leads
        WHERE status IN ('failed', 'skipped')
        ORDER BY status, id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_responded(lead_id: int, notes: str = "") -> bool:
    """リードに返信ありとしてマーク"""
    from datetime import datetime
    conn = get_connection()
    conn.execute(
        "UPDATE leads SET response_received = 1, responded_at = ?, response_notes = ? WHERE id = ?",
        (datetime.now().isoformat(), notes, lead_id)
    )
    conn.commit()
    conn.close()
    return True


def get_weekly_analytics(weeks_back: int = 1) -> dict:
    """過去N週間の分析データを取得"""
    from datetime import datetime, timedelta
    conn = get_connection()
    since = (datetime.now() - timedelta(weeks=weeks_back)).isoformat()

    # テンプレート別パフォーマンス
    template_stats = conn.execute("""
        SELECT
            template_name,
            COUNT(*) as sent,
            SUM(response_received) as responses,
            ROUND(SUM(response_received) * 100.0 / COUNT(*), 1) as response_rate
        FROM leads
        WHERE status = 'sent' AND sent_at >= ?
        GROUP BY template_name
        ORDER BY response_rate DESC
    """, (since,)).fetchall()

    # 業種別パフォーマンス
    industry_stats = conn.execute("""
        SELECT
            industry,
            COUNT(*) as sent,
            SUM(response_received) as responses,
            ROUND(SUM(response_received) * 100.0 / COUNT(*), 1) as response_rate
        FROM leads
        WHERE status = 'sent' AND sent_at >= ?
        GROUP BY industry
        ORDER BY responses DESC, sent DESC
        LIMIT 10
    """, (since,)).fetchall()

    # 曜日別送信数
    weekday_stats = conn.execute("""
        SELECT
            CASE CAST(strftime('%w', sent_at) AS INTEGER)
                WHEN 0 THEN '日' WHEN 1 THEN '月' WHEN 2 THEN '火'
                WHEN 3 THEN '水' WHEN 4 THEN '木' WHEN 5 THEN '金' WHEN 6 THEN '土'
            END as weekday,
            COUNT(*) as sent,
            SUM(response_received) as responses
        FROM leads
        WHERE status = 'sent' AND sent_at >= ?
        GROUP BY strftime('%w', sent_at)
        ORDER BY strftime('%w', sent_at)
    """, (since,)).fetchall()

    # 全期間累計
    totals = conn.execute("""
        SELECT
            COUNT(*) as total_sent,
            SUM(response_received) as total_responses,
            ROUND(SUM(response_received) * 100.0 / NULLIF(COUNT(*), 0), 1) as overall_rate
        FROM leads WHERE status = 'sent'
    """).fetchone()

    conn.close()
    return {
        "template_stats": [dict(r) for r in template_stats],
        "industry_stats": [dict(r) for r in industry_stats],
        "weekday_stats": [dict(r) for r in weekday_stats],
        "totals": dict(totals) if totals else {},
    }


def get_all_leads_for_export() -> list[dict]:
    """全リードデータをエクスポート用に取得"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            id, company_name, industry, url, contact_form_url,
            status, source_file, template_name,
            response_received, responded_at, response_notes,
            sent_at, skip_reason, created_at
        FROM leads
        ORDER BY id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_list_stats() -> list[dict]:
    """ソースファイル×テンプレート別の統計を取得"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            source_file,
            template_name,
            COUNT(*) as total,
            SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) as sent,
            SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) as skipped,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending
        FROM leads
        GROUP BY source_file, template_name
        ORDER BY source_file
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_lead(lead_id: int, **kwargs):
    """リードの情報を更新"""
    conn = get_connection()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [lead_id]
    conn.execute(f"UPDATE leads SET {sets} WHERE id = ?", values)
    conn.commit()
    conn.close()


def log_send(lead_id: int, success: bool, error_message: str = "", screenshot_path: str = ""):
    """送信ログを記録"""
    conn = get_connection()
    conn.execute(
        "INSERT INTO send_log (lead_id, success, error_message, screenshot_path) VALUES (?, ?, ?, ?)",
        (lead_id, int(success), error_message, screenshot_path)
    )
    conn.commit()
    conn.close()


def get_today_send_count() -> int:
    """今日の送信数を取得"""
    conn = get_connection()
    today = date.today().isoformat()
    row = conn.execute(
        "SELECT count FROM daily_counter WHERE date = ?", (today,)
    ).fetchone()
    conn.close()
    return row["count"] if row else 0


def increment_daily_counter():
    """今日の送信カウンターをインクリメント"""
    conn = get_connection()
    today = date.today().isoformat()
    conn.execute(
        """INSERT INTO daily_counter (date, count) VALUES (?, 1)
           ON CONFLICT(date) DO UPDATE SET count = count + 1""",
        (today,)
    )
    conn.commit()
    conn.close()


def get_stats() -> dict:
    """統計情報を取得"""
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) as c FROM leads").fetchone()["c"]
    sent = conn.execute("SELECT COUNT(*) as c FROM leads WHERE status = 'sent'").fetchone()["c"]
    skipped = conn.execute("SELECT COUNT(*) as c FROM leads WHERE status = 'skipped'").fetchone()["c"]
    failed = conn.execute("SELECT COUNT(*) as c FROM leads WHERE status = 'failed'").fetchone()["c"]
    pending = conn.execute("SELECT COUNT(*) as c FROM leads WHERE status = 'pending'").fetchone()["c"]
    today_count = get_today_send_count()
    conn.close()
    return {
        "total": total,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "pending": pending,
        "today_sent": today_count,
    }
