"""
毎日の自動メンテナンス（cron: 毎朝6:45実行）

1. failedリードの自動仕分け
   - ネットワークエラー / DBロック → pending（再試行）
   - フォームなし / ボタンなし → skipped（永久除外）

2. リード残量チェック → 不足時はSlackアラート

3. list_generatorでリードを自動補充（残り300件以下なら実行）
"""

import sqlite3
import os
import sys
import subprocess
import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "data/form_sales.db")

# ネットワーク系エラー → pending にリセット（再試行可能）
RETRYABLE_PATTERNS = [
    "ERR_INTERNET_DISCONNECTED",
    "ERR_NAME_NOT_RESOLVED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_REFUSED",
    "ERR_HTTP2_PROTOCOL_ERROR",
    "ERR_TIMED_OUT",
    "Timeout",
    "timeout",
    "database is locked",
    "スクレイピングエラー",
]

# フォーム系エラー → skipped に変換（これ以上試行しない）
PERMANENT_SKIP_PATTERNS = [
    "問い合わせフォームが見つかりません",
    "フォームHTMLを取得できません",
    "送信ボタンが見つかりません",
    "フォームにエラー表示を検出",
]

# リード残量の警告しきい値
LOW_LEAD_THRESHOLD = 300


def load_config():
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def post_slack(webhook_url: str, message: str):
    try:
        requests.post(webhook_url, json={"text": f"<!channel> {message}"}, timeout=10)
    except Exception:
        pass


def main():
    config = load_config()
    webhook_url = config.get("slack", {}).get("webhook_url", "")

    conn = sqlite3.connect(DB_PATH)

    # ---- 1. failedリードの仕分け ----
    failed_rows = conn.execute(
        "SELECT id, skip_reason FROM leads WHERE status = 'failed'"
    ).fetchall()

    retry_ids = []
    skip_ids = []

    for lead_id, reason in failed_rows:
        reason = reason or ""
        if any(p in reason for p in PERMANENT_SKIP_PATTERNS):
            skip_ids.append(lead_id)
        elif any(p in reason for p in RETRYABLE_PATTERNS):
            retry_ids.append(lead_id)
        else:
            # 判断できないものはpendingに戻して再試行
            retry_ids.append(lead_id)

    if retry_ids:
        conn.execute(
            f"UPDATE leads SET status='pending', skip_reason='' WHERE id IN ({','.join(map(str, retry_ids))})"
        )
        print(f"[メンテ] failed→pending リセット: {len(retry_ids)}件")

    if skip_ids:
        conn.execute(
            f"UPDATE leads SET status='skipped', skip_reason='フォーム未対応（自動判定）' WHERE id IN ({','.join(map(str, skip_ids))})"
        )
        print(f"[メンテ] failed→skipped 確定: {len(skip_ids)}件")

    conn.commit()

    # ---- 2. リード残量チェック ----
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status='pending' AND url LIKE 'http%'"
    ).fetchone()[0]
    print(f"[メンテ] 実URL pending残量: {pending_count}件")

    conn.close()

    # ---- 3. リード不足なら自動補充 ----
    if pending_count < LOW_LEAD_THRESHOLD:
        print(f"[メンテ] リード不足（{pending_count}件）→ list_generator を実行します")
        script_dir = os.path.dirname(__file__)

        for source, keyword in [("wantedly", None), ("kyujinbox", "中小企業"), ("green", None)]:
            try:
                cmd = ["python3", os.path.join(script_dir, "list_generator.py"), source, "--pages", "10", "--import"]
                if keyword:
                    cmd += ["--keyword", keyword]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=script_dir)
                print(f"  {source}: {result.stdout.strip()[-100:] if result.stdout else ''}")
            except Exception as e:
                print(f"  {source}: エラー {e}")

        # 補充後の件数確認
        conn2 = sqlite3.connect(DB_PATH)
        new_count = conn2.execute(
            "SELECT COUNT(*) FROM leads WHERE status='pending' AND url LIKE 'http%'"
        ).fetchone()[0]
        conn2.close()

        added = new_count - pending_count
        print(f"[メンテ] 補充後: {new_count}件（+{added}件）")

        if new_count < LOW_LEAD_THRESHOLD and webhook_url:
            post_slack(
                webhook_url,
                f":warning: *リード不足アラート*\n"
                f"実URL pending: {new_count}件（補充後）\n"
                f"このまま続くと300件/日を下回る可能性があります。"
            )

    print("[メンテ] 完了")


if __name__ == "__main__":
    main()
