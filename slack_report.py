"""
フォーム営業 日次・週次レポートをSlackに自動投稿
- #04-営業部 に @channel メンション付きで親投稿
- スレッドに詳細レポートを投稿
- 週次レポートはPDCA改善提案付き
"""

import requests
import json
import sys
import os
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))
from database import get_stats, get_connection
from analytics import build_weekly_report_text, build_weekly_blocks


def get_today_detail() -> dict:
    """今日の送信詳細（業種別）を取得"""
    conn = get_connection()
    today = date.today().isoformat()
    rows = conn.execute("""
        SELECT l.industry, COUNT(*) as cnt
        FROM send_log sl
        JOIN leads l ON sl.lead_id = l.id
        WHERE sl.sent_at LIKE ? AND sl.success = 1
        GROUP BY l.industry
        ORDER BY cnt DESC
        LIMIT 5
    """, (today + "%",)).fetchall()

    failed_rows = conn.execute("""
        SELECT l.company_name, sl.error_message
        FROM send_log sl
        JOIN leads l ON sl.lead_id = l.id
        WHERE sl.sent_at LIKE ? AND sl.success = 0
        ORDER BY sl.id DESC
        LIMIT 3
    """, (today + "%",)).fetchall()

    conn.close()
    return {
        "by_industry": [dict(r) for r in rows],
        "recent_failures": [dict(r) for r in failed_rows],
    }


def post_slack_report(bot_token: str, channel_name: str, webhook_url: str):
    """Slackにレポートを投稿（親投稿 + スレッド）"""

    channel_id = channel_name  # チャンネル名またはIDをそのまま使用

    # ① 親投稿：@channel メンション
    parent_resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={
            "channel": channel_id,
            "text": "<!channel> *フォーム営業 日次レポート*",
        }
    )
    parent_data = parent_resp.json()
    parent_ts = parent_data.get("ts")

    if not parent_ts:
        print(f"親投稿失敗: {parent_data}")
        return

    # ② スレッド：詳細レポート
    stats = get_stats()
    detail = get_today_detail()
    today_str = date.today().strftime("%Y年%m月%d日")

    success_rate = 0
    total_processed = stats["today_sent"] + (stats["failed"] if stats["failed"] else 0)
    if total_processed > 0:
        success_rate = round(stats["today_sent"] / total_processed * 100)

    # 業種別サマリー
    industry_text = ""
    if detail["by_industry"]:
        industry_text = "\n*📊 業種別 送信数（上位）*\n"
        for row in detail["by_industry"]:
            industry_text += f"　• {row['industry']}：{row['cnt']}件\n"

    report_text = f"""*{today_str} フォーム営業 実行レポート*

*📬 本日の送信結果*
　• 送信成功：*{stats['today_sent']}件*
　• スキップ：{stats['skipped']}件（営業お断り等）
　• 失敗：{stats['failed']}件
{industry_text}
*📈 累計ステータス*
　• 総リード数：{stats['total']}件
　• 送信済み合計：{stats['sent']}件
　• 残り未処理：{stats['pending']}件

*⚙️ 稼働設定*
　• 1日上限：300件
　• 送信間隔：20〜45秒
　• 実行時間：毎朝7:00〜20:00（自動）

次回実行は明朝7:00です。"""

    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={
            "channel": channel_id,
            "thread_ts": parent_ts,
            "text": report_text,
        }
    )
    print("Slackレポート投稿完了")


def post_weekly_slack_report(bot_token: str, channel_name: str):
    """週次PDCAレポートをSlackに投稿"""
    # 親投稿
    parent_resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={
            "channel": channel_name,
            "text": "<!channel> *📊 フォーム営業 週次PDCAレポート*",
        }
    )
    parent_data = parent_resp.json()
    parent_ts = parent_data.get("ts")

    if not parent_ts:
        print(f"週次レポート親投稿失敗: {parent_data}")
        return

    # スレッドに週次分析レポート（Block Kit）
    blocks = build_weekly_blocks(weeks_back=1)
    fallback_text = build_weekly_report_text(weeks_back=1)

    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={
            "channel": channel_name,
            "thread_ts": parent_ts,
            "text": fallback_text,
            "blocks": blocks,
        }
    )
    print("週次Slackレポート投稿完了")


if __name__ == "__main__":
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    slack_cfg = config.get("slack", {})
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "weekly":
        post_weekly_slack_report(
            bot_token=slack_cfg["bot_token"],
            channel_name=slack_cfg["channel"],
        )
    else:
        post_slack_report(
            bot_token=slack_cfg["bot_token"],
            channel_name=slack_cfg["channel"],
            webhook_url=slack_cfg["webhook_url"],
        )
