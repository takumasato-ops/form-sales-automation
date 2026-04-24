"""
フォーム営業 PDCAサイクル — 分析エンジン

週次・累計のパフォーマンスを分析し、改善提案を生成する。
"""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from database import get_weekly_analytics, get_stats


def generate_recommendations(analytics: dict) -> list[str]:
    """分析結果から改善提案を生成"""
    recs = []
    template_stats = analytics.get("template_stats", [])
    industry_stats = analytics.get("industry_stats", [])
    totals = analytics.get("totals", {})

    overall_rate = totals.get("overall_rate") or 0

    # テンプレート改善提案
    if len(template_stats) >= 2:
        best = template_stats[0]
        worst = template_stats[-1]
        if best["response_rate"] > (worst["response_rate"] or 0) + 1:
            recs.append(
                f"✅ テンプレート「{best['template_name']}」が返信率{best['response_rate']}%でトップ。"
                f"「{worst['template_name']}」({worst['response_rate']}%)のリストにも適用を検討してください。"
            )
    elif len(template_stats) == 1:
        recs.append(
            f"📊 現在テンプレートは「{template_stats[0]['template_name']}」のみ使用中。"
            "業種別テンプレート（it / education）も試してA/B比較しましょう。"
        )

    # 業種改善提案
    responded_industries = [r for r in industry_stats if (r.get("responses") or 0) > 0]
    if responded_industries:
        top = responded_industries[0]
        recs.append(
            f"🏆 返信が多い業種：「{top['industry']}」（{top['responses']}件返信 / {top['sent']}件送信）。"
            "この業種のリストを増やすと効果的です。"
        )

    no_response_industries = [r for r in industry_stats if (r.get("responses") or 0) == 0 and r["sent"] >= 5]
    if no_response_industries:
        names = "、".join([r["industry"] for r in no_response_industries[:3]])
        recs.append(
            f"⚠️ 返信ゼロの業種（5件以上送信）：{names}。"
            "文面の見直しまたは業種別テンプレートへの切り替えを検討してください。"
        )

    # 曜日改善提案
    weekday_stats = analytics.get("weekday_stats", [])
    if weekday_stats:
        best_day = max(weekday_stats, key=lambda x: x.get("responses") or 0)
        if (best_day.get("responses") or 0) > 0:
            recs.append(
                f"📅 返信が来た曜日：{best_day['weekday']}曜日（{best_day['responses']}件）。"
                "送信キャンペーンをこの曜日に集中させると返信率が上がる可能性があります。"
            )

    # 全体返信率
    if overall_rate == 0:
        recs.append(
            "📬 まだ返信記録がありません。返信が来たら `python3 main.py respond <ID>` で記録してください。"
            "データが蓄積されると改善提案の精度が上がります。"
        )
    elif overall_rate < 1:
        recs.append(
            f"📈 全体返信率 {overall_rate}%。業界平均（1〜3%）を下回っています。"
            "件名・冒頭の文面・ターゲット業種の見直しを優先しましょう。"
        )
    elif overall_rate >= 3:
        recs.append(
            f"🎉 全体返信率 {overall_rate}%！業界平均を超えています。"
            "このペースを維持しながら送信件数を増やすタイミングです。"
        )

    if not recs:
        recs.append("データが少なく分析できません。50件以上の送信・返信記録が蓄積されると精度が上がります。")

    return recs


def build_weekly_report_text(weeks_back: int = 1) -> str:
    """週次レポートのフォールバック用プレーンテキストを生成"""
    from datetime import datetime, timedelta
    analytics = get_weekly_analytics(weeks_back=weeks_back)
    stats = get_stats()
    totals = analytics.get("totals", {})
    period_start = (datetime.now() - timedelta(weeks=weeks_back)).strftime("%m/%d")
    period_end   = datetime.now().strftime("%m/%d")
    return (
        f"📊 *フォーム営業 週次レポート（{period_start}〜{period_end}）*\n"
        f"累計送信：{stats['sent']}件 / 返信率：{totals.get('overall_rate') or 0}% "
        f"/ 未処理残：{stats['pending']}件"
    )


def build_weekly_blocks(weeks_back: int = 1) -> list:
    """
    Slack Block Kit 形式で週次レポートを生成する。
    見やすいセクション・フィールド・区切り線で構成。
    """
    from datetime import datetime, timedelta
    analytics = get_weekly_analytics(weeks_back=weeks_back)
    stats     = get_stats()
    recs      = generate_recommendations(analytics)
    totals    = analytics.get("totals", {})

    period_start = (datetime.now() - timedelta(weeks=weeks_back)).strftime("%Y/%m/%d")
    period_end   = datetime.now().strftime("%Y/%m/%d")
    overall_rate = totals.get("overall_rate") or 0
    total_resp   = totals.get("total_responses") or 0
    total_sent_t = totals.get("total_sent") or 0

    # 返信率に応じたラベル
    if overall_rate >= 3:
        rate_label = f"🟢 *{overall_rate}%*  _(業界平均超え！)_"
    elif overall_rate >= 1:
        rate_label = f"🟡 *{overall_rate}%*  _(業界平均水準)_"
    else:
        rate_label = f"🔴 *{overall_rate}%*  _(改善の余地あり)_"

    blocks = []

    # ── ヘッダー ──────────────────────────────
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "📊 フォーム営業 週次 PDCA レポート", "emoji": True}
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                       "text": f"📅  集計期間：*{period_start} 〜 {period_end}*　｜　🏢 株式会社キャナルAI"}]
    })
    blocks.append({"type": "divider"})

    # ── KPI サマリー ──────────────────────────
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*📈  今週の実績サマリー*"}
    })
    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"📬  *累計送信数*\n`{stats['sent']}件`"},
            {"type": "mrkdwn", "text": f"💬  *累計返信数*\n`{total_resp}件`"},
            {"type": "mrkdwn", "text": f"📊  *返信率*\n{rate_label}"},
            {"type": "mrkdwn", "text": f"⏳  *未処理残*\n`{stats['pending']}件`"},
            {"type": "mrkdwn", "text": f"⏭️  *スキップ*\n`{stats['skipped']}件`"},
            {"type": "mrkdwn", "text": f"❌  *失敗*\n`{stats['failed']}件`"},
        ]
    })
    blocks.append({"type": "divider"})

    # ── テンプレート別パフォーマンス ──────────
    template_stats = analytics.get("template_stats", [])
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*🎯  テンプレート別パフォーマンス*"}
    })
    if template_stats:
        lines = []
        for t in template_stats:
            rate = t.get("response_rate") or 0
            filled = int(rate * 3)
            bar = "█" * filled + "░" * max(0, 15 - filled)
            if rate >= 3:
                icon = "🟢"
            elif rate >= 1:
                icon = "🟡"
            else:
                icon = "🔴"
            lines.append(
                f"{icon}  `{t['template_name']:<10}`  {bar}  *{rate}%*"
                f"  _（{t.get('responses') or 0} / {t['sent']} 件）_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })
    else:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_まだ送信データがありません_"}]
        })
    blocks.append({"type": "divider"})

    # ── 業種別ランキング ──────────────────────
    industry_stats = analytics.get("industry_stats", [])
    responded = [r for r in industry_stats if (r.get("responses") or 0) > 0]
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*🏆  返信が来た業種 TOP 5*"}
    })
    if responded:
        lines = []
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, r in enumerate(responded[:5]):
            rate = r.get("response_rate") or 0
            lines.append(
                f"{medals[i]}  *{r['industry']}*　"
                f"{r.get('responses') or 0}件返信 / {r['sent']}件送信　_({rate}%)_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })
    else:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn",
                           "text": "_まだ返信記録がありません。返信が来たら `python3 main.py respond <ID>` で記録してください。_"}]
        })
    blocks.append({"type": "divider"})

    # ── 曜日別パフォーマンス ──────────────────
    weekday_stats = analytics.get("weekday_stats", [])
    if weekday_stats:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*📅  曜日別 送信・返信数*"}
        })
        lines = []
        for w in weekday_stats:
            resp = w.get("responses") or 0
            rate = round(resp / max(w["sent"], 1) * 100, 1)
            bar  = "▪" * min(w["sent"] // 5 + 1, 12)
            lines.append(f"`{w['weekday']}曜`  {bar}  {w['sent']}件送信 / {resp}件返信 _{rate}%_")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })
        blocks.append({"type": "divider"})

    # ── 改善提案 ──────────────────────────────
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*💡  今週の改善提案*"}
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(f"> {rec}" for rec in recs)}
    })
    blocks.append({"type": "divider"})

    # ── フッター ──────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                       "text": "🤖  _毎週月曜 8:00 自動配信　｜　データが増えるほど提案精度が上がります_"}]
    })

    return blocks


def print_report_to_console(weeks_back: int = 1):
    """コンソールにレポートを表示"""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console()
    analytics = get_weekly_analytics(weeks_back=weeks_back)
    stats = get_stats()
    recs = generate_recommendations(analytics)
    totals = analytics.get("totals", {})

    from datetime import datetime, timedelta
    period_start = (datetime.now() - timedelta(weeks=weeks_back)).strftime("%m/%d")
    period_end = datetime.now().strftime("%m/%d")

    # ヘッダー
    console.print(Panel(
        f"[bold]週次PDCAレポート  {period_start}〜{period_end}[/bold]\n"
        f"累計送信：{stats['sent']}件  |  返信率：{totals.get('overall_rate') or 0}%"
        f"（{totals.get('total_responses') or 0}/{totals.get('total_sent') or 0}件）  |  未処理残：{stats['pending']}件",
        title="フォーム営業 PDCA",
        style="bold blue",
    ))

    # テンプレート別
    if analytics["template_stats"]:
        t_table = Table(title="テンプレート別パフォーマンス")
        t_table.add_column("テンプレート", style="cyan")
        t_table.add_column("送信", justify="right")
        t_table.add_column("返信", justify="right", style="green")
        t_table.add_column("返信率", justify="right", style="bold")
        for t in analytics["template_stats"]:
            rate = t["response_rate"] or 0
            color = "green" if rate >= 2 else ("yellow" if rate >= 1 else "red")
            t_table.add_row(
                t["template_name"],
                str(t["sent"]),
                str(t["responses"] or 0),
                f"[{color}]{rate}%[/{color}]",
            )
        console.print(t_table)

    # 業種別
    if analytics["industry_stats"]:
        i_table = Table(title="業種別パフォーマンス（送信上位）")
        i_table.add_column("業種", style="cyan")
        i_table.add_column("送信", justify="right")
        i_table.add_column("返信", justify="right", style="green")
        i_table.add_column("返信率", justify="right")
        for r in analytics["industry_stats"][:8]:
            rate = r["response_rate"] or 0
            color = "green" if rate >= 2 else ("yellow" if rate >= 1 else "dim")
            i_table.add_row(
                r["industry"],
                str(r["sent"]),
                str(r["responses"] or 0),
                f"[{color}]{rate}%[/{color}]",
            )
        console.print(i_table)

    # 改善提案
    console.print(Panel(
        "\n".join(recs),
        title="今週の改善提案",
        style="yellow",
    ))
