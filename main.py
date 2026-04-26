"""
フォーム営業自動化ツール - メインオーケストレーター
全モジュールを統合し、CLIから操作する。
"""

import asyncio
import sys
import os
import random
import yaml
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.panel import Panel
from rich import print as rprint

import database as db
import ai_engine as ai
import scraper
import form_filler
import analytics as anlx

console = Console()


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_template(name: str = "default") -> str:
    template_path = os.path.join(os.path.dirname(__file__), "templates", f"{name}.txt")
    if not os.path.exists(template_path):
        console.print(f"[red]テンプレートが見つかりません: {name}.txt[/red]")
        raise FileNotFoundError(template_path)
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def list_templates() -> list[str]:
    """利用可能なテンプレート一覧を返す"""
    templates_dir = os.path.join(os.path.dirname(__file__), "templates")
    return [os.path.splitext(f)[0] for f in os.listdir(templates_dir) if f.endswith(".txt")]


def check_business_hours(config: dict) -> bool:
    """営業時間内かチェック"""
    if not config["rate_limit"]["business_hours_only"]:
        return True
    now = datetime.now()
    hour = now.hour
    return config["rate_limit"]["business_hour_start"] <= hour < config["rate_limit"]["business_hour_end"]


def show_stats():
    """統計情報を表示"""
    stats = db.get_stats()
    table = Table(title="送信統計")
    table.add_column("項目", style="cyan")
    table.add_column("件数", style="magenta", justify="right")
    table.add_row("総リード数", str(stats["total"]))
    table.add_row("送信済み", str(stats["sent"]))
    table.add_row("スキップ", str(stats["skipped"]))
    table.add_row("失敗", str(stats["failed"]))
    table.add_row("未処理", str(stats["pending"]))
    table.add_row("本日の送信数", str(stats["today_sent"]))
    console.print(table)


async def process_single_lead(lead: dict, context, template: str, config: dict) -> dict:
    """1社分の処理を実行"""
    company = lead["company_name"]
    url = lead["url"]
    lead_id = lead["id"]

    result = {"status": "failed", "message": ""}

    try:
        # Step 1: 企業サイトをスクレイピング
        console.print(f"  [dim]├─ サイト解析中: {url}[/dim]")
        site_data = await scraper.scrape_company(url, context)

        if site_data.get("error"):
            result["message"] = f"スクレイピングエラー: {site_data['error']}"
            db.update_lead(lead_id, status="failed", skip_reason=result["message"])
            db.log_send(lead_id, success=False, error_message=result["message"])
            return result

        # Step 2: 営業お断りチェック
        if site_data["has_refusal"]:
            result["status"] = "skipped"
            result["message"] = f"営業お断り検出: 「{site_data['refusal_keyword']}」"
            db.update_lead(lead_id, status="skipped", skip_reason=result["message"])
            console.print(f"  [yellow]├─ ⚠ スキップ: {result['message']}[/yellow]")
            return result

        # Step 3: フォームURLが見つからない場合
        if not site_data["contact_form_url"]:
            result["status"] = "skipped"
            result["message"] = "問い合わせフォームが見つかりません"
            db.update_lead(lead_id, status="skipped", skip_reason=result["message"])
            console.print(f"  [yellow]├─ ⚠ スキップ: {result['message']}[/yellow]")
            return result

        if not site_data["form_html"]:
            result["status"] = "skipped"
            result["message"] = "フォームHTMLを取得できません"
            db.update_lead(lead_id, status="skipped", skip_reason=result["message"])
            console.print(f"  [yellow]├─ ⚠ スキップ: {result['message']}[/yellow]")
            return result

        # Step 4: AIで企業情報を要約
        console.print(f"  [dim]├─ AI分析中...[/dim]")
        company_info = ai.summarize_company(company, site_data["page_text"])
        db.update_lead(lead_id, ai_summary=str(company_info), industry=company_info.get("industry", ""))

        # Step 4b: ターゲット条件チェック
        is_target, skip_reason = ai.check_target_criteria(company_info, config)
        if not is_target:
            result["status"] = "skipped"
            result["message"] = skip_reason
            db.update_lead(lead_id, status="skipped", skip_reason=skip_reason)
            console.print(f"  [yellow]├─ ⚠ ターゲット外: {skip_reason}[/yellow]")
            return result

        emp = company_info.get("employee_count")
        cap = company_info.get("capital_man")
        listing = company_info.get("listing", "unknown")
        detail = f"従業員:{emp or '不明'}名 / 資本金:{cap or '不明'}万円（参考） / 上場:{listing}（参考）"
        console.print(f"  [dim]├─ ✓ ターゲット判定: {detail}[/dim]")

        # Step 5: 日程候補を生成
        schedule_slots = ai.generate_schedule_slots(config)

        # Step 6: 営業文面をパーソナライズ
        booking_url = config["scheduling"].get("booking_url", "")
        message = ai.personalize_message(template, company, company_info, schedule_slots, booking_url)
        subject = ai.generate_subject(company, company_info)
        db.update_lead(lead_id, personalized_message=message)

        # Step 7: AIでフォームフィールドを解析
        console.print(f"  [dim]├─ フォーム解析中...[/dim]")
        form_analysis = ai.analyze_form_fields(site_data["form_html"])

        # Step 8: フォームページに移動して入力・送信
        console.print(f"  [dim]├─ フォーム入力・送信中...[/dim]")
        page = await context.new_page()
        timeout = config["browser"]["timeout_sec"] * 1000

        try:
            await page.goto(site_data["contact_form_url"], wait_until="domcontentloaded", timeout=timeout)
            await page.wait_for_timeout(2000)

            fill_result = await form_filler.fill_form(
                page=page,
                field_mappings=form_analysis.get("field_mappings", []),
                submit_selector=form_analysis.get("submit_selector", ""),
                sender=config["sender"],
                subject=subject,
                message=message,
            )

            if fill_result["success"]:
                result["status"] = "sent"
                result["message"] = "送信成功"
                db.update_lead(lead_id, status="sent", sent_at=datetime.now().isoformat(),
                             contact_form_url=site_data["contact_form_url"])
                db.log_send(lead_id, success=True, screenshot_path=fill_result.get("screenshot_path", ""))
                db.increment_daily_counter()
            else:
                result["message"] = fill_result.get("error", "送信失敗")
                db.update_lead(lead_id, status="failed", skip_reason=result["message"],
                             contact_form_url=site_data["contact_form_url"])
                db.log_send(lead_id, success=False, error_message=result["message"],
                          screenshot_path=fill_result.get("screenshot_path", ""))
        finally:
            await page.close()

    except Exception as e:
        result["message"] = str(e)
        db.update_lead(lead_id, status="failed", skip_reason=str(e))
        db.log_send(lead_id, success=False, error_message=str(e))

    return result


async def run_campaign(max_sends: int = None, template_name: str = None):
    """営業キャンペーンを実行"""
    config = load_config()
    # テンプレート指定がない場合はリードごとのtemplate_nameを使用
    fixed_template = load_template(template_name) if template_name else None

    if not check_business_hours(config):
        console.print("[red]現在は営業時間外です。営業時間内に再実行してください。[/red]")
        console.print(f"[dim]営業時間: {config['rate_limit']['business_hour_start']}:00 〜 {config['rate_limit']['business_hour_end']}:00（平日のみ）[/dim]")
        return

    # 今日の残り送信可能数を計算
    today_count = db.get_today_send_count()
    daily_limit = config["rate_limit"]["max_per_day"]
    remaining = daily_limit - today_count

    if remaining <= 0:
        console.print(f"[red]本日の送信上限（{daily_limit}件）に達しています。[/red]")
        return

    if max_sends:
        remaining = min(remaining, max_sends)

    # 未処理リードを取得
    leads = db.get_pending_leads(limit=remaining, template_name=template_name)
    if not leads:
        msg = f"テンプレート '{template_name}' の未処理リードがありません。" if template_name else "未処理のリードがありません。CSVをインポートしてください。"
        console.print(f"[yellow]{msg}[/yellow]")
        return

    console.print(Panel(
        f"処理対象: {len(leads)}社 / 本日残り: {remaining}件 / 1日上限: {daily_limit}件",
        title="キャンペーン開始",
        style="green",
    ))

    # ブラウザ起動
    pw, browser, context = await scraper.create_browser()

    sent_count = 0
    skip_count = 0
    fail_count = 0

    try:
        for i, lead in enumerate(leads):
            # 営業時間チェック（ループ中も確認）
            if not check_business_hours(config):
                console.print("\n[red]営業時間外になりました。残りは次回に持ち越します。[/red]")
                break

            # 日次上限チェック
            if db.get_today_send_count() >= daily_limit:
                console.print(f"\n[red]本日の送信上限（{daily_limit}件）に達しました。[/red]")
                break

            console.print(f"\n[bold cyan]▶ [{i+1}/{len(leads)}] {lead['company_name']}[/bold cyan]")

            # リードごとのテンプレートを使用（固定指定がある場合はそちら優先）
            template = fixed_template or load_template(lead.get("template_name", "default"))
            result = await process_single_lead(lead, context, template, config)

            if result["status"] == "sent":
                sent_count += 1
                console.print(f"  [green]└─ ✓ 送信完了[/green]")
            elif result["status"] == "skipped":
                skip_count += 1
            else:
                fail_count += 1
                console.print(f"  [red]└─ ✗ 失敗: {result['message'][:80]}[/red]")

            # 次の処理まで待機（bot検出を回避）
            if i < len(leads) - 1:
                if result["status"] == "sent":
                    interval = random.randint(
                        config["rate_limit"]["min_interval_sec"],
                        config["rate_limit"]["max_interval_sec"],
                    )
                    console.print(f"  [dim]⏳ 次の送信まで {interval}秒 待機...[/dim]")
                else:
                    interval = random.randint(3, 8)
                await asyncio.sleep(interval)

    finally:
        await browser.close()
        await pw.stop()

    # 結果サマリー
    console.print(Panel(
        f"送信: {sent_count}件 / スキップ: {skip_count}件 / 失敗: {fail_count}件",
        title="キャンペーン完了",
        style="bold green" if fail_count == 0 else "bold yellow",
    ))

    # 失敗・スキップがあれば自動でリトライリストCSV出力
    if skip_count > 0 or fail_count > 0:
        path = export_retry_list()
        if path:
            console.print(f"[yellow]⚠ 失敗・スキップ分のリトライリストを出力しました: {path}[/yellow]")
            console.print(f"[dim]  再送信するには: python3 main.py retry --reset[/dim]")


def cmd_import(csv_path: str, template_name: str = "default"):
    """CSVインポート（テンプレート名を指定可能）"""
    if not os.path.exists(csv_path):
        console.print(f"[red]ファイルが見つかりません: {csv_path}[/red]")
        return
    available = list_templates()
    if template_name not in available:
        console.print(f"[red]テンプレート '{template_name}' が存在しません。[/red]")
        console.print(f"[dim]利用可能: {', '.join(available)}[/dim]")
        return
    count = db.import_leads_from_csv(csv_path, template_name=template_name)
    console.print(f"[green]{count}件のリードをインポートしました。テンプレート: {template_name}[/green]")


def cmd_stats():
    """統計表示"""
    show_stats()


def cmd_preview(limit: int = 3):
    """パーソナライズのプレビュー（送信せずにAI生成結果を確認）"""
    config = load_config()
    template = load_template()
    leads = db.get_pending_leads(limit=limit)

    if not leads:
        console.print("[yellow]未処理のリードがありません。[/yellow]")
        return

    for lead in leads:
        console.print(f"\n[bold cyan]━━━ {lead['company_name']} ━━━[/bold cyan]")
        schedule_slots = ai.generate_schedule_slots(config)

        # 簡易的にダミー情報で生成（実際のスクレイピングはしない）
        company_info = {
            "industry": lead.get("industry", "IT"),
            "summary": f"{lead['company_name']}の事業展開",
        }
        booking_url = config["scheduling"].get("booking_url", "")
        message = ai.personalize_message(template, lead["company_name"], company_info, schedule_slots, booking_url)
        console.print(message)
        console.print()


def cmd_list():
    """リスト別・テンプレート別の送信状況を表示"""
    stats = db.get_list_stats()
    if not stats:
        console.print("[yellow]データがありません。[/yellow]")
        return
    table = Table(title="リスト管理 — ソースファイル × テンプレート別")
    table.add_column("ソースCSV", style="cyan")
    table.add_column("テンプレート", style="magenta")
    table.add_column("合計", justify="right")
    table.add_column("送信済", style="green", justify="right")
    table.add_column("スキップ", style="yellow", justify="right")
    table.add_column("失敗", style="red", justify="right")
    table.add_column("未処理", style="dim", justify="right")
    for s in stats:
        table.add_row(
            s["source_file"] or "（不明）",
            s["template_name"] or "default",
            str(s["total"]),
            str(s["sent"]),
            str(s["skipped"]),
            str(s["failed"]),
            str(s["pending"]),
        )
    console.print(table)


def cmd_sent():
    """送信済みリードの一覧を表示"""
    leads = db.get_sent_leads()
    if not leads:
        console.print("[yellow]送信済みのリードがありません。[/yellow]")
        return
    table = Table(title=f"送信済みリード（{len(leads)}件）")
    table.add_column("ID", style="dim", justify="right")
    table.add_column("企業名", style="cyan")
    table.add_column("業種", style="magenta")
    table.add_column("ソースCSV", style="dim")
    table.add_column("テンプレート", style="dim")
    table.add_column("送信日時", style="green")
    for lead in leads:
        table.add_row(
            str(lead["id"]),
            lead["company_name"],
            lead["industry"] or "不明",
            lead["source_file"] or "不明",
            lead["template_name"] or "default",
            (lead["sent_at"] or "")[:16],
        )
    console.print(table)


def export_retry_list() -> str:
    """失敗・スキップリードをCSVに出力"""
    import csv
    from datetime import datetime
    leads = db.get_retry_leads()
    if not leads:
        return ""
    exports_dir = os.path.join(os.path.dirname(__file__), "data", "exports")
    os.makedirs(exports_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(exports_dir, f"retry_list_{timestamp}.csv")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "company_name", "url", "industry", "source_file", "template_name", "status", "skip_reason"])
        writer.writeheader()
        writer.writerows(leads)
    return path


def cmd_retry(reset: bool = False):
    """失敗・スキップリードの一覧表示 + リセット"""
    leads = db.get_retry_leads()
    if not leads:
        console.print("[green]失敗・スキップのリードはありません。[/green]")
        return

    # テーブル表示
    table = Table(title=f"リトライ対象リード（{len(leads)}件）")
    table.add_column("ID", style="dim", justify="right")
    table.add_column("企業名", style="cyan")
    table.add_column("業種")
    table.add_column("ステータス", style="yellow")
    table.add_column("理由", style="dim")
    by_status = {}
    for lead in leads:
        st = lead["status"]
        by_status[st] = by_status.get(st, 0) + 1
        table.add_row(
            str(lead["id"]),
            lead["company_name"],
            lead["industry"] or "不明",
            st,
            (lead["skip_reason"] or "")[:50],
        )
    console.print(table)

    # CSV出力
    path = export_retry_list()
    if path:
        console.print(f"[green]✓ リトライリストをCSV出力: {path}[/green]")

    if reset:
        count = db.reset_for_retry()
        console.print(f"[green]✓ {count}件をpendingに戻しました。次回 run で再送信されます。[/green]")
    else:
        console.print(f"\n[dim]pendingに戻して再送信するには: python3 main.py retry --reset[/dim]")


def cmd_respond(lead_id: int, notes: str = ""):
    """リードへの返信をマーク"""
    conn = db.get_connection()
    lead = conn.execute("SELECT id, company_name, status FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if not lead:
        console.print(f"[red]ID {lead_id} のリードが見つかりません。[/red]")
        return
    if lead["status"] != "sent":
        console.print(f"[yellow]ID {lead_id}（{lead['company_name']}）はステータス '{lead['status']}' です。[/yellow]")
    db.mark_responded(lead_id, notes)
    console.print(f"[green]✓ {lead['company_name']} を返信ありとして記録しました。[/green]")
    if notes:
        console.print(f"[dim]  メモ: {notes}[/dim]")


def cmd_report(weeks_back: int = 1):
    """週次PDCAレポートをコンソールに表示"""
    anlx.print_report_to_console(weeks_back=weeks_back)


def cmd_export():
    """全データをCSV/Google Sheetsにエクスポート"""
    import sheets_export
    config = load_config()

    # CSV エクスポート
    path = sheets_export.export_csv()
    console.print(f"[green]✓ CSVエクスポート完了: {path}[/green]")

    # Google Sheets（設定済みの場合）
    sheets_cfg = config.get("sheets", {})
    if sheets_cfg.get("spreadsheet_id") and sheets_cfg.get("service_account_json"):
        console.print("[dim]Google Sheetsに同期中...[/dim]")
        success = sheets_export.export_to_sheets(config)
        if success:
            console.print("[green]✓ Google Sheetsへの同期完了[/green]")
        else:
            console.print("[yellow]Google Sheets同期に失敗しました（CSVは出力済み）[/yellow]")
    else:
        console.print("[dim]Google Sheets未設定。config.yaml の sheets セクションを設定するとスプレッドシートにも自動同期されます。[/dim]")


def cmd_templates():
    """利用可能なテンプレート一覧を表示"""
    available = list_templates()
    table = Table(title="テンプレート一覧")
    table.add_column("名前", style="cyan")
    table.add_column("ファイル", style="dim")
    table.add_column("用途", style="magenta")
    descriptions = {
        "default":       "汎用・長文版（全業種対応）※メイン使用",
        "default_short": "汎用・短文版（未使用）",
        "it":            "IT・SaaS向け長文版",
        "it_short":      "IT・SaaS向け短文版（未使用）",
        "education":     "教育業界向け",
    }
    for name in sorted(available):
        table.add_row(
            name,
            f"templates/{name}.txt",
            descriptions.get(name, "—"),
        )
    console.print(table)


def print_help():
    console.print(Panel("""
[bold]フォーム営業自動化ツール[/bold]

[cyan]基本コマンド:[/cyan]
  python3 main.py import <CSV> [--template <名前>]  リードをインポート（テンプレート指定可）
  python3 main.py run [--max N] [--template <名前>] 営業キャンペーンを実行
  python3 main.py stats                             送信統計を表示
  python3 main.py preview [--limit N]               文面プレビュー（送信しない）

[cyan]管理コマンド:[/cyan]
  python3 main.py list                   リスト別・テンプレート別の送信状況
  python3 main.py sent                   送信済みリードの一覧
  python3 main.py templates             利用可能なテンプレート一覧

[cyan]PDCAコマンド:[/cyan]
  python3 main.py respond <ID> [メモ]   返信ありとして記録
  python3 main.py report [--weeks N]    週次PDCAレポートを表示
  python3 main.py export                全データをCSV/Sheetsにエクスポート

[cyan]テンプレート一覧:[/cyan]
  default    汎用（全業種対応）
  it         IT・SaaS企業向け
  education  教育業界向け

[cyan]使用例:[/cyan]
  python3 main.py import data/leads.csv --template it
  python3 main.py run --template it --max 50
  python3 main.py respond 42 "興味ありとの返信。来週商談予定"
  python3 main.py report
  python3 main.py export
""", title="ヘルプ", style="blue"))


def main():
    db.init_db()

    if len(sys.argv) < 2:
        print_help()
        return

    command = sys.argv[1]

    def get_flag(flag: str, default=None):
        if flag in sys.argv:
            idx = sys.argv.index(flag)
            if idx + 1 < len(sys.argv):
                return sys.argv[idx + 1]
        return default

    if command == "import" and len(sys.argv) >= 3:
        template_name = get_flag("--template", "default")
        cmd_import(sys.argv[2], template_name=template_name)

    elif command == "run":
        max_sends = get_flag("--max")
        if max_sends:
            max_sends = int(max_sends)
        template_name = get_flag("--template")
        asyncio.run(run_campaign(max_sends=max_sends, template_name=template_name))

    elif command == "stats":
        cmd_stats()

    elif command == "preview":
        limit = get_flag("--limit", 3)
        cmd_preview(int(limit))

    elif command == "list":
        cmd_list()

    elif command == "sent":
        cmd_sent()

    elif command == "templates":
        cmd_templates()

    elif command == "retry":
        reset = "--reset" in sys.argv
        cmd_retry(reset=reset)

    elif command == "respond" and len(sys.argv) >= 3:
        lead_id = int(sys.argv[2])
        notes = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
        cmd_respond(lead_id, notes)

    elif command == "report":
        weeks = get_flag("--weeks", 1)
        cmd_report(int(weeks))

    elif command == "export":
        cmd_export()

    elif command == "help":
        print_help()

    else:
        print_help()


if __name__ == "__main__":
    main()
