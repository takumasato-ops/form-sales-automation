"""
企業リスト自動生成モジュール

無料の公開データから営業ターゲット企業リストを自動生成する。

対応ソース:
  - Wantedly（IT・スタートアップ系）
  - Green（IT系転職サイト）
  - 求人ボックス（幅広い業種）

使い方:
  python3 list_generator.py wantedly --pages 5
  python3 list_generator.py green --pages 5
  python3 list_generator.py kyujinbox --keyword "メーカー" --pages 5
  python3 list_generator.py all --pages 3
"""

import asyncio
import csv
import os
import re
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "generated")


def normalize_url(url: str) -> str:
    """URLを正規化（末尾スラッシュ統一など）"""
    if not url:
        return ""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def is_duplicate(url: str, existing: set) -> bool:
    base = normalize_url(url)
    return base in existing


async def scrape_wantedly(browser, pages: int = 5) -> list[dict]:
    """
    Wantedlyから企業リストを取得
    https://www.wantedly.com/companies
    """
    results = []
    seen = set()
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        locale="ja-JP",
    )
    page = await context.new_page()

    print("  [Wantedly] スクレイピング開始...")
    for p in range(1, pages + 1):
        try:
            url = f"https://www.wantedly.com/companies?page={p}"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")

            # 企業カードを探す
            cards = soup.find_all("a", href=re.compile(r"/companies/[a-z0-9_-]+$"))
            for card in cards:
                href = card.get("href", "")
                if not href or "/companies/" not in href:
                    continue
                full_url = f"https://www.wantedly.com{href}" if href.startswith("/") else href

                # 企業名を取得
                company_name = ""
                name_el = card.find(["h2", "h3", "p", "span"], string=True)
                if name_el:
                    company_name = name_el.get_text(strip=True)

                # 企業の公式HP URLは個別ページから取得（ここではWantedly URLを仮置き）
                if company_name and full_url not in seen:
                    seen.add(full_url)

            # 別の方法でカードを探す
            company_links = await page.eval_on_selector_all(
                "a[href*='/companies/']",
                """els => els.map(e => ({
                    href: e.href,
                    text: e.textContent.trim().substring(0, 100)
                }))"""
            )

            for link in company_links:
                href = link["href"]
                if re.search(r"/companies/[a-z0-9_-]+$", href):
                    if href not in seen:
                        seen.add(href)

            print(f"  [Wantedly] page {p}: 累計 {len(seen)} 社")
            await page.wait_for_timeout(1500)

        except Exception as e:
            print(f"  [Wantedly] page {p} エラー: {e}")
            continue

    # Wantedly企業ページから公式URLを収集
    collected = list(seen)[:min(len(seen), pages * 30)]
    print(f"  [Wantedly] 企業ページから公式URL取得中（{len(collected)}社）...")

    for i, wantedly_url in enumerate(collected):
        try:
            await page.goto(wantedly_url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(1000)

            # 企業名
            company_name = ""
            name_el = await page.query_selector("h1")
            if name_el:
                company_name = (await name_el.inner_text()).strip()

            # 公式HP URL
            official_url = ""
            links = await page.eval_on_selector_all(
                "a[href]",
                "els => els.map(e => e.href)"
            )
            for link in links:
                parsed = urlparse(link)
                if parsed.netloc and "wantedly.com" not in parsed.netloc and parsed.scheme in ("http", "https"):
                    if not any(s in parsed.netloc for s in ["twitter", "facebook", "instagram", "linkedin", "youtube"]):
                        official_url = f"{parsed.scheme}://{parsed.netloc}"
                        break

            # 業種
            industry = ""
            industry_el = await page.query_selector("[class*='industry'], [class*='Industry']")
            if industry_el:
                industry = (await industry_el.inner_text()).strip()

            if company_name and official_url:
                results.append({
                    "company_name": company_name,
                    "url": official_url,
                    "industry": industry or "IT/スタートアップ",
                    "source": "wantedly",
                })

            if (i + 1) % 10 == 0:
                print(f"  [Wantedly] {i+1}/{len(collected)} 処理済")

            await page.wait_for_timeout(800)

        except Exception as e:
            continue

    await context.close()
    print(f"  [Wantedly] 完了: {len(results)}社取得")
    return results


async def scrape_green(browser, pages: int = 5) -> list[dict]:
    """
    Green（IT系転職サイト）から企業リストを取得
    https://www.green-japan.com/company_top
    """
    results = []
    seen = set()
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        locale="ja-JP",
    )
    page = await context.new_page()

    print("  [Green] スクレイピング開始...")
    for p in range(1, pages + 1):
        try:
            url = f"https://www.green-japan.com/company_top?page={p}"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # 企業カードを取得
            cards = await page.eval_on_selector_all(
                "a[href*='/company/']",
                """els => els.map(e => ({
                    href: e.href,
                    text: e.closest('[class*=\"company\"], [class*=\"card\"], li, article')
                         ? e.closest('[class*=\"company\"], [class*=\"card\"], li, article').textContent.trim().substring(0, 200)
                         : e.textContent.trim()
                }))"""
            )

            for card in cards:
                href = card["href"]
                if re.search(r"/company/\d+", href) and href not in seen:
                    seen.add(href)

            print(f"  [Green] page {p}: 累計 {len(seen)} 社")
            await page.wait_for_timeout(1500)

        except Exception as e:
            print(f"  [Green] page {p} エラー: {e}")
            continue

    # 各企業ページから詳細取得
    collected = list(seen)[:min(len(seen), pages * 20)]
    print(f"  [Green] 企業詳細取得中（{len(collected)}社）...")

    for i, green_url in enumerate(collected):
        try:
            await page.goto(green_url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(1000)

            # 企業名
            company_name = ""
            for sel in ["h1", "[class*='company-name']", "[class*='companyName']"]:
                el = await page.query_selector(sel)
                if el:
                    company_name = (await el.inner_text()).strip()
                    break

            # 公式URL
            official_url = ""
            url_patterns = ["[class*='url']", "[href*='http']:not([href*='green-japan'])"]
            all_links = await page.eval_on_selector_all(
                "a[href]",
                "els => els.map(e => e.href)"
            )
            for link in all_links:
                parsed = urlparse(link)
                if (parsed.netloc and "green-japan.com" not in parsed.netloc
                        and parsed.scheme in ("http", "https")
                        and not any(s in parsed.netloc for s in ["twitter", "facebook", "instagram", "linkedin", "youtube"])):
                    official_url = f"{parsed.scheme}://{parsed.netloc}"
                    break

            # 業種
            industry = "IT"
            industry_els = await page.query_selector_all("[class*='industry'], [class*='tag'], [class*='category']")
            if industry_els:
                industry = (await industry_els[0].inner_text()).strip()[:30]

            if company_name and official_url:
                results.append({
                    "company_name": company_name,
                    "url": official_url,
                    "industry": industry or "IT",
                    "source": "green",
                })

            if (i + 1) % 10 == 0:
                print(f"  [Green] {i+1}/{len(collected)} 処理済")

            await page.wait_for_timeout(800)

        except Exception as e:
            continue

    await context.close()
    print(f"  [Green] 完了: {len(results)}社取得")
    return results


async def scrape_kyujinbox(browser, keyword: str = "IT 中小企業", pages: int = 5) -> list[dict]:
    """
    求人ボックスから企業リストを取得（幅広い業種対応）
    """
    results = []
    seen = set()
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        locale="ja-JP",
    )
    page = await context.new_page()

    print(f"  [求人ボックス] キーワード「{keyword}」でスクレイピング開始...")
    encoded_kw = quote(keyword)

    for p in range(1, pages + 1):
        try:
            url = f"https://求人ボックス.com/jobs/search?q={encoded_kw}&page={p}"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # 企業名・URLを取得
            items = await page.eval_on_selector_all(
                "[class*='company'], [class*='corp']",
                "els => els.map(e => ({text: e.textContent.trim(), href: e.querySelector('a') ? e.querySelector('a').href : ''}))"
            )

            for item in items:
                text = item.get("text", "").strip()[:100]
                href = item.get("href", "")
                if text and href and href not in seen:
                    seen.add(href)
                    results.append({
                        "company_name": text,
                        "url": normalize_url(href) if href.startswith("http") else "",
                        "industry": keyword,
                        "source": "kyujinbox",
                    })

            print(f"  [求人ボックス] page {p}: 累計 {len(results)} 社")
            await page.wait_for_timeout(1500)

        except Exception as e:
            print(f"  [求人ボックス] page {p} エラー: {e}")
            continue

    await context.close()
    results = [r for r in results if r.get("url")]
    print(f"  [求人ボックス] 完了: {len(results)}社取得")
    return results


def deduplicate(companies: list[dict]) -> list[dict]:
    """URLベースで重複排除"""
    seen = set()
    unique = []
    for c in companies:
        url = normalize_url(c.get("url", ""))
        if url and url not in seen:
            seen.add(url)
            unique.append(c)
    return unique


def save_csv(companies: list[dict], tag: str = "") -> str:
    """企業リストをCSVに保存"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag_str = f"_{tag}" if tag else ""
    path = os.path.join(OUTPUT_DIR, f"generated{tag_str}_{timestamp}.csv")

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["company_name", "url", "industry"])
        writer.writeheader()
        for c in companies:
            writer.writerow({
                "company_name": c.get("company_name", ""),
                "url": c.get("url", ""),
                "industry": c.get("industry", ""),
            })

    return path


async def run_generator(source: str, pages: int = 5, keyword: str = ""):
    """メイン実行"""
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    )

    all_companies = []

    try:
        if source in ("wantedly", "all"):
            companies = await scrape_wantedly(browser, pages=pages)
            all_companies.extend(companies)

        if source in ("green", "all"):
            companies = await scrape_green(browser, pages=pages)
            all_companies.extend(companies)

        if source in ("kyujinbox", "all"):
            kw = keyword or "中小企業 IT"
            companies = await scrape_kyujinbox(browser, keyword=kw, pages=pages)
            all_companies.extend(companies)

    finally:
        await browser.close()
        await pw.stop()

    # 重複排除
    unique = deduplicate(all_companies)
    print(f"\n重複排除後: {len(unique)}社（元: {len(all_companies)}社）")

    if not unique:
        print("取得できた企業がありません。")
        return None

    # CSV保存
    path = save_csv(unique, tag=source)
    print(f"CSV保存完了: {path}")
    return path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="企業リスト自動生成")
    parser.add_argument("source", choices=["wantedly", "green", "kyujinbox", "all"], help="取得元")
    parser.add_argument("--pages", type=int, default=5, help="取得ページ数（デフォルト: 5）")
    parser.add_argument("--keyword", type=str, default="", help="求人ボックス用キーワード")
    parser.add_argument("--import", dest="do_import", action="store_true", help="生成後に自動インポート")
    parser.add_argument("--template", type=str, default="default", help="インポート時のテンプレート")
    args = parser.parse_args()

    path = asyncio.run(run_generator(args.source, pages=args.pages, keyword=args.keyword))

    if path and args.do_import:
        import database as db
        db.init_db()
        count = db.import_leads_from_csv(path, template_name=args.template)
        print(f"\nDBインポート完了: {count}件追加")
        stats = db.get_stats()
        print(f"pending合計: {stats['pending']}件")
