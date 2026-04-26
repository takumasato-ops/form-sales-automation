"""
Yahoo Japan検索ベースのリード自動発掘スクリプト（VPS対応・ログイン不要）

業種キーワード × 都道府県 の組み合わせで企業サイトを検索し、
問い合わせフォームがありそうな企業をDBに追加する。

使い方:
  python3 lead_hunter.py                  # デフォルト設定で実行
  python3 lead_hunter.py --target 200     # 200件追加するまで実行
  python3 lead_hunter.py --dry-run        # DB更新せず確認のみ
"""

import asyncio
import sqlite3
import re
import sys
import os
from urllib.parse import quote_plus, urlparse

from playwright.async_api import async_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), "data/form_sales.db")

# 検索キーワード（業種 × 地域の組み合わせで展開）
INDUSTRIES = [
    "製造業", "建設会社", "不動産会社", "物流会社", "運送会社",
    "食品メーカー", "印刷会社", "医療機器メーカー", "化学メーカー",
    "卸売業", "小売業", "サービス業", "IT企業", "システム開発",
    "人材派遣", "広告代理店", "コンサルティング", "会計事務所",
    "社会保険労務士", "設計事務所", "商社", "金属加工", "機械製造",
]

PREFECTURES = [
    "大阪", "東京", "名古屋", "福岡", "札幌", "仙台",
    "広島", "神戸", "京都", "横浜", "川崎", "埼玉",
    "千葉", "静岡", "岡山", "熊本", "鹿児島",
]

SKIP_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "instagram.com",
    "youtube.com", "wikipedia.org", "wantedly.com", "indeed.com",
    "mynavi.jp", "rikunabi.com", "doda.jp", "tabelog.com", "rakuten.co.jp",
    "amazon.co.jp", "google.com", "google.co.jp", "bing.com", "yahoo.co.jp",
    "prtimes.jp", "openwork.jp", "jobmedley.com", "en-gage.net",
    "recruit.co.jp", "type.jp", "townnews.co.jp",
}


def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        if not url.startswith("http"):
            return False
        for skip in SKIP_DOMAINS:
            if skip in domain:
                return False
        return True
    except Exception:
        return False


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"


async def search_companies(page, query: str, existing_urls: set) -> list[dict]:
    """Yahoo Japan検索で企業URLを取得"""
    results = []
    try:
        q = quote_plus(query + " 会社 お問い合わせ")
        search_url = f"https://search.yahoo.co.jp/search?p={q}&n=20"
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(1200)

        links = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(e => e.href).filter(h =>
                h.startsWith('http') &&
                !h.includes('yahoo') && !h.includes('google') && !h.includes('bing')
            )"""
        )

        seen_in_query = set()
        for url in links[:30]:
            if not is_valid_url(url):
                continue
            top_url = normalize_url(url)
            if top_url in existing_urls or top_url in seen_in_query:
                continue
            seen_in_query.add(top_url)

            # 会社名をドメインから推定（後でメインページから取得）
            domain = urlparse(top_url).netloc.replace("www.", "")
            results.append({
                "company_name": domain,  # 仮置き。実際はスクレイプ時に取得
                "url": top_url,
                "industry": query,
                "source_file": "lead_hunter",
                "template_name": "default",
            })

    except Exception as e:
        pass

    return results


async def fetch_company_name(page, url: str) -> str:
    """企業サイトのトップページからOGPタイトルや<title>を取得して会社名を推定"""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(800)

        # OGPのsite_name
        og_name = await page.eval_on_selector(
            'meta[property="og:site_name"]',
            'el => el.getAttribute("content")',
        )
        if og_name and len(og_name) < 40:
            return og_name.strip()

        # <title>タグ
        title = await page.title()
        if title:
            # 「| 株式会社〇〇」のようなパターンを抽出
            for sep in ["|", "｜", "-", "－", "–", "/"]:
                parts = title.split(sep)
                for part in parts:
                    p = part.strip()
                    if 2 < len(p) < 30 and ("株式会社" in p or "有限会社" in p or "合同会社" in p):
                        return p
            # タイトル全体が短ければそのまま
            if len(title) < 30:
                return title.strip()

    except Exception:
        pass

    return ""


async def main():
    dry_run = "--dry-run" in sys.argv
    target = 500
    for i, arg in enumerate(sys.argv):
        if arg == "--target" and i + 1 < len(sys.argv):
            target = int(sys.argv[i + 1])

    conn = sqlite3.connect(DB_PATH)
    existing_urls = set(row[0] for row in conn.execute("SELECT url FROM leads").fetchall())
    existing_companies = set(row[0] for row in conn.execute("SELECT company_name FROM leads").fetchall())
    print(f"既存リード: {len(existing_urls)}件 / 目標追加: {target}件 {'(dry-run)' if dry_run else ''}")

    added = 0
    queries = [f"{ind} {pref}" for pref in PREFECTURES for ind in INDUSTRIES]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja-JP,ja;q=0.9"},
        )
        search_page = await context.new_page()
        fetch_page = await context.new_page()

        for query in queries:
            if added >= target:
                break

            candidates = await search_companies(search_page, query, existing_urls)

            for c in candidates:
                if added >= target:
                    break

                url = c["url"]
                company_name = await fetch_company_name(fetch_page, url)
                if not company_name:
                    company_name = urlparse(url).netloc.replace("www.", "")

                if company_name in existing_companies:
                    continue

                print(f"[+{added+1}] {company_name} → {url}")

                if not dry_run:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO leads (company_name, url, industry, source_file, template_name) VALUES (?, ?, ?, ?, ?)",
                            (company_name, url, query, "lead_hunter", "default")
                        )
                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            added += 1
                            existing_urls.add(url)
                            existing_companies.add(company_name)
                            if added % 20 == 0:
                                conn.commit()
                    except Exception as e:
                        pass
                else:
                    added += 1
                    existing_urls.add(url)

                await asyncio.sleep(0.8)

            await asyncio.sleep(1.5)

        await browser.close()

    if not dry_run:
        conn.commit()
    conn.close()

    print(f"\n完了: {added}件追加")


if __name__ == "__main__":
    asyncio.run(main())
