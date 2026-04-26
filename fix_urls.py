"""
eventos_booth リードの偽URL（eventos://）を実際の公式サイトURLに補完するスクリプト。
DuckDuckGoで会社名を検索し、最初の結果URLをDBに書き込む。

使い方:
  python3 fix_urls.py            # 全484件を処理
  python3 fix_urls.py --limit 50 # 先頭50件だけ処理
  python3 fix_urls.py --dry-run  # DBを変更せず確認のみ
"""

import asyncio
import sqlite3
import re
import sys
import time
import os
from urllib.parse import quote_plus, urlparse

# Playwright
from playwright.async_api import async_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), "data/form_sales.db")

# 明らかに公式サイトでないドメインを除外
SKIP_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "instagram.com",
    "youtube.com", "wikipedia.org", "wantedly.com", "indeed.com", "recruit.co.jp",
    "mynavi.jp", "rikunabi.com", "doda.jp", "jobtalk.jp", "careermark.net",
    "tabelog.com", "hotpepper.jp", "rakuten.co.jp", "amazon.co.jp",
    "google.com", "google.co.jp", "bing.com", "yahoo.co.jp",
    "prtimes.jp", "newsrelease", "news.", "press.",
    "openwork.jp", "vorkers.com", "kaishahyouban",
}


def is_valid_corporate_url(url: str, company_name: str) -> bool:
    """公式サイトらしいURLかどうかを判定"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        # スキップドメインチェック
        for skip in SKIP_DOMAINS:
            if skip in domain:
                return False
        # httpから始まること
        if not url.startswith("http"):
            return False
        return True
    except Exception:
        return False


async def search_company_url(page, company_name: str) -> str:
    """Yahoo Japan検索で会社名を検索して公式URLを返す"""
    query = quote_plus(company_name)
    search_url = f"https://search.yahoo.co.jp/search?p={query}"

    try:
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(1500)

        links = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(e => e.href).filter(h =>
                h.startsWith('http') &&
                !h.includes('yahoo') && !h.includes('google') &&
                !h.includes('bing') && !h.includes('microsoft')
            )"""
        )

        for url in links[:15]:
            if is_valid_corporate_url(url, company_name):
                parsed = urlparse(url)
                top_url = f"{parsed.scheme}://{parsed.netloc}/"
                return top_url

    except Exception:
        pass

    return ""


async def main():
    dry_run = "--dry-run" in sys.argv
    limit = None
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, company_name FROM leads WHERE url LIKE 'eventos://%' AND status = 'pending' ORDER BY id"
    ).fetchall()

    if limit:
        rows = rows[:limit]

    total = len(rows)
    print(f"対象: {total}件 {'(dry-run)' if dry_run else ''}")

    found = 0
    not_found = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja-JP,ja;q=0.9"},
        )
        page = await context.new_page()

        for i, (lead_id, company_name) in enumerate(rows, 1):
            print(f"[{i}/{total}] {company_name} ...", end=" ", flush=True)

            url = await search_company_url(page, company_name)

            if url:
                print(f"→ {url}")
                if not dry_run:
                    # 同じURLが既存のレコードに使われていた場合は末尾に#idを付けてユニーク化
                    existing = conn.execute("SELECT id FROM leads WHERE url = ? AND id != ?", (url, lead_id)).fetchone()
                    if existing:
                        url = f"{url}#{lead_id}"
                    conn.execute("UPDATE leads SET url = ? WHERE id = ?", (url, lead_id))
                    if i % 10 == 0:
                        conn.commit()
                found += 1
            else:
                print("→ 見つからず（スキップのまま）")
                not_found += 1

            # 過負荷防止
            await asyncio.sleep(1.5)

        await browser.close()

    if not dry_run:
        conn.commit()
    conn.close()

    print(f"\n完了: URL取得成功 {found}件 / 取得失敗 {not_found}件")
    if not dry_run:
        print(f"DBを更新しました。次回のcron実行から自動送信されます。")


if __name__ == "__main__":
    asyncio.run(main())
