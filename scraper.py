"""
企業Webサイトのスクレイピング
- メインページのテキスト抽出
- 営業お断り表記の検出
- 問い合わせフォームURLの自動検出
"""

import asyncio
from playwright.async_api import async_playwright, Page, Browser
from bs4 import BeautifulSoup
import yaml
import os
import re
from urllib.parse import urljoin


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
]


async def create_browser() -> tuple:
    """Playwrightブラウザを起動"""
    import random
    config = load_config()
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=config["browser"]["headless"],
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="ja-JP",
        ignore_https_errors=True,
        extra_http_headers={"Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8"},
    )
    return pw, browser, context


async def extract_page_text(page: Page) -> str:
    """ページ本文のテキストを抽出"""
    content = await page.content()
    soup = BeautifulSoup(content, "html.parser")
    # 不要要素を除去
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # 空行を圧縮
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    return "\n".join(lines[:200])  # 最大200行


async def check_sales_refusal(page: Page, config: dict) -> tuple[bool, str]:
    """営業お断り表記がないかチェック"""
    content = await page.content()
    text = BeautifulSoup(content, "html.parser").get_text()

    for keyword in config["compliance"]["refusal_keywords"]:
        if keyword in text:
            return True, keyword
    return False, ""


async def find_contact_form_url(page: Page, base_url: str) -> str:
    """問い合わせフォームのURLを探す"""
    # よくあるパターンのリンクを探す
    contact_patterns = [
        r"お問い合わせ",
        r"お問合せ",
        r"問い合わせ",
        r"contact",
        r"inquiry",
        r"お問合わせ",
    ]

    links = await page.eval_on_selector_all(
        "a[href]",
        """elements => elements.map(e => ({
            href: e.href,
            text: e.textContent.trim(),
        }))"""
    )

    # テキストマッチで探す
    for link in links:
        for pattern in contact_patterns:
            if re.search(pattern, link["text"], re.IGNORECASE) or re.search(pattern, link["href"], re.IGNORECASE):
                return link["href"]

    # URLパスで探す
    url_patterns = ["/contact", "/inquiry", "/form", "/toiawase", "/otoiawase"]
    for link in links:
        for pattern in url_patterns:
            if pattern in link["href"].lower():
                return link["href"]

    return ""


async def find_and_scrape_company_profile(page: Page, base_url: str) -> str:
    """
    「会社概要」ページを探してテキストを追加取得する（従業員数・資本金の精度向上）
    """
    profile_patterns = [
        r"会社概要", r"企業概要", r"会社情報", r"企業情報",
        r"about", r"company", r"corporate",
    ]
    try:
        links = await page.eval_on_selector_all(
            "a[href]",
            "elements => elements.map(e => ({href: e.href, text: e.textContent.trim()}))"
        )
        for link in links:
            for pattern in profile_patterns:
                if re.search(pattern, link["text"], re.IGNORECASE) or re.search(pattern, link["href"], re.IGNORECASE):
                    try:
                        profile_page = await page.context.new_page()
                        await profile_page.goto(link["href"], wait_until="domcontentloaded", timeout=15000)
                        await profile_page.wait_for_timeout(1000)
                        text = await extract_page_text(profile_page)
                        await profile_page.close()
                        return text
                    except:
                        pass
    except:
        pass
    return ""


async def extract_form_html(page: Page) -> str:
    """ページ内のフォームHTMLを抽出"""
    forms = await page.query_selector_all("form")

    if forms:
        # 最も入力フィールドが多いフォームを選ぶ
        best_form = None
        max_inputs = 0
        for form in forms:
            inputs = await form.query_selector_all("input, textarea, select")
            if len(inputs) > max_inputs:
                max_inputs = len(inputs)
                best_form = form
        if best_form and max_inputs >= 2:
            return await best_form.inner_html()

    # formタグがなくても入力フィールドがある場合（SPA等）
    inputs = await page.query_selector_all("input, textarea, select")
    if len(inputs) >= 3:
        # ページ全体のbody内HTMLからフォーム部分を抽出
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        # 入力フィールドを含む最も小さい親要素を探す
        for parent_tag in ["form", "div", "section", "main"]:
            containers = soup.find_all(parent_tag)
            for container in containers:
                field_count = len(container.find_all(["input", "textarea", "select"]))
                if field_count >= 3:
                    return str(container)

    return ""


async def find_form_in_sublinks(page: Page, config: dict) -> tuple[str, str]:
    """
    問い合わせページにフォームがない場合、サブリンクを探索する。
    戻り値: (form_url, form_html)
    """
    timeout = config["browser"]["timeout_sec"] * 1000

    # 問い合わせ系のサブリンクを探す（強化版）
    sub_patterns = [
        r"一般.*問い合わせ", r"その他.*問い合わせ", r"サービス.*問い合わせ",
        r"ビジネス", r"企業.*お問", r"法人.*お問", r"パートナー",
        r"同意して.*進む", r"フォームに進む", r"入力.*進む",
        r"上記以外", r"その他のお問い合わせ",
    ]

    links = await page.eval_on_selector_all(
        "a[href]",
        """elements => elements.map(e => ({
            href: e.href,
            text: e.textContent.trim(),
        }))"""
    )

    for link in links:
        for pattern in sub_patterns:
            if re.search(pattern, link["text"]):
                try:
                    await page.goto(link["href"], wait_until="domcontentloaded", timeout=timeout)
                    await page.wait_for_timeout(2000)
                    form_html = await extract_form_html(page)
                    if form_html:
                        return link["href"], form_html
                except:
                    continue

    # URLパスにform/entry/inputを含むリンクへの遷移
    url_sub_patterns = ["form.php", "entry.php", "input.php", "/form/", "/entry/", "/input/"]
    for link in links:
        for pattern in url_sub_patterns:
            if pattern in link["href"].lower():
                try:
                    await page.goto(link["href"], wait_until="domcontentloaded", timeout=timeout)
                    await page.wait_for_timeout(2000)
                    form_html = await extract_form_html(page)
                    if form_html:
                        return link["href"], form_html
                except:
                    continue

    return "", ""


RETRYABLE_ERRORS = [
    "ERR_HTTP2_PROTOCOL_ERROR",
    "ERR_CONNECTION_RESET",
    "ERR_TIMED_OUT",
    "ERR_INTERNET_DISCONNECTED",
    "ERR_NAME_NOT_RESOLVED",
    "ERR_CONNECTION_REFUSED",
    "Timeout",
    "timeout",
]

MAX_RETRIES = 3
RETRY_DELAY_SEC = 5


async def scrape_company(url: str, context) -> dict:
    """
    企業サイトをスクレイピングして情報を収集する。
    リトライ機能付き（ネットワークエラー時に最大3回再試行）
    """
    config = load_config()
    result = {
        "page_text": "",
        "has_refusal": False,
        "refusal_keyword": "",
        "contact_form_url": "",
        "form_html": "",
        "error": "",
    }

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        page = await context.new_page()
        try:
            timeout = config["browser"]["timeout_sec"] * 1000
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await page.wait_for_timeout(2000)

            main_text = await extract_page_text(page)
            # 会社概要ページも取得して従業員数・資本金の精度向上
            profile_text = await find_and_scrape_company_profile(page, url)
            result["page_text"] = main_text + ("\n\n【会社概要】\n" + profile_text if profile_text else "")

            has_refusal, keyword = await check_sales_refusal(page, config)
            result["has_refusal"] = has_refusal
            result["refusal_keyword"] = keyword
            if has_refusal:
                return result

            contact_url = await find_contact_form_url(page, url)
            result["contact_form_url"] = contact_url

            if contact_url:
                await page.goto(contact_url, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_timeout(2000)

                has_refusal, keyword = await check_sales_refusal(page, config)
                if has_refusal:
                    result["has_refusal"] = True
                    result["refusal_keyword"] = keyword
                    return result

                result["form_html"] = await extract_form_html(page)

                if not result["form_html"]:
                    sub_url, sub_html = await find_form_in_sublinks(page, config)
                    if sub_html:
                        result["contact_form_url"] = sub_url
                        result["form_html"] = sub_html

            result["error"] = ""
            return result  # 成功

        except Exception as e:
            last_error = str(e)
            is_retryable = any(err in last_error for err in RETRYABLE_ERRORS)
            if is_retryable and attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_SEC * attempt)
                continue
            else:
                result["error"] = f"スクレイピングエラー: {last_error}"
                return result
        finally:
            await page.close()

    result["error"] = f"スクレイピングエラー（{MAX_RETRIES}回失敗）: {last_error}"
    return result
