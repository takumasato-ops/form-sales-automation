"""
フォーム自動入力・送信エンジン（v2 安定性改善版）
- AIマッピング + ラベルベース直接検出のハイブリッド方式
- Cookie同意ダイアログの自動処理
- select/ラジオボタンの自動選択
- メール確認フィールド対応
- ステップ形式フォーム対応
- 確認画面の検出と対応
- 送信前後のスクリーンショット保存
"""

import asyncio
import os
import re
from datetime import datetime
from playwright.async_api import Page
import yaml


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ラベルテキストから入力値の種別を判定するルール
LABEL_TO_FILL = [
    (r"会社名|企業名|法人名|組織名|貴社名", "sender_company"),
    (r"^姓$|お名前.*姓|氏名.*姓|苗字|last.?name", "sender_name_sei"),
    (r"^名$|お名前.*名$|氏名.*名$|名前.*名$|first.?name", "sender_name_mei"),
    (r"お名前|ご氏名|氏名|担当者名|ご担当|名前", "sender_name"),
    (r"メール.*確認|確認.*メール|email.*confirm|mail2|再入力", "sender_email_confirm"),
    (r"メール|email|e-mail|Eメール", "sender_email"),
    (r"電話|TEL|tel|Phone|携帯", "sender_phone"),
    (r"役職|肩書", "sender_title"),
    (r"件名|タイトル|subject|題名", "subject"),
    (r"内容|本文|メッセージ|備考|詳細|body|message|ご用件|お問い合わせ内容|ご相談|ご依頼", "message"),
]


def resolve_fill_value(fill_with: str, sender: dict, subject: str, message: str) -> str:
    """fill_withの種別に応じて実際の入力値を決定"""
    sei = sender.get("name_sei", "")
    mei = sender.get("name_mei", "")
    full_name = f"{sei} {mei}".strip() if sei and mei else sender.get("name", "")
    mapping = {
        "sender_company": sender.get("company", ""),
        "sender_name": full_name,
        "sender_name_sei": sei or sender.get("name", ""),
        "sender_name_mei": mei,
        "sender_email": sender.get("email", ""),
        "sender_email_confirm": sender.get("email", ""),
        "sender_phone": sender.get("phone", ""),
        "sender_title": sender.get("title", ""),
        "subject": subject,
        "message": message,
    }
    return mapping.get(fill_with, "")


def guess_fill_type_from_label(label_text: str, name_attr: str = "") -> str:
    """ラベルテキストとname属性からフィールド種別を推定"""
    # name属性によるメール確認の特別判定
    if name_attr and re.search(r"mail2|mail_conf|email_confirm|email_check", name_attr, re.IGNORECASE):
        return "sender_email_confirm"

    label_text = label_text.strip()
    for pattern, fill_type in LABEL_TO_FILL:
        if re.search(pattern, label_text, re.IGNORECASE):
            return fill_type
    return ""


async def dismiss_cookie_dialog(page: Page):
    """Cookie同意ダイアログを閉じる"""
    cookie_selectors = [
        '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
        '#CybotCookiebotDialogBodyButtonDecline',
        'button:has-text("すべて同意")',
        'button:has-text("同意する")',
        'button:has-text("Accept All")',
        'button:has-text("Accept")',
        '[data-consent="accept"]',
        '.cookie-accept',
        '#cookie-accept',
    ]
    for sel in cookie_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=800):
                await btn.click(timeout=3000)
                await page.wait_for_timeout(500)
                return
        except:
            continue


async def detect_and_fill_fields(page: Page, sender: dict, subject: str, message: str) -> list[str]:
    """ページ内のフォームフィールドをラベルベースで検出し、直接入力する。"""
    errors = []

    fields = await page.evaluate("""() => {
        const results = [];
        const inputs = document.querySelectorAll('input, textarea, select');

        for (const input of inputs) {
            const type = input.tagName.toLowerCase() === 'textarea' ? 'textarea' :
                         input.tagName.toLowerCase() === 'select' ? 'select' :
                         (input.type || 'text');

            if (['hidden', 'submit', 'button', 'image', 'file', 'reset'].includes(type)) continue;

            let label = '';
            let selector = '';

            if (input.id) {
                const labelEl = document.querySelector(`label[for="${input.id}"]`);
                if (labelEl) label = labelEl.textContent.trim();
                selector = '#' + input.id;
            }

            if (!selector && input.name) {
                selector = `[name="${input.name}"]`;
            }

            if (!label) {
                const parentLabel = input.closest('label');
                if (parentLabel) label = parentLabel.textContent.trim();
            }

            if (!label && input.getAttribute('aria-label')) {
                label = input.getAttribute('aria-label');
            }

            if (!label && input.placeholder) {
                label = input.placeholder;
            }

            if (!label) {
                const prev = input.previousElementSibling;
                if (prev && ['LABEL', 'SPAN', 'P', 'DT', 'TH', 'DIV'].includes(prev.tagName)) {
                    label = prev.textContent.trim();
                }
            }

            if (!label) {
                const parent = input.closest('div, li, tr, dd, fieldset, dl');
                if (parent) {
                    const labelEl = parent.querySelector('label, span.label, th, dt, p, h3, h4');
                    if (labelEl) label = labelEl.textContent.trim();
                }
            }

            if (selector) {
                results.push({
                    selector: selector,
                    label: label.substring(0, 100),
                    type: type,
                    tagName: input.tagName.toLowerCase(),
                    name: input.name || '',
                    isVisible: input.offsetParent !== null,
                });
            }
        }
        return results;
    }""")

    filled_types = set()
    # メール確認とname_sei/name_meiは複数入力を許可
    allow_duplicate = {"sender_name_sei", "sender_name_mei", "sender_email_confirm"}

    for field in fields:
        if not field.get("isVisible", False):
            continue

        selector = field["selector"]
        label = field["label"]
        field_type = field["type"]
        name_attr = field.get("name", "")

        fill_type = guess_fill_type_from_label(label, name_attr)
        if not fill_type:
            continue

        if fill_type in filled_types and fill_type not in allow_duplicate:
            continue

        value = resolve_fill_value(fill_type, sender, subject, message)
        if not value:
            continue

        try:
            element = page.locator(selector).first

            if field_type == "select":
                await element.select_option(value=value, timeout=5000)
            elif field_type == "checkbox":
                try:
                    if not await element.is_checked():
                        await element.check(timeout=5000)
                except:
                    await element.click(timeout=5000)
            elif field_type in ("textarea",) or field.get("tagName") == "textarea":
                await element.click(timeout=5000)
                await element.fill(value, timeout=5000)
            else:
                await element.click(timeout=5000)
                await element.fill(value, timeout=5000)

            filled_types.add(fill_type)
            await page.wait_for_timeout(200)

        except Exception as e:
            errors.append(f"[{label}:{selector}] {str(e)[:80]}")

    return errors


async def fill_form_with_ai(page: Page, field_mappings: list[dict], sender: dict, subject: str, message: str) -> list[str]:
    """AIマッピングベースの入力（フォールバック用）"""
    errors = []
    for field in field_mappings:
        selector = field.get("selector", "")
        field_type = field.get("type", "input")
        fill_with = field.get("fill_with", "other")
        static_value = field.get("value", "")

        value = resolve_fill_value(fill_with, sender, subject, message) or static_value
        if not selector or not value:
            continue

        try:
            element = page.locator(selector).first
            is_visible = await element.is_visible(timeout=2000)
            if not is_visible:
                continue

            if field_type == "select":
                await element.select_option(value=value, timeout=5000)
            elif field_type == "checkbox":
                try:
                    if not await element.is_checked():
                        await element.check(timeout=5000)
                except:
                    try:
                        await element.click(timeout=5000)
                    except:
                        pass
            elif field_type == "textarea":
                await element.click(timeout=5000)
                await element.fill(value, timeout=5000)
            else:
                await element.click(timeout=5000)
                await element.fill(value, timeout=5000)

            await page.wait_for_timeout(200)

        except Exception as e:
            errors.append(f"[{selector}] {str(e)[:80]}")

    return errors


async def fill_required_selects(page: Page):
    """必須selectで未選択のものに「その他」またはデフォルト値を設定"""
    await page.evaluate("""() => {
        document.querySelectorAll('select').forEach(sel => {
            if (sel.offsetParent === null) return;
            if (sel.selectedIndex <= 0 && sel.options.length > 1) {
                // 「その他」があればそれを選択
                for (let i = 0; i < sel.options.length; i++) {
                    if (sel.options[i].text.includes('その他')) {
                        sel.selectedIndex = i;
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        return;
                    }
                }
                // なければ2番目を選択（1番目はプレースホルダー）
                sel.selectedIndex = 1;
                sel.dispatchEvent(new Event('change', {bubbles: true}));
            }
        });
    }""")


async def fill_required_radios(page: Page):
    """必須ラジオボタンで未選択のグループに「その他」またはデフォルト値を設定"""
    await page.evaluate("""() => {
        const groups = {};
        document.querySelectorAll('input[type="radio"]').forEach(r => {
            if (r.name && r.offsetParent !== null) {
                if (!groups[r.name]) groups[r.name] = [];
                groups[r.name].push(r);
            }
        });
        for (const [name, radios] of Object.entries(groups)) {
            const checked = radios.some(r => r.checked);
            if (!checked && radios.length > 0) {
                // 「その他」を優先
                const other = radios.find(r => {
                    const label = r.closest('label')?.textContent || '';
                    const next = r.nextElementSibling;
                    const nextText = next ? next.textContent : '';
                    return (label + nextText).includes('その他');
                });
                const target = other || radios[radios.length - 1];
                target.checked = true;
                target.dispatchEvent(new Event('change', {bubbles: true}));
                target.dispatchEvent(new Event('click', {bubbles: true}));
            }
        }
    }""")


async def handle_privacy_checkbox(page: Page):
    """プライバシーポリシー同意チェックボックスを処理（強化版）"""
    privacy_selectors = [
        'input[type="checkbox"][name*="privacy"]',
        'input[type="checkbox"][name*="agree"]',
        'input[type="checkbox"][name*="consent"]',
        'input[type="checkbox"][name*="personal_info"]',
        'input[type="checkbox"][name*="privacypolicy"]',
        'input[type="checkbox"][name*="policy"]',
        'input[type="checkbox"][name*="terms"]',
        'input[type="checkbox"][id*="privacy"]',
        'input[type="checkbox"][id*="agree"]',
        'input[type="checkbox"][id*="consent"]',
        'input[type="checkbox"][class*="acceptance"]',
    ]

    for sel in privacy_selectors:
        try:
            cb = page.locator(sel).first
            if await cb.is_visible(timeout=800):
                if not await cb.is_checked():
                    await cb.check(timeout=3000)
                return
        except:
            continue

    # テキストベースで「同意」「プライバシー」付近のチェックボックスを探す
    try:
        checkboxes = page.locator('input[type="checkbox"]')
        count = await checkboxes.count()
        for i in range(min(count, 10)):
            cb = checkboxes.nth(i)
            if not await cb.is_visible(timeout=500):
                continue
            try:
                parent = cb.locator('..')
                parent_text = await parent.inner_text(timeout=1000)
                if any(kw in parent_text for kw in ["プライバシー", "同意", "個人情報", "利用規約", "承諾"]):
                    if not await cb.is_checked():
                        await cb.check(timeout=3000)
                    return
            except:
                continue
    except:
        pass


async def click_submit(page: Page, ai_submit_selector: str = "") -> bool:
    """送信ボタンをクリック（v2: form内限定 + aタグ/divタグ対応）"""

    # 1. Cookie同意ダイアログを先に閉じる
    await dismiss_cookie_dialog(page)

    # 2. AIが指定したセレクタを最優先
    if ai_submit_selector:
        try:
            btn = page.locator(ai_submit_selector).first
            if await btn.is_visible(timeout=1500):
                await btn.scroll_into_view_if_needed(timeout=3000)
                await btn.click(timeout=5000)
                return True
        except:
            pass

    # 3. formタグ内に限定してボタンを探す（Cookie同意ボタン等の誤検出を防ぐ）
    form_scoped = [
        'form button[type="submit"]',
        'form input[type="submit"]',
        'form button:has-text("送信")',
        'form button:has-text("送信する")',
        'form input[value*="送信"]',
        'form input[value*="確認"]',
        'form button:has-text("確認")',
        'form button:has-text("確認する")',
        'form button:has-text("確認画面")',
        'form button:has-text("同意して送信")',
        'form button:has-text("プライバシーポリシーに同意して送信")',
    ]

    for sel in form_scoped:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.scroll_into_view_if_needed(timeout=3000)
                await btn.click(timeout=5000)
                return True
        except:
            continue

    # 4. aタグの送信ボタン（neo-career等）
    a_tag = [
        'a#submitButton',
        'a#submit',
        'form a:has-text("送信する")',
        'form a:has-text("送信")',
        'form a:has-text("同意して送信")',
        'form a:has-text("内容を送信")',
        'a:has-text("送信する")',
        'a:has-text("同意して送信")',
    ]

    for sel in a_tag:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.scroll_into_view_if_needed(timeout=3000)
                await btn.click(timeout=5000)
                return True
        except:
            continue

    # 5. div/spanタグのボタン（leverages等のステップフォーム）
    div_button = [
        'div[role="button"]:has-text("送信")',
        'div[role="button"]:has-text("SEND")',
        'div:has-text("NEXT STEP") >> visible=true',
        'div.js-confirm',
    ]

    for sel in div_button:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.scroll_into_view_if_needed(timeout=3000)
                await btn.click(timeout=5000)
                return True
        except:
            continue

    # 6. 最後のフォールバック（formスコープなし）
    fallback = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("送信")',
        'button:has-text("Submit")',
        '#submit',
        '.submit-btn',
        '.btn-submit',
    ]

    for sel in fallback:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.scroll_into_view_if_needed(timeout=3000)
                await btn.click(timeout=5000)
                return True
        except:
            continue

    return False


async def handle_step_form(page: Page) -> bool:
    """ステップ形式フォームのNEXT/次へボタンを検出してクリック"""
    step_selectors = [
        'div:has-text("NEXT STEP") >> visible=true',
        'a:has-text("NEXT STEP")',
        'button:has-text("次へ")',
        'button:has-text("NEXT")',
        'a:has-text("次のステップ")',
        '.js-confirm',
    ]
    for sel in step_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1500):
                await btn.click(timeout=5000)
                await page.wait_for_timeout(2000)
                return True
        except:
            continue
    return False


async def fill_form(
    page: Page,
    field_mappings: list[dict],
    submit_selector: str,
    sender: dict,
    subject: str,
    message: str,
) -> dict:
    """
    ハイブリッド方式でフォームを入力・送信する（v2）
    1. Cookie同意ダイアログを閉じる
    2. ラベルベースの直接検出で入力
    3. AIマッピングで補完
    4. 未選択のselect/ラジオボタンを自動選択
    5. プライバシーチェックボックス処理
    6. 送信ボタンクリック
    """
    config = load_config()
    result = {"success": False, "error": "", "screenshot_path": ""}
    logs_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    try:
        # Phase 0: Cookie同意ダイアログを閉じる
        await dismiss_cookie_dialog(page)

        # Phase 1: ラベルベースの直接検出で入力
        errors1 = await detect_and_fill_fields(page, sender, subject, message)

        # Phase 2: AIマッピングで補完入力
        errors2 = await fill_form_with_ai(page, field_mappings, sender, subject, message)

        # Phase 3: 未選択のselect/ラジオボタンを自動選択
        await fill_required_selects(page)
        await fill_required_radios(page)

        # Phase 4: プライバシーポリシー同意チェックボックス
        await handle_privacy_checkbox(page)

        await page.wait_for_timeout(500)

        if errors1 or errors2:
            result["error"] += "; ".join(errors1 + errors2)[:200] + "; "

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Phase 5: 送信ボタンをクリック
        submit_clicked = await click_submit(page, submit_selector)

        if not submit_clicked:
            result["error"] += "送信ボタンが見つかりません; "
            return result

        await page.wait_for_timeout(3000)

        # ステップフォーム対応: NEXT STEPの後に再度送信ボタンがあるか確認
        page_text = await page.inner_text("body")
        if "STEP" in page_text or "ステップ" in page_text:
            await handle_step_form(page)
            await page.wait_for_timeout(2000)

        # 確認画面の検出と対応
        await handle_confirm_page(page)

        # 送信成功の判定
        page_text = await page.inner_text("body")
        error_indicators = ["入力内容に誤り", "必須項目です", "正しく入力", "入力してください", "必須項目が", "is required"]
        success_indicators = ["送信完了", "ありがとうございます", "送信しました", "受け付けました", "thank you", "送信が完了", "Thank you"]

        has_error = any(ind in page_text for ind in error_indicators)
        has_success = any(ind in page_text for ind in success_indicators)

        if has_success:
            result["success"] = True
        elif has_error:
            result["error"] += "フォームにエラー表示を検出"
        else:
            result["success"] = True
            result["error"] += "(成功判定は推定)"

    except Exception as e:
        result["error"] = str(e)
        if config["browser"]["screenshot_on_error"]:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                ss_err = os.path.join(logs_dir, f"error_{timestamp}.png")
                await page.screenshot(path=ss_err, full_page=True)
                result["screenshot_path"] = ss_err
            except:
                pass

    return result


async def handle_confirm_page(page: Page) -> bool:
    """確認画面を検出し、送信ボタンがあればクリックする。"""
    page_text = await page.inner_text("body")

    confirm_indicators = [
        "確認", "内容をご確認", "以下の内容で送信", "よろしければ送信",
        "入力内容の確認", "この内容で送信",
    ]

    is_confirm_page = any(ind in page_text for ind in confirm_indicators)
    if not is_confirm_page:
        return False

    confirm_buttons = [
        'form button:has-text("送信")',
        'form button:has-text("送信する")',
        'form button:has-text("この内容で送信")',
        'form input[type="submit"][value*="送信"]',
        'button:has-text("送信")',
        'button:has-text("送信する")',
        'input[type="submit"][value*="送信"]',
        'a:has-text("送信")',
        'button:has-text("同意して送信")',
    ]

    for selector in confirm_buttons:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=2000):
                await btn.click(timeout=5000)
                await page.wait_for_timeout(3000)
                return True
        except:
            continue

    return False
