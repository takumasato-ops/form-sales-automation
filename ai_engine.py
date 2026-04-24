"""
Gemini API を使ったAIエンジン
- 企業HPの情報から業界・事業を要約
- 営業文面のパーソナライズ
- フォームフィールドの自動マッピング
"""

from google import genai
from google.genai import types
import yaml
import os
import json
import re
from datetime import datetime, timedelta


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_client() -> genai.Client:
    config = load_config()
    return genai.Client(api_key=config["gemini"]["api_key"])


def get_model() -> str:
    return load_config()["gemini"]["model"]


def _parse_json_response(text: str) -> dict:
    """GeminiのレスポンスからJSONを抽出・パースする（堅牢版）"""
    text = text.strip()
    # そのままパースを試みる
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # ```json ... ``` ブロックを抽出
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    # 最後の手段: テキスト内の最初の { ... } を正規表現で抽出
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    # すべて失敗した場合のフォールバック
    return {"industry": "不明", "summary": "情報取得不可", "business_keywords": [], "employee_scale": "不明"}


def summarize_company(company_name: str, page_text: str) -> dict:
    """
    企業HPのテキストから業界・事業内容・規模情報を要約する。
    戻り値: {
        "industry": str,
        "summary": str,
        "business_keywords": list,
        "employee_count": int or None,  # 推定従業員数
        "capital_man": int or None,     # 資本金（万円）
        "listing": str,                 # "prime" / "standard" / "growth" / "unlisted" / "unknown"
    }
    """
    client = get_client()
    prompt = f"""以下は「{company_name}」の企業Webサイトから抽出したテキストです。
この企業の情報を分析し、以下のJSON形式で回答してください。

```json
{{
  "industry": "業界名（例：製造業、IT、不動産、医療など）",
  "summary": "この企業の事業内容を1〜2文で要約（営業文面に挿入するため、簡潔かつ具体的に）",
  "business_keywords": ["キーワード1", "キーワード2", "キーワード3"],
  "employee_count": 従業員数の数値（テキストに記載があれば抽出、なければ null）,
  "capital_man": 資本金の万円単位の数値（例: 1000万円なら1000、1億円なら10000、なければ null）,
  "listing": "prime または standard または growth または unlisted または unknown"
}}
```

listingの判定基準:
- 「東証プライム」「東京証券取引所プライム市場」「プライム市場」→ "prime"
- 「東証スタンダード」「スタンダード市場」→ "standard"
- 「東証グロース」「グロース市場」→ "growth"
- 「非上場」「株式非公開」→ "unlisted"
- 判断できない → "unknown"

重要: JSON以外の文字は出力しないでください。

--- 企業サイトのテキスト ---
{page_text[:4000]}
"""
    response = client.models.generate_content(
        model=get_model(),
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            max_output_tokens=600,
            response_mime_type="application/json",
        ),
    )
    text = response.text.strip()
    result = _parse_json_response(text)
    # デフォルト値補完
    result.setdefault("employee_count", None)
    result.setdefault("capital_man", None)
    result.setdefault("listing", "unknown")
    return result


def check_target_criteria(company_info: dict, config: dict) -> tuple[bool, str]:
    """
    ターゲット条件に合致するか判定する。
    絶対条件: 従業員数が判明していて min_employees 未満の場合のみ除外
    資本金・上場区分は参考情報として扱い、スキップには使わない。
    戻り値: (is_target: bool, reason: str)
    """
    t = config.get("targeting", {})
    if not t:
        return True, ""

    employee = company_info.get("employee_count")
    min_emp = t.get("min_employees", 20)

    # 絶対条件：従業員数が判明していて min_emp 未満の場合のみ除外
    if employee is not None and employee < min_emp:
        return False, f"従業員数 {employee}名（{min_emp}名未満）のため除外"

    # 資本金・上場区分はスキップ条件としない（参考扱い）
    return True, ""


def generate_subject(company_name: str, company_info: dict) -> str:
    """
    会社ごとにパーソナライズされた件名を生成する。
    業界名を活かした複数パターンからランダム選択し、開封率を分散させる。
    """
    import random
    industry = company_info.get("industry", "") or ""

    if industry:
        patterns = [
            f"{industry}向け｜現場工数を月36時間削減した実績事例のご紹介",
            f"{industry}の業務プロセス改善｜AI再構築で工数1/4を実現",
            f"{industry}企業様へ｜業務効率化の他社事例を45分でご説明",
            f"{industry}向け｜社員1名あたり月36h創出した仕組みのご紹介",
        ]
    else:
        patterns = [
            "業務プロセスのAI再構築｜月36時間削減の実績事例をご紹介",
            "現場工数を1/4に削減｜AI業務改善の他社事例45分でご説明",
            "社員1名あたり月36h創出｜業務再構築プログラムのご案内",
        ]

    return random.choice(patterns)


def personalize_message(template: str, company_name: str, company_info: dict, schedule_slots: list[str], booking_url: str = "") -> str:
    """
    テンプレートの変数を企業情報で埋めてパーソナライズされた営業文面を生成する。
    booking_urlが設定されている場合、日程候補の代わりにURLを案内する。
    """
    client = get_client()

    if booking_url:
        slots_text = f"下記URLよりご都合の良い日時をお選びください。\n{booking_url}"
    else:
        slots_text = "\n".join(f"・{slot}" for slot in schedule_slots)

    schedule_instruction = (
        f"2. `【日数指定タグ①】` `【日数指定タグ②】` `【日数指定タグ③】` の3行を削除し、代わりに以下を挿入してください:\n{slots_text}"
        if booking_url else
        f"2. `【日数指定タグ①】` `【日数指定タグ②】` `【日数指定タグ③】` を以下の日程候補で置換してください:\n{slots_text}"
    )

    industry = company_info.get('industry', '') or '不明'
    summary = company_info.get('summary', '') or ''

    prompt = f"""以下の営業メールテンプレートを、企業情報を使ってパーソナライズしてください。

【対象企業】{company_name}
【業界】{industry}
【事業要約】{summary if summary else '（情報なし）'}

【テンプレート】
{template}

【ルール】
1. `#function(【業界・事業要約_01】){{......}}#` を**短い名詞句**に置き換えてください。
   この直後に「で事業拡大を推進されている貴社へ、」が続くため、自然につながる形にしてください。

   **必ず業界・事業内容を反映した具体的なフレーズにしてください。**

   業界別の例：
   - IT・SaaS → 「クラウドサービス事業」「SaaSプロダクト開発」「DX推進支援」
   - 製造業 → 「製造業での生産効率化」「工場のDX推進」「製品品質向上への取り組み」
   - 不動産 → 「不動産事業での顧客対応」「物件管理・仲介業務」
   - 人材・HR → 「人材採用・育成事業」「HR Tech領域での事業拡大」
   - 教育 → 「教育サービスの提供」「人材育成・研修事業」
   - コンサル → 「経営コンサルティング事業」「企業支援・業務改善支援」
   - 物流 → 「物流・サプライチェーン管理」「配送ネットワークの拡大」
   - 医療・介護 → 「医療・ヘルスケアサービス」「介護サービスの品質向上」
   - 飲食・小売 → 「店舗運営・顧客体験向上」「小売事業での売上拡大」
   - 金融 → 「金融サービスの多角化」「資産運用・ファイナンス事業」

   事業要約が不明な場合でも、業界名から推測して具体的なフレーズを作ること。
   汎用的な「日々の業務改善」「事業運営」のような表現は使わないこと。

   NG例：
   - 「貴社が推進されている〜」（直後の文と重複）
   - 会社名や「様」を含む表現
   - 「日々の業務改善」「事業運営」などの汎用フレーズ

{schedule_instruction}
3. それ以外のテンプレート文面は一切変更しないでください。
4. 完成した営業文面だけを出力してください。余計な説明・コメントは不要です。
"""
    response = client.models.generate_content(
        model=get_model(),
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.3,
            max_output_tokens=2000,
        ),
    )
    return response.text.strip()


def analyze_form_fields(form_html: str) -> dict:
    """
    フォームのHTMLを解析し、各フィールドに何を入力すべきかをマッピングする。
    戻り値: {"field_mappings": [{"selector": "...", "type": "...", "fill_with": "...", "value": "..."}]}
    """
    client = get_client()
    prompt = f"""以下は企業の問い合わせフォームのHTMLです。
各入力フィールドを分析し、営業メールを送るために何を入力すべきかJSON形式で回答してください。

```json
{{
  "field_mappings": [
    {{
      "selector": "CSSセレクタ（name属性 or id属性ベース）",
      "field_name": "フィールドの名前（例：会社名、名前、メールアドレス等）",
      "type": "input/textarea/select/checkbox/radio",
      "fill_with": "sender_company/sender_name/sender_name_sei/sender_name_mei/sender_email/sender_phone/sender_title/subject/message/other",
      "value": "selectやradioの場合の選択値、checkboxならtrue/false、それ以外は空文字"
    }}
  ],
  "submit_selector": "送信ボタンのCSSセレクタ",
  "has_confirm_page": true/false,
  "notes": "特記事項（任意）"
}}
```

重要:
- selectorはname属性があれば `[name="xxx"]`、なければid属性 `#xxx` を優先してください
- 「お問い合わせ種別」のようなselectやradioがある場合、最も一般的な選択肢のvalueを指定してください
- 必須項目は見逃さないでください
- JSON以外の文字は出力しないでください
- 姓名が分かれている場合: 姓→sender_name_sei、名→sender_name_mei
- 役職欄がある場合: sender_title
- 「その他　備考欄」「お問い合わせ内容」などのテキストエリアはmessageにマッピング
- プライバシーポリシーの同意チェックボックスがある場合: fill_with="other", value="true"

--- フォームHTML ---
{form_html[:6000]}
"""
    response = client.models.generate_content(
        model=get_model(),
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=2000,
            response_mime_type="application/json",
        ),
    )
    text = response.text.strip()
    result = _parse_json_response(text)
    # フォールバック: field_mappingsがない場合のデフォルト
    if "field_mappings" not in result:
        result = {"field_mappings": [], "submit_selector": "", "has_confirm_page": False, "notes": ""}
    return result


def generate_schedule_slots(config: dict) -> list[str]:
    """送信日から指定日数後以降の日程候補を生成"""
    sched = config["scheduling"]
    days_after = sched["days_after_send"]
    num_slots = sched["num_slots"]
    start_hour = int(sched["time_range_start"].split(":")[0])
    end_hour = int(sched["time_range_end"].split(":")[0])

    base_date = datetime.now() + timedelta(days=days_after)
    slots = []
    candidate = base_date

    while len(slots) < num_slots:
        # 土日はスキップ
        if candidate.weekday() < 5:
            hour = start_hour + (len(slots) * 3) % (end_hour - start_hour)
            slot_time = candidate.replace(hour=hour, minute=0)
            slots.append(slot_time.strftime("%m月%d日（{}）%H:%M〜".format(
                ["月", "火", "水", "木", "金", "土", "日"][candidate.weekday()]
            )))
        candidate += timedelta(days=1)

    return slots
