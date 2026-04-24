"""
Google Sheets / CSV エクスポートモジュール（プロデザイン版）

シート構成:
  1. 📊 ダッシュボード  — KPIカード + 週次サマリー
  2. 📋 全リード一覧    — 条件付き書式付きの全データ
  3. 📈 分析レポート    — テンプレート・業種・曜日別パフォーマンス
"""

import os
import csv
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from database import get_all_leads_for_export, get_weekly_analytics, get_stats


EXPORT_DIR = os.path.join(os.path.dirname(__file__), "data", "exports")

HEADERS = [
    "ID", "企業名", "業種", "URL", "問い合わせフォームURL",
    "ステータス", "ソースCSV", "テンプレート",
    "返信あり", "返信日時", "返信メモ",
    "送信日時", "スキップ理由", "登録日時",
]

STATUS_LABELS = {
    "pending": "未処理",
    "sent": "送信済",
    "skipped": "スキップ",
    "failed": "失敗",
}


# ─────────────────────────────────────────────
# カラーパレット（Sheets API: 0.0〜1.0）
# ─────────────────────────────────────────────
def _rgb(r, g, b):
    return {"red": r / 255, "green": g / 255, "blue": b / 255}

C = {
    "navy":        _rgb(26,  46,  90),   # #1A2E5A ヘッダー背景
    "blue":        _rgb(66, 133, 244),   # #4285F4 サブヘッダー
    "blue_light":  _rgb(210, 227, 252),  # #D2E3FC 薄青
    "teal":        _rgb(0,  150, 136),   # #009688 アクセント緑
    "teal_light":  _rgb(224, 247, 245),  # #E0F7FA 薄緑
    "green":       _rgb(52,  168,  83),  # #34A853 成功
    "green_light": _rgb(230, 244, 234),  # #E6F4EA 薄緑
    "red":         _rgb(234,  67,  53),  # #EA4335 失敗
    "red_light":   _rgb(253, 232, 230),  # #FDE8E6 薄赤
    "amber":       _rgb(251, 188,   4),  # #FBBC04 警告
    "amber_light": _rgb(255, 249, 224),  # #FFF9E0 薄黄
    "gray_dark":   _rgb( 95,  99, 104),  # #5F6368 グレー文字
    "gray_mid":    _rgb(218, 220, 224),  # #DADCE0 ボーダー
    "gray_light":  _rgb(248, 249, 250),  # #F8F9FA 交互行
    "white":       _rgb(255, 255, 255),
}


def _cell_fmt(bg=None, fg=None, bold=False, size=10, halign=None, valign=None,
              wrap=None, top=False, bottom=False, border_color=None):
    """セルフォーマット辞書を生成"""
    fmt = {}
    if bg:
        fmt["backgroundColor"] = bg
    if fg or bold or size != 10:
        fmt["textFormat"] = {}
        if fg:
            fmt["textFormat"]["foregroundColor"] = fg
        if bold:
            fmt["textFormat"]["bold"] = True
        if size != 10:
            fmt["textFormat"]["fontSize"] = size
    if halign:
        fmt["horizontalAlignment"] = halign
    if valign:
        fmt["verticalAlignment"] = valign
    if wrap:
        fmt["wrapStrategy"] = wrap
    if top or bottom:
        border = {"style": "SOLID", "width": 2,
                  "color": border_color or C["gray_mid"]}
        fmt["borders"] = {}
        if top:
            fmt["borders"]["top"] = border
        if bottom:
            fmt["borders"]["bottom"] = border
    return fmt


def _range(sheet_id, r1, c1, r2, c2):
    return {
        "sheetId": sheet_id,
        "startRowIndex": r1,
        "endRowIndex": r2,
        "startColumnIndex": c1,
        "endColumnIndex": c2,
    }


def _repeat_cell(sheet_id, r1, c1, r2, c2, fmt):
    return {
        "repeatCell": {
            "range": _range(sheet_id, r1, c1, r2, c2),
            "cell": {"userEnteredFormat": fmt},
            "fields": "userEnteredFormat(" + ",".join([
                "backgroundColor", "textFormat",
                "horizontalAlignment", "verticalAlignment",
                "wrapStrategy", "borders",
            ]) + ")",
        }
    }


def _merge(sheet_id, r1, c1, r2, c2):
    return {
        "mergeCells": {
            "range": _range(sheet_id, r1, c1, r2, c2),
            "mergeType": "MERGE_ALL",
        }
    }


def _col_width(sheet_id, col_start, col_end, px):
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": col_start,
                "endIndex": col_end,
            },
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }


def _row_height(sheet_id, row_start, row_end, px):
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": row_start,
                "endIndex": row_end,
            },
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }


def _freeze(sheet_id, rows=1, cols=0):
    return {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": rows, "frozenColumnCount": cols},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    }


def _tab_color(sheet_id, color):
    return {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "tabColorStyle": {"rgbColor": color},
            },
            "fields": "tabColorStyle",
        }
    }


# ─────────────────────────────────────────────
# CSV エクスポート
# ─────────────────────────────────────────────
def _row_to_list(lead: dict) -> list:
    return [
        lead["id"],
        lead["company_name"],
        lead["industry"] or "",
        lead["url"],
        lead["contact_form_url"] or "",
        STATUS_LABELS.get(lead["status"], lead["status"]),
        lead["source_file"] or "",
        lead["template_name"] or "default",
        "○" if lead["response_received"] else "",
        (lead["responded_at"] or "")[:16],
        lead["response_notes"] or "",
        (lead["sent_at"] or "")[:16],
        lead["skip_reason"] or "",
        (lead["created_at"] or "")[:16],
    ]


def export_csv(output_path: str = None) -> str:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(EXPORT_DIR, f"leads_{timestamp}.csv")
    leads = get_all_leads_for_export()
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        for lead in leads:
            writer.writerow(_row_to_list(lead))
    return output_path


# ─────────────────────────────────────────────
# Google Sheets エクスポート（メイン）
# ─────────────────────────────────────────────
def export_to_sheets(config: dict) -> bool:
    sheets_cfg = config.get("sheets", {})
    sa_json = sheets_cfg.get("service_account_json", "")
    spreadsheet_id = sheets_cfg.get("spreadsheet_id", "")

    if not sa_json or not spreadsheet_id:
        return False

    # パスが相対パスの場合は絶対パスに変換
    if not os.path.isabs(sa_json):
        sa_json = os.path.join(os.path.dirname(__file__), sa_json)

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("gspread が未インストールです。`pip3 install gspread` を実行してください。")
        return False

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(sa_json, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)

    analytics = get_weekly_analytics(weeks_back=4)
    stats = get_stats()
    leads = get_all_leads_for_export()

    _build_dashboard(sh, stats, analytics)
    _build_leads_sheet(sh, leads)
    _build_analysis_sheet(sh, analytics, stats)

    return True


# ─────────────────────────────────────────────
# Sheet 1: 📊 ダッシュボード
# ─────────────────────────────────────────────
def _build_dashboard(sh, stats: dict, analytics: dict):
    import gspread
    SNAME = "📊 ダッシュボード"
    try:
        ws = sh.worksheet(SNAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SNAME, rows=80, cols=10)

    sid = ws.id
    totals = analytics.get("totals", {})
    now_str = datetime.now().strftime("%Y/%m/%d %H:%M 更新")
    overall_rate = totals.get("overall_rate") or 0
    total_responses = totals.get("total_responses") or 0

    # ── データ書き込み ──
    ws.update([
        # タイトルエリア
        ["キャナルAI　フォーム営業 ダッシュボード", "", "", "", "", "", "", "", "", ""],
        [now_str, "", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", ""],

        # KPIラベル行
        ["総リード数", "", "送信済み", "", "返信数", "", "返信率", "", "未処理", ""],
        # KPI値行
        [stats["total"], "", stats["sent"], "", total_responses, "",
         f"{overall_rate}%", "", stats["pending"], ""],
        # KPIサブ行
        ["全登録企業", "", "送信完了", "", "返信受信", "", "送信対比", "", "送信待ち", ""],
        ["", "", "", "", "", "", "", "", "", ""],

        # 2段目KPIラベル
        ["スキップ数", "", "失敗数", "", "本日の送信", "", "1日上限", "", "", ""],
        [stats["skipped"], "", stats["failed"], "", stats["today_sent"], "", "500件", "", "", ""],
        ["営業お断り等", "", "フォームエラー等", "", "今日の実績", "", "設定値", "", "", ""],
        ["", "", "", "", "", "", "", "", "", ""],

        # セクション区切り
        ["テンプレート別パフォーマンス（過去4週間）", "", "", "", "", "", "", "", "", ""],
        ["テンプレート", "送信数", "返信数", "返信率", "評価", "", "", "", "", ""],
    ], value_input_option="USER_ENTERED")

    # テンプレートデータ
    t_rows = []
    for t in analytics.get("template_stats", []):
        rate = t.get("response_rate") or 0
        if rate >= 3:
            eval_str = "★★★ 高パフォーマンス"
        elif rate >= 1:
            eval_str = "★★☆ 標準"
        else:
            eval_str = "★☆☆ 要改善"
        t_rows.append([t["template_name"], t["sent"], t.get("responses") or 0,
                        f"{rate}%", eval_str, "", "", "", "", ""])
    if not t_rows:
        t_rows = [["データなし", 0, 0, "0%", "—", "", "", "", "", ""]]

    ws.append_rows(t_rows, value_input_option="USER_ENTERED")
    next_row = 13 + len(t_rows)

    # 業種別セクション
    industry_data = analytics.get("industry_stats", [])
    ws.append_rows([
        ["", "", "", "", "", "", "", "", "", ""],
        ["業種別パフォーマンス（送信上位）", "", "", "", "", "", "", "", "", ""],
        ["業種", "送信数", "返信数", "返信率", "", "", "", "", "", ""],
    ], value_input_option="USER_ENTERED")
    ind_row_start = next_row + 3
    ind_rows = []
    for r in industry_data[:10]:
        ind_rows.append([
            r["industry"], r["sent"], r.get("responses") or 0,
            f"{r.get('response_rate') or 0}%", "", "", "", "", "", ""
        ])
    if not ind_rows:
        ind_rows = [["データなし", 0, 0, "0%", "", "", "", "", "", ""]]
    ws.append_rows(ind_rows, value_input_option="USER_ENTERED")

    # ── フォーマット適用 ──
    requests = []

    # カラム幅設定
    requests += [
        _col_width(sid, 0, 1, 160),   # A: ラベル
        _col_width(sid, 1, 2, 90),    # B
        _col_width(sid, 2, 3, 160),   # C
        _col_width(sid, 3, 4, 90),    # D
        _col_width(sid, 4, 5, 120),   # E
        _col_width(sid, 5, 6, 90),    # F
        _col_width(sid, 6, 7, 120),   # G
        _col_width(sid, 7, 8, 90),    # H
        _col_width(sid, 8, 9, 120),   # I
        _col_width(sid, 9, 10, 90),   # J
    ]

    # 行高設定
    requests += [
        _row_height(sid, 0, 1, 50),   # タイトル行
        _row_height(sid, 1, 2, 24),
        _row_height(sid, 3, 4, 24),   # KPIラベル
        _row_height(sid, 4, 5, 56),   # KPI値（大きく）
        _row_height(sid, 5, 6, 20),   # KPIサブ
        _row_height(sid, 7, 8, 24),
        _row_height(sid, 8, 9, 56),
        _row_height(sid, 9, 10, 20),
    ]

    # ①タイトル行（A1:J1）navy背景・白文字
    requests += [
        _merge(sid, 0, 0, 1, 10),
        _repeat_cell(sid, 0, 0, 1, 10, _cell_fmt(
            bg=C["navy"], fg=C["white"], bold=True, size=16, halign="CENTER", valign="MIDDLE"
        )),
    ]

    # ②更新日時行（A2:J2）
    requests += [
        _merge(sid, 1, 0, 2, 10),
        _repeat_cell(sid, 1, 0, 2, 10, _cell_fmt(
            bg=C["blue_light"], fg=C["gray_dark"], halign="RIGHT", valign="MIDDLE"
        )),
    ]

    # ③ KPI ブロック（1段目）——各KPIを2列ペア
    kpi1_pairs = [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]
    kpi1_colors = [C["blue_light"], C["green_light"], C["teal_light"], C["amber_light"], C["gray_light"]]
    for (c1, c2), bg in zip(kpi1_pairs, kpi1_colors):
        # ラベル
        requests += [
            _merge(sid, 3, c1, 4, c2),
            _repeat_cell(sid, 3, c1, 4, c2, _cell_fmt(
                bg=bg, fg=C["gray_dark"], bold=True, halign="CENTER", valign="MIDDLE"
            )),
            # 値（大きい数字）
            _merge(sid, 4, c1, 5, c2),
            _repeat_cell(sid, 4, c1, 5, c2, _cell_fmt(
                bg=bg, fg=C["navy"], bold=True, size=22, halign="CENTER", valign="MIDDLE"
            )),
            # サブ説明
            _merge(sid, 5, c1, 6, c2),
            _repeat_cell(sid, 5, c1, 6, c2, _cell_fmt(
                bg=bg, fg=C["gray_dark"], halign="CENTER", valign="MIDDLE", size=9
            )),
        ]

    # ④ KPI ブロック（2段目）
    kpi2_pairs = [(0, 2), (2, 4), (4, 6), (6, 8)]
    kpi2_colors = [C["amber_light"], C["red_light"], C["green_light"], C["blue_light"]]
    for (c1, c2), bg in zip(kpi2_pairs, kpi2_colors):
        requests += [
            _merge(sid, 7, c1, 8, c2),
            _repeat_cell(sid, 7, c1, 8, c2, _cell_fmt(
                bg=bg, fg=C["gray_dark"], bold=True, halign="CENTER", valign="MIDDLE"
            )),
            _merge(sid, 8, c1, 9, c2),
            _repeat_cell(sid, 8, c1, 9, c2, _cell_fmt(
                bg=bg, fg=C["navy"], bold=True, size=22, halign="CENTER", valign="MIDDLE"
            )),
            _merge(sid, 9, c1, 10, c2),
            _repeat_cell(sid, 9, c1, 10, c2, _cell_fmt(
                bg=bg, fg=C["gray_dark"], halign="CENTER", valign="MIDDLE", size=9
            )),
        ]

    # ⑤ テンプレートセクションヘッダー（Row 12）
    requests += [
        _merge(sid, 11, 0, 12, 10),
        _repeat_cell(sid, 11, 0, 12, 10, _cell_fmt(
            bg=C["navy"], fg=C["white"], bold=True, size=12,
            halign="LEFT", valign="MIDDLE"
        )),
        # テーブルヘッダー（Row 13）
        _repeat_cell(sid, 12, 0, 13, 5, _cell_fmt(
            bg=C["blue"], fg=C["white"], bold=True, halign="CENTER", valign="MIDDLE"
        )),
        # テンプレートデータ行（交互色）
    ]
    for i in range(len(t_rows)):
        row = 13 + i
        bg = C["white"] if i % 2 == 0 else C["gray_light"]
        requests.append(_repeat_cell(sid, row, 0, row + 1, 5, _cell_fmt(
            bg=bg, halign="CENTER", valign="MIDDLE"
        )))
        # 返信率列（D列 = col 3）を色付け
        rate_val = t_rows[i][3]
        rate_num = float(str(rate_val).replace("%", "") or 0)
        if rate_num >= 3:
            rate_color = C["green_light"]
        elif rate_num >= 1:
            rate_color = C["amber_light"]
        else:
            rate_color = C["red_light"]
        requests.append(_repeat_cell(sid, row, 3, row + 1, 4, _cell_fmt(
            bg=rate_color, bold=True, halign="CENTER", valign="MIDDLE"
        )))

    # ⑥ 業種セクションヘッダー
    ind_section_row = next_row
    requests += [
        _merge(sid, ind_section_row + 1, 0, ind_section_row + 2, 10),
        _repeat_cell(sid, ind_section_row + 1, 0, ind_section_row + 2, 10, _cell_fmt(
            bg=C["teal"], fg=C["white"], bold=True, size=12,
            halign="LEFT", valign="MIDDLE"
        )),
        _repeat_cell(sid, ind_section_row + 2, 0, ind_section_row + 3, 4, _cell_fmt(
            bg=C["blue"], fg=C["white"], bold=True, halign="CENTER", valign="MIDDLE"
        )),
    ]
    for i in range(len(ind_rows)):
        row = ind_row_start + i
        bg = C["white"] if i % 2 == 0 else C["gray_light"]
        requests.append(_repeat_cell(sid, row, 0, row + 1, 4, _cell_fmt(
            bg=bg, halign="CENTER", valign="MIDDLE"
        )))

    # タブ色・フリーズ
    requests += [
        _tab_color(sid, C["navy"]),
        _freeze(sid, rows=0),
    ]

    sh.batch_update({"requests": requests})


# ─────────────────────────────────────────────
# Sheet 2: 📋 全リード一覧
# ─────────────────────────────────────────────
def _build_leads_sheet(sh, leads: list):
    import gspread
    SNAME = "📋 全リード一覧"
    try:
        ws = sh.worksheet(SNAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SNAME, rows=max(len(leads) + 10, 200), cols=14)

    sid = ws.id
    rows = [HEADERS] + [_row_to_list(lead) for lead in leads]
    ws.update(rows, value_input_option="USER_ENTERED")

    requests = []

    # カラム幅
    col_widths = [50, 200, 100, 200, 180, 80, 120, 90, 70, 130, 180, 130, 200, 130]
    for i, px in enumerate(col_widths):
        requests.append(_col_width(sid, i, i + 1, px))

    # ヘッダー行
    requests += [
        _row_height(sid, 0, 1, 36),
        _repeat_cell(sid, 0, 0, 1, 14, _cell_fmt(
            bg=C["navy"], fg=C["white"], bold=True, halign="CENTER", valign="MIDDLE"
        )),
        _freeze(sid, rows=1, cols=2),
    ]

    # データ行の交互色 + ステータス列の条件色
    status_col = 5  # F列 = index 5
    for i, lead in enumerate(leads):
        row = i + 1
        bg = C["white"] if i % 2 == 0 else C["gray_light"]
        requests.append(_repeat_cell(sid, row, 0, row + 1, 14, _cell_fmt(
            bg=bg, valign="MIDDLE"
        )))
        # ステータス列を色分け
        status = lead.get("status", "")
        if status == "sent":
            st_bg = C["green_light"]
            st_fg = C["green"]
        elif status == "skipped":
            st_bg = C["amber_light"]
            st_fg = _rgb(180, 100, 0)
        elif status == "failed":
            st_bg = C["red_light"]
            st_fg = C["red"]
        else:
            st_bg = C["blue_light"]
            st_fg = C["blue"]
        requests.append(_repeat_cell(sid, row, status_col, row + 1, status_col + 1, _cell_fmt(
            bg=st_bg, fg=st_fg, bold=True, halign="CENTER", valign="MIDDLE"
        )))

    # オートフィルター
    requests.append({
        "setBasicFilter": {
            "filter": {
                "range": _range(sid, 0, 0, len(leads) + 1, 14)
            }
        }
    })

    requests.append(_tab_color(sid, C["blue"]))
    sh.batch_update({"requests": requests})


# ─────────────────────────────────────────────
# Sheet 3: 📈 分析レポート
# ─────────────────────────────────────────────
def _build_analysis_sheet(sh, analytics: dict, stats: dict):
    import gspread
    SNAME = "📈 分析レポート"
    try:
        ws = sh.worksheet(SNAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SNAME, rows=100, cols=8)

    sid = ws.id
    now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
    totals = analytics.get("totals", {})

    template_stats = analytics.get("template_stats", [])
    industry_stats = analytics.get("industry_stats", [])
    weekday_stats  = analytics.get("weekday_stats",  [])

    rows_data = [
        # タイトル
        ["分析レポート（過去4週間）", "", "", "", "", "", "", ""],
        [f"生成日時：{now_str}", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", ""],

        # 全体サマリー
        ["▌ 全体サマリー", "", "", "", "", "", "", ""],
        ["累計送信数", stats["sent"], "累計返信数", totals.get("total_responses") or 0,
         "累計返信率", f"{totals.get('overall_rate') or 0}%", "", ""],
        ["", "", "", "", "", "", "", ""],

        # テンプレート別
        ["▌ テンプレート別パフォーマンス", "", "", "", "", "", "", ""],
        ["テンプレート", "送信数", "返信数", "返信率 (%)", "グラフ（棒）", "", "", ""],
    ]

    t_start = len(rows_data)
    for t in template_stats:
        rate = t.get("response_rate") or 0
        bar = "█" * int(rate * 2) + "░" * max(0, 20 - int(rate * 2))
        rows_data.append([t["template_name"], t["sent"], t.get("responses") or 0,
                           rate, bar, "", "", ""])

    if not template_stats:
        rows_data.append(["データなし", 0, 0, 0, "", "", "", ""])
    t_end = len(rows_data)

    rows_data += [
        ["", "", "", "", "", "", "", ""],
        ["▌ 業種別パフォーマンス（送信上位10）", "", "", "", "", "", "", ""],
        ["業種", "送信数", "返信数", "返信率 (%)", "グラフ（棒）", "", "", ""],
    ]
    ind_start = len(rows_data)
    for r in industry_stats[:10]:
        rate = r.get("response_rate") or 0
        bar = "█" * int(rate * 2) + "░" * max(0, 20 - int(rate * 2))
        rows_data.append([r["industry"], r["sent"], r.get("responses") or 0,
                           rate, bar, "", "", ""])
    if not industry_stats:
        rows_data.append(["データなし", 0, 0, 0, "", "", "", ""])
    ind_end = len(rows_data)

    rows_data += [
        ["", "", "", "", "", "", "", ""],
        ["▌ 曜日別パフォーマンス", "", "", "", "", "", "", ""],
        ["曜日", "送信数", "返信数", "返信率 (%)", "", "", "", ""],
    ]
    wd_start = len(rows_data)
    for w in weekday_stats:
        rate = round(((w.get("responses") or 0) / max(w["sent"], 1)) * 100, 1)
        rows_data.append([w["weekday"] + "曜日", w["sent"],
                           w.get("responses") or 0, rate, "", "", "", ""])
    if not weekday_stats:
        rows_data.append(["データなし", 0, 0, 0, "", "", "", ""])

    ws.update(rows_data, value_input_option="USER_ENTERED")

    requests = []

    # カラム幅
    for i, px in enumerate([180, 80, 80, 100, 220, 100, 100, 100]):
        requests.append(_col_width(sid, i, i + 1, px))

    # タイトル
    requests += [
        _row_height(sid, 0, 1, 44),
        _merge(sid, 0, 0, 1, 8),
        _repeat_cell(sid, 0, 0, 1, 8, _cell_fmt(
            bg=C["navy"], fg=C["white"], bold=True, size=15, halign="CENTER", valign="MIDDLE"
        )),
        _merge(sid, 1, 0, 2, 8),
        _repeat_cell(sid, 1, 0, 2, 8, _cell_fmt(
            bg=C["blue_light"], fg=C["gray_dark"], halign="RIGHT", valign="MIDDLE"
        )),
    ]

    # セクションヘッダー＆テーブルヘッダースタイル適用
    section_rows   = [3, 6, t_end + 1, ind_end + 1]
    section_colors = [C["navy"], C["navy"], C["teal"], C["navy"]]
    for sr, sc in zip(section_rows, section_colors):
        requests += [
            _merge(sid, sr, 0, sr + 1, 8),
            _row_height(sid, sr, sr + 1, 32),
            _repeat_cell(sid, sr, 0, sr + 1, 8, _cell_fmt(
                bg=sc, fg=C["white"], bold=True, size=11,
                halign="LEFT", valign="MIDDLE"
            )),
        ]

    # サマリーデータ行（Row 4 = index 4）
    requests += [
        _row_height(sid, 4, 5, 30),
        _repeat_cell(sid, 4, 0, 5, 6, _cell_fmt(
            bg=C["blue_light"], bold=True, halign="CENTER", valign="MIDDLE", size=11
        )),
    ]

    # テーブルヘッダー
    for header_row in [t_start - 1, ind_start - 1, wd_start - 1]:
        requests += [
            _row_height(sid, header_row, header_row + 1, 30),
            _repeat_cell(sid, header_row, 0, header_row + 1, 5, _cell_fmt(
                bg=C["blue"], fg=C["white"], bold=True, halign="CENTER", valign="MIDDLE"
            )),
        ]

    # テンプレートデータ行
    for i in range(t_end - t_start):
        row = t_start + i
        bg = C["white"] if i % 2 == 0 else C["gray_light"]
        requests.append(_repeat_cell(sid, row, 0, row + 1, 5, _cell_fmt(
            bg=bg, halign="CENTER", valign="MIDDLE"
        )))
        try:
            rate_num = float(rows_data[row][3] or 0)
        except (ValueError, TypeError):
            rate_num = 0
        if rate_num >= 3:
            rate_bg = C["green_light"]
        elif rate_num >= 1:
            rate_bg = C["amber_light"]
        else:
            rate_bg = C["red_light"]
        requests.append(_repeat_cell(sid, row, 3, row + 1, 4, _cell_fmt(
            bg=rate_bg, bold=True, halign="CENTER", valign="MIDDLE"
        )))

    # 業種データ行
    for i in range(ind_end - ind_start):
        row = ind_start + i
        bg = C["white"] if i % 2 == 0 else C["teal_light"]
        requests.append(_repeat_cell(sid, row, 0, row + 1, 5, _cell_fmt(
            bg=bg, halign="CENTER", valign="MIDDLE"
        )))

    requests += [
        _tab_color(sid, C["teal"]),
        _freeze(sid, rows=1),
    ]

    sh.batch_update({"requests": requests})


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    path = export_csv()
    print(f"CSVエクスポート完了: {path}")

    if config.get("sheets", {}).get("spreadsheet_id"):
        success = export_to_sheets(config)
        if success:
            print("Google Sheetsへの同期完了")
        else:
            print("Google Sheets設定が未完了のため、CSVのみエクスポートしました")
