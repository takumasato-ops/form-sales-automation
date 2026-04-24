#!/bin/bash
# フォーム営業自動化 cron セットアップスクリプト
# 実行: bash setup_cron.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON=$(which python3)
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "=== フォーム営業 cron セットアップ ==="
echo "スクリプトディレクトリ: $SCRIPT_DIR"
echo "Python: $PYTHON"

# 既存のフォーム営業 cron を削除
crontab -l 2>/dev/null | grep -v "form-sales-automation" > /tmp/crontab_backup.txt

# 新しい cron タスクを追加
cat >> /tmp/crontab_backup.txt << EOF

# ========================================
# フォーム営業自動化
# ========================================

# 毎日（土日含む）7:00〜20:00 に30分ごとに自動実行（最大50件/回）
0,30 7-19 * * * cd $SCRIPT_DIR && $PYTHON main.py run --max 50 >> $LOG_DIR/cron_run.log 2>&1

# 毎日 20:30 に日次Slackレポートを送信
30 20 * * * cd $SCRIPT_DIR && $PYTHON slack_report.py daily >> $LOG_DIR/cron_daily_report.log 2>&1

# 毎朝 6:00 にリストを自動補充（Wantedly + 求人ボックス）
0 6 * * * cd $SCRIPT_DIR && $PYTHON list_generator.py wantedly --pages 5 --import >> $LOG_DIR/cron_list_gen.log 2>&1
0 6 * * * cd $SCRIPT_DIR && $PYTHON list_generator.py kyujinbox --keyword "中小企業" --pages 5 --import >> $LOG_DIR/cron_list_gen.log 2>&1

# 毎週月曜 8:00 に週次PDCAレポートをSlack送信 + CSVエクスポート
0 8 * * 1 cd $SCRIPT_DIR && $PYTHON slack_report.py weekly >> $LOG_DIR/cron_weekly_report.log 2>&1
0 8 * * 1 cd $SCRIPT_DIR && $PYTHON sheets_export.py >> $LOG_DIR/cron_export.log 2>&1

EOF

# cron を反映
crontab /tmp/crontab_backup.txt
rm /tmp/crontab_backup.txt

echo ""
echo "✅ cron設定完了！"
echo ""
echo "設定されたタスク:"
echo "  - 毎日（土日含む）7:00〜19:30 毎30分: 自動送信（最大50件/回）"
echo "  - 毎日 20:30: 日次Slackレポート"
echo "  - 毎朝 6:00: リスト自動補充（Wantedly + 求人ボックス）"
echo "  - 毎週月曜 8:00: 週次PDCAレポート + CSVエクスポート"
echo ""
echo "現在のcron一覧:"
crontab -l | grep "form-sales"
