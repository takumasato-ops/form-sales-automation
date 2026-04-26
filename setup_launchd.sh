#!/bin/bash
# ============================================================
# フォーム営業自動化 launchd セットアップスクリプト
# cronより確実に動作するmacOS標準のスケジューラを使用
#
# 使い方: bash setup_launchd.sh
# 解除:   bash setup_launchd.sh --unload
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST_SRC="$SCRIPT_DIR/com.canalair.form-sales.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/com.canalair.form-sales.plist"
LOG_DIR="$SCRIPT_DIR/logs"
LABEL="com.canalair.form-sales"

mkdir -p "$LOG_DIR"

if [ "$1" = "--unload" ]; then
    echo "=== launchd ジョブを停止・削除します ==="
    launchctl unload "$PLIST_DEST" 2>/dev/null
    rm -f "$PLIST_DEST"
    echo "✅ 削除完了"
    exit 0
fi

echo "=== フォーム営業 launchd セットアップ ==="
echo "plistコピー先: $PLIST_DEST"

# 既存のジョブを一旦停止
launchctl unload "$PLIST_DEST" 2>/dev/null

# plistをLaunchAgentsにコピー
cp "$PLIST_SRC" "$PLIST_DEST"

# launchdに登録
launchctl load "$PLIST_DEST"

echo ""
echo "✅ launchd登録完了！"
echo ""
echo "動作確認:"
launchctl list | grep "canalair"
echo ""
echo "設定内容:"
echo "  - 毎日 7:00〜19:00 を30分ごとに自動実行（25回/日）"
echo "  - 1回あたり最大15件 × 25回 = 最大375件/日"
echo "  - max_per_day: 300件でキャップ"
echo "  - ログ: $LOG_DIR/launchd_run.log"
echo ""
echo "ログ確認:"
echo "  tail -f $LOG_DIR/launchd_run.log"
echo ""
echo "停止する場合:"
echo "  bash setup_launchd.sh --unload"
