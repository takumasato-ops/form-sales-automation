"""
300件到達まで連続バッチを回すスクリプト
使い方: python3 chase300.py
"""
import subprocess
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
import database

DAILY_LIMIT = 300
BATCH_SIZE = 50

def get_running_batches():
    result = subprocess.run(
        ["ps", "aux"],
        capture_output=True, text=True
    )
    count = sum(1 for line in result.stdout.splitlines()
                if "main.py run" in line and "grep" not in line)
    return count

def main():
    print("=== 300件チェイサー起動 ===")
    while True:
        today_sent = database.get_stats()["today_sent"]
        print(f"[{time.strftime('%H:%M')}] 今日の送信: {today_sent}件", flush=True)

        if today_sent >= DAILY_LIMIT:
            print(f"[{time.strftime('%H:%M')}] 🎉 {DAILY_LIMIT}件達成！終了します")
            break

        running = get_running_batches()
        if running == 0:
            remaining = DAILY_LIMIT - today_sent
            batch = min(BATCH_SIZE, remaining + 10)
            print(f"[{time.strftime('%H:%M')}] バッチ起動: {batch}件処理", flush=True)
            subprocess.run(
                ["python3", "main.py", "run", f"--max", str(batch)],
                cwd=os.path.dirname(__file__)
            )
        else:
            print(f"[{time.strftime('%H:%M')}]  バッチ実行中 ({running}プロセス) ... 30秒待機", flush=True)
            time.sleep(30)

if __name__ == "__main__":
    main()
