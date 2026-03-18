import sys
import argparse
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from context.constant import BASE_DIR, TIME_ZONE
from context.pipeline import run_analysis
from context.util import log, seconds_until_next_run


sys.dont_write_bytecode = True

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Pivotal Weather model imagery in parallel (beta API)")
    parser.add_argument("--out", type=Path, default=BASE_DIR, help="Output directory for downloaded images")
    parser.add_argument("--workers", type=int, default=12, help="Thread pool size")
    parser.add_argument("--debug", action="store_true", help="Run once and exit instead of looping every 24h")
    parser.add_argument("--no-gpt", action="store_true", help="Do not call chatgpt.")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Do not run immediately on startup; wait until the next scheduled run time.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.debug:
        run_analysis(args)
        return

    def do_sleep(run_again):
        sleep_seconds = seconds_until_next_run(run_again)
        next_run = datetime.now(timezone.utc).astimezone(TIME_ZONE) + timedelta(seconds=sleep_seconds)
        log(f"Next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M %Z')} ({sleep_seconds}s)")
        time.sleep(sleep_seconds)

    if args.wait:
        do_sleep(False)

    while True:
        run_again = False
        try:
            run_again = run_analysis(args)
        except Exception:
            log(traceback.format_exc())
        do_sleep(run_again)


if __name__ == "__main__":
    main()
