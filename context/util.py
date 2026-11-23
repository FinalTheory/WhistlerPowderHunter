import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from PIL import Image, ImageDraw

from context.constant import TIME_ZONE, RESORT_CLOSE_HOUR


def log(msg) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{timestamp}] {msg}")


def trunc_to_hour(dt: datetime, hour: int) -> datetime:
    return dt.replace(hour=hour, minute=0, second=0, microsecond=0)


def decide_forecast_image_start_time():
    now = datetime.now(TIME_ZONE)
    close = trunc_to_hour(now, RESORT_CLOSE_HOUR)
    return max(now, close)


def format_generated_at_local() -> str:
    local_now = datetime.now(timezone.utc).astimezone(TIME_ZONE)
    return local_now.strftime("%Y-%m-%d %H:%M (Whistler, UTC%z)")


def seconds_until_next_run(run_again: bool) -> int:
    now = datetime.now(timezone.utc).astimezone(TIME_ZONE)
    morning_target = now.replace(hour=10, minute=0, second=0, microsecond=0)
    afternoon_target = now.replace(hour=16, minute=30, second=0, microsecond=0)
    if now < morning_target:
        target = morning_target
    elif run_again and now < afternoon_target:
        target = afternoon_target
    else:
        target = morning_target + timedelta(days=1)
    return max(1, int((target - now).total_seconds())) + 120


def hours_between(start_ts: str, end_ts: str) -> int:
    start = parse_utc_timestamp(start_ts)
    end = parse_utc_timestamp(end_ts)
    return max(0, int((end - start).total_seconds() // 3600))


def parse_utc_timestamp(ts: str) -> datetime:
    if len(ts) == 10:
        return datetime.strptime(ts, "%Y%m%d%H")
    if len(ts) == 12:
        return datetime.strptime(ts, "%Y%m%d%H%M")
    raise ValueError(f"Unsupported timestamp format: {ts}")


def add_red_dot(path: Path, x: int, y: int, radius: int) -> None:
    img = Image.open(path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=(255, 0, 0, 255))
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        img = img.convert("RGB")
    img.save(path)


def prune_old_runs(root: Path, *, days: int, keep: set[tuple[str, str]]) -> None:
    if not root.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for model_dir in (p for p in root.iterdir() if p.is_dir()):
        for run_dir in (p for p in model_dir.iterdir() if p.is_dir()):
            try:
                run_dt = datetime.strptime(run_dir.name, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if run_dt < cutoff and (model_dir.name, run_dir.name) not in keep:
                shutil.rmtree(run_dir, ignore_errors=True)
