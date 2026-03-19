import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from PIL import Image, ImageDraw

from context.constant import BASE_DIR, LOG_PATH, TIME_ZONE


def log(msg, stdout: bool = True) -> None:
    if stdout:
        print(msg)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    line = f"[{timestamp}] {msg}"
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def trunc_to_hour(dt: datetime, hour: int) -> datetime:
    return dt.replace(hour=hour, minute=0, second=0, microsecond=0)


def format_generated_at_pst() -> str:
    now_pst = datetime.now(timezone.utc).astimezone(TIME_ZONE)
    return now_pst.strftime("%Y-%m-%d %H:%M %Z")


def seconds_until_next_run(run_again: bool) -> int:
    now = datetime.now(timezone.utc).astimezone(TIME_ZONE)
    morning_target = now.replace(hour=10, minute=0, second=0, microsecond=0)
    afternoon_target = now.replace(hour=16, minute=0, second=0, microsecond=0)
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


def sort_images_by_valid_time(image_paths: List[Path]) -> List[Path]:
    def extract_sort_key(path: Path):
        try:
            rel_parts = path.relative_to(BASE_DIR).parts
            if len(rel_parts) < 5 or rel_parts[0] != "data":
                raise ValueError(f"Unexpected image path layout: {path}")
            model_name = rel_parts[1]
            product_name = rel_parts[3]
            valid_dt = parse_utc_timestamp(Path(rel_parts[4]).stem)
            return (0, valid_dt, model_name, product_name, str(path))
        except Exception:
            return (1, datetime.max, "zz_unknown", "zz_unknown", str(path))

    return sorted(list(dict.fromkeys(image_paths)), key=extract_sort_key)


def prune_old_runs(root: Path, days: int = 2) -> None:
    if not root.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for model_dir in (p for p in root.iterdir() if p.is_dir()):
        for run_dir in (p for p in model_dir.iterdir() if p.is_dir()):
            try:
                run_dt = datetime.strptime(run_dir.name, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if run_dt < cutoff:
                shutil.rmtree(run_dir, ignore_errors=True)
