import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from context.constant import FRAME_TS_RE, VIEWER_TEMPLATE
from context.model import MODEL_GROUPS, MODELS, ModelConfig
from context.util import hours_between


def latest_run_dir(data_root: Path, model_name: str, product_dir: str) -> Optional[Path]:
    model_dir = data_root / model_name
    if not model_dir.exists():
        return None

    def run_has_images(run_dir: Path) -> bool:
        for product_subdir in (p for p in run_dir.iterdir() if p.is_dir() and p.name == product_dir):
            for file in product_subdir.iterdir():
                if file.is_file() and file.suffix.lower() in {".png", ".jpg", ".jpeg"}:
                    return True
        return False

    for run_dir in sorted((p for p in model_dir.iterdir() if p.is_dir()), reverse=True):
        if run_has_images(run_dir):
            return run_dir
    return None


def frames_in_run(run_dir: Path, data_root: Path, product_dir: str) -> List[Dict[str, object]]:
    frames: List[Dict[str, object]] = []
    for product_subdir in sorted(p for p in run_dir.iterdir() if p.is_dir() and p.name == product_dir):
        for file in sorted(product_subdir.iterdir()):
            if not file.is_file():
                continue
            match = FRAME_TS_RE.search(file.stem)
            if not match:
                continue
            timestamp = match.group(1)
            frames.append({"timestamp": timestamp, "url": str(Path("data") / file.relative_to(data_root))})
    frames.sort(key=lambda f: f["timestamp"])
    return frames


def build_model_payload(model: ModelConfig, data_root: Path) -> Optional[Dict[str, object]]:
    run_dir = latest_run_dir(data_root, model.name, model.product_dir)
    if not run_dir:
        return None
    frames = frames_in_run(run_dir, data_root, model.product_dir)
    if not frames:
        return None
    return {
        "id": model.id,
        "name": model.name,
        "run": run_dir.name,
        "frames": frames,
        "min_timestamp": min(f["timestamp"] for f in frames),
        "max_timestamp": max(f["timestamp"] for f in frames),
        "hide": getattr(model, "hide", False),
    }


def build_groups_payload(data_root: Path) -> List[Dict[str, object]]:
    groups_payload: List[Dict[str, object]] = []
    for group in MODEL_GROUPS:
        models_payload = [payload for model in group.models if (payload := build_model_payload(model, data_root))]
        if not models_payload:
            continue
        group_min_ts = min(m["min_timestamp"] for m in models_payload)
        group_max_ts = max(m["max_timestamp"] for m in models_payload)
        common: Optional[set[str]] = None
        for model in models_payload:
            ts_set = {frame["timestamp"] for frame in model.get("frames", [])}
            common = ts_set if common is None else common & ts_set
            if common is not None and not common:
                break
        groups_payload.append(
            {
                "name": group.name,
                "models": models_payload,
                "min_timestamp": group_min_ts,
                "max_timestamp": group_max_ts,
                "span_hours": hours_between(group_min_ts, group_max_ts),
                "start_timestamp": min(common) if common else group_min_ts,
            }
        )
    return groups_payload


def build_data_inventory(data_root: Path) -> Dict[str, object]:
    inventory: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for model in MODELS:
        run_dir = latest_run_dir(data_root, model.name, model.product_dir)
        if not run_dir:
            continue
        run_payload: Dict[str, List[str]] = {}
        for product_dir in sorted(p for p in run_dir.iterdir() if p.is_dir()):
            files = [f.name for f in sorted(product_dir.iterdir()) if f.is_file()]
            if files:
                run_payload[product_dir.name] = files
        if run_payload:
            inventory.setdefault(model.name, {})[run_dir.name] = run_payload
    return inventory


def render_viewer_html(groups_payload: List[Dict[str, object]], synopsis: Dict[str, str], generated_at: str, output_path: Path) -> Path:
    if not VIEWER_TEMPLATE.exists():
        raise FileNotFoundError(f"Template not found: {VIEWER_TEMPLATE}")
    template = VIEWER_TEMPLATE.read_text(encoding="utf-8")
    rendered = template.replace("{{ groups_json|safe }}", json.dumps(groups_payload))
    rendered = rendered.replace("{{ synopsis_text_zh|safe }}", synopsis.get("zh", "NO Chinese Data"))
    rendered = rendered.replace("{{ synopsis_text_en|safe }}", synopsis.get("en", "NO DATA"))
    rendered = rendered.replace("{{ generated_at|safe }}", generated_at)
    output_path.write_text(rendered, encoding="utf-8")
    return output_path
