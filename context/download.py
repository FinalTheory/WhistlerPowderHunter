import concurrent.futures
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from context.constant import DEFAULT_HEADERS
from context.model import ModelConfig
from context.util import add_red_dot, log


def download_image(url: str, dest: Path, overwrite: bool = False) -> bool:
    if dest.exists() and not overwrite:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.Session().get(url, headers=DEFAULT_HEADERS, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed {resp.status_code} for {url}")
    dest.write_bytes(resp.content)
    return True


def download_model_run(
    model: ModelConfig,
    tasks: Iterable[Tuple[str, str, str]],
    output_dir: Path,
    workers: int = 8,
    overwrite: bool = False,
    dot_radius: int = 5,
) -> List[Path]:
    saved: List[Path] = []
    coord = model.annotate

    def task(item: Tuple[str, str, str]) -> Optional[Path]:
        frame_time, url, run_hour = item
        target = model.dest_path(output_dir, run_hour, frame_time)
        try:
            changed = download_image(url, target, overwrite=overwrite)
            if changed and coord is not None:
                add_red_dot(target, x=coord[0], y=coord[1], radius=dot_radius)
            return target if changed else None
        except Exception as exc:
            log(f"[{model.name} {run_hour} {frame_time}] error: {exc}")
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(task, tasks):
            if result:
                saved.append(result)
    return saved


def download_all_models(
    models: Iterable[ModelConfig],
    output_dir: Path,
    workers: int = 8,
    overwrite: bool = False,
) -> Dict[str, List[Path]]:
    output: Dict[str, List[Path]] = {}
    model_by_id = {m.id: m for m in models}
    plan_map = {m.id: m.list_forecast_images() for m in models}

    for model_name, plan in plan_map.items():
        model_cfg = model_by_id[model_name]
        if not plan:
            log(f"No runs available for {model_name}")
            continue
        log(f"Downloading {model_name} run {plan.run_hour} to {output_dir} (product={plan.product}, max_fh = {int(plan.max_forecast_hour)})")
        output[model_name] = download_model_run(
            model=model_cfg,
            tasks=plan.tasks,
            output_dir=output_dir,
            workers=workers,
            overwrite=overwrite,
        )
    return output
