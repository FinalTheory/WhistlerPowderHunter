"""
Basic framework to fetch model imagery from Pivotal Weather in parallel.
- Queries the status endpoint per model to discover available runs and lead time range.
- Downloads images for a chosen product across all lead times using a thread pool.

This is only the downloading layer. The later workflow (analysis, HTML generation, GPT orchestration)
can build on top of the saved files in the output directory.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import requests
from PIL import Image, ImageDraw

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}

BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data" / "raw"
VIEWER_TEMPLATE = BASE_DIR / "viewer.html"
VIEWER_RENDERED = BASE_DIR / "index.html"
FRAME_TS_RE = re.compile(r"(\d{10})")

def clean_output_dir(out_dir: Path) -> None:
    """Remove existing downloaded images so each run starts fresh."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


class ModelConfig(ABC):
    """Abstract base for a single model source."""

    def __init__(self, name: str, product: str, hide: bool, annotate: Optional[Tuple[int, int]] = None) -> None:
        self.name = name
        self.product = product
        self.hide = hide
        self.annotate = annotate

    @abstractmethod
    def list_forecast_images(self, session: Optional[requests.Session] = None) -> Optional[Dict[str, object]]:
        """Return a download plan dict or None if unavailable.

        Expected keys: run_hour (str), tasks (List[(frame_time,url,run_hour)]),
        product_label (str), max_fh (int).
        """

    @abstractmethod
    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        """Return destination path for a given frame."""


class PivotalWeatherModel(ModelConfig):
    """Pivotal Weather beta API implementation."""
    BETA_BASE = "https://beta.pivotalweather.com/api"

    def __init__(
        self,
        name: str,
        product: str,
        region: str,
        hide: bool = False,
        run_selector = None,
        annotate: Optional[Tuple[int, int]] = None,
    ) -> None:
        super().__init__(name, product, hide, annotate=annotate)
        self.region = region
        self.run_selector = run_selector or self.longest_run

    @staticmethod
    def fetch_runs_beta(name: str, product: str, region: str, session: Optional[requests.Session] = None) -> List[Dict]:
        """Fetch run manifests from the beta API for a given model/product/region."""
        sess = session or requests.Session()
        url = f"{PivotalWeatherModel.BETA_BASE}/models/{name}/{product}/{region}/runs"
        resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("error"):
            raise ValueError(f"API error for {name}/{product}/{region}: {payload.get('error')}")

        data = payload.get("data")
        if data is None or not isinstance(data, dict):
            raise ValueError(f"Missing 'data' in API response for {name}/{product}/{region}")

        if "runs" not in data:
            raise ValueError(f"Missing 'runs' in API response for {name}/{product}/{region}")

        runs = data["runs"]
        if not isinstance(runs, list):
            raise ValueError(f"'runs' is not a list for {name}/{product}/{region}")
        return runs

    @staticmethod
    def latest_run(runs: List[Dict]) -> Optional[Dict]:
        """Pick the newest run by date string value."""
        if not runs:
            return None
        return sorted(runs, key=lambda r: r.get("date", ""), reverse=True)[0]

    @staticmethod
    def longest_run(runs: List[Dict]) -> Optional[Dict]:
        """Pick the run with the most available forecast hours (break ties by date)."""
        if not runs:
            return None

        def _available_count(run: Dict) -> int:
            forecasts = run.get("forecasts", {}) or {}
            return sum(1 for v in forecasts.values() if isinstance(v, dict) and v.get("available"))

        return sorted(runs, key=lambda r: (_available_count(r), r.get("date", "")), reverse=True)[0]

    @staticmethod
    def build_layer_url(server: str, src: str) -> str:
        """Construct full URL from server code and src path provided by beta API."""
        src_clean = src.lstrip("/")
        return f"https://{server}.pivotalweather.com/{src_clean}"

    def filename(self) -> str:
        return f"{self.product}.{self.region}.png"

    def _iter_forecast_tasks(self, run_manifest: Dict) -> Iterable[Tuple[str, str, str]]:
        run_hour = run_manifest.get("date", "")
        forecasts = run_manifest.get("forecasts")
        if forecasts is None or not isinstance(forecasts, dict):
            raise ValueError("Run manifest missing 'forecasts' dict")

        run_dt = datetime.strptime(run_hour, "%Y%m%d%H").replace(tzinfo=timezone.utc)

        tasks: List[Tuple[str, str, str]] = []
        for fhr_str, fcst in forecasts.items():
            if not isinstance(fcst, dict):
                raise ValueError(f"Forecast entry not dict for fhr {fhr_str}")
            if not fcst.get("available"):
                continue
            layers = fcst.get("layers")
            if not layers:
                raise ValueError(f"Missing layers for fhr {fhr_str}")
            layer = layers[0]
            src = layer.get("src")
            server = layer.get("server")
            if not src or not server:
                raise ValueError(f"Layer missing src/server for fhr {fhr_str}")
            fhr = int(fhr_str)

            frame_dt = run_dt + timedelta(hours=fhr)
            frame_time = frame_dt.strftime("%Y%m%d%H")
            tasks.append((frame_time, self.build_layer_url(server, src), run_hour))

        tasks.sort(key=lambda t: t[0])
        return tasks

    def list_forecast_images(self, session: Optional[requests.Session] = None) -> Optional[Dict[str, object]]:
        runs = self.fetch_runs_beta(self.name, self.product, self.region, session=session)
        run = self.run_selector(runs) if runs else None
        if not run:
            return None

        run_hour = run.get("date", "")
        forecasts = run.get("forecasts", {}) or {}
        max_fh = max((int(k) for k, v in forecasts.items() if isinstance(v, dict) and v.get("available")), default=0)
        tasks = list(self._iter_forecast_tasks(run))
        return {
            "run_hour": run_hour,
            "tasks": tasks,
            "product_label": self.filename(),
            "max_fh": max_fh,
        }

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        product_filename = self.filename()
        return output_dir / self.name / run_hour / f"{self.name}_{frame_time}_{product_filename}"


class AvalancheCanadaModel(ModelConfig):
    """Static URL pattern fetcher for Avalanche Canada imagery."""

    BASE = "https://s3-us-west-2.amazonaws.com/mountain-weather-forecast"

    def __init__(
        self,
        name: str,
        product: str,
        max_fh: int,
        step_hours: int,
        hide: bool = False,
        annotate: Optional[Tuple[int, int]] = None,
    ) -> None:
        super().__init__(name, product, hide, annotate=annotate)
        self.max_fh = max_fh
        self.step_hours = step_hours

    def _select_run_dt(self, now: Optional[datetime] = None) -> datetime:
        now_utc = now or datetime.now(timezone.utc)
        hour = now_utc.hour
        run_hour = 12 if hour >= 12 else 0
        return now_utc.replace(hour=run_hour, minute=0, second=0, microsecond=0)

    def filename(self) -> str:
        return f"{self.product}.jpg"

    def _iter_tasks(self, run_dt: datetime) -> List[Tuple[str, str, str]]:
        run_str = run_dt.strftime("%Y%m%d%H")
        day_path = run_dt.strftime("%Y/%m/%d")
        tasks: List[Tuple[str, str, str]] = []
        for fh in range(0, self.max_fh + self.step_hours, self.step_hours):
            frame_dt = run_dt + timedelta(hours=fh)
            frame_time = frame_dt.strftime("%Y%m%d%H")
            url = f"{self.BASE}/{day_path}/{self.product}_{run_str}_{fh:03d}HR.jpg"
            tasks.append((frame_time, url, run_str))
        return tasks

    def list_forecast_images(self, session: Optional[requests.Session] = None) -> Optional[Dict[str, object]]:
        run_dt = self._select_run_dt()
        run_hour = run_dt.strftime("%Y%m%d%H")
        tasks = self._iter_tasks(run_dt)
        return {
            "run_hour": run_hour,
            "tasks": tasks,
            "product_label": self.filename(),
            "max_fh": self.max_fh,
        }

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        return output_dir / self.name / run_hour / f"{self.name}_{frame_time}_{self.filename()}"


class ModelGroup:
    """Logical grouping of models for side-by-side comparison."""

    def __init__(self, name: str, models: List[ModelConfig]) -> None:
        self.name = name
        self.models = models

WHISTLER_REGIONAL_LOCATION = (608.1, 628.6)
WHISTLER_GLOBAL_LOCATION = (437, 374)
WHISTLER_AC_LOCATION = (907, 477)

MODEL_GROUPS: List[ModelGroup] = [
    ModelGroup(
        name="Regional Precipitation",
        models=[
            AvalancheCanadaModel(name="ac_gdps", product="AC_GDPS_EPA_clds-th-500hts", max_fh=144, step_hours=6, annotate=WHISTLER_AC_LOCATION),
            PivotalWeatherModel(name="rdps", product="prateptype-met", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="nam", product="ref1km_ptype", region="ca_w", hide=True, annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="hrdps", product="prateptype-met", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
        ],
    ),
    ModelGroup(
        name="Global Trending",
        models=[
            PivotalWeatherModel(name="cfs", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="gfs", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="aigfs", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="ecmwf_full", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="ecmwf_aifs", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="gdps", product="500h_anom", region="na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel(name="icon", product="500h_anom", region="na", hide=True, annotate=WHISTLER_GLOBAL_LOCATION),
        ],
    ),
]

# Flattened list for download pipeline compatibility.
MODELS: List[ModelConfig] = [m for g in MODEL_GROUPS for m in g.models]


def download_image(
    url: str,
    dest: Path,
    session: Optional[requests.Session] = None,
    overwrite: bool = False,
) -> bool:
    """Download a single image to dest. Returns True if saved, False otherwise."""
    if dest.exists() and not overwrite:
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    sess = session or requests.Session()
    resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed {resp.status_code} for {url}")

    dest.write_bytes(resp.content)
    return True


def add_red_dot(path: Path, x: int, y: int, radius: int) -> None:
    img = Image.open(path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=(255, 0, 0, 255))

    ext = path.suffix.lower()
    if ext in {".jpg", ".jpeg"}:
        img = img.convert("RGB")  # JPEG does not support alpha channel

    img.save(path)


def download_model_run(
    model: ModelConfig,
    tasks: Iterable[Tuple[str, str, str]],
    output_dir: Path,
    workers: int = 8,
    overwrite: bool = False,
    dot_radius: int = 8,
) -> List[Path]:
    """Download available forecast images for one run using the model's source implementation."""
    saved: List[Path] = []
    coord = model.annotate

    def _task(item: Tuple[str, str, str]) -> Optional[Path]:
        frame_time, url, run_hour = item
        target = model.dest_path(output_dir, run_hour, frame_time)
        try:
            changed = download_image(url, target, overwrite=overwrite)
            if changed and coord is not None:
                add_red_dot(target, x=coord[0], y=coord[1], radius=dot_radius)
            return target if changed else None
        except Exception as exc:  # keep going if one forecast hour fails
            print(f"[{model.name} {run_hour} {frame_time}] error: {exc}")
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_task, tasks):
            if result:
                saved.append(result)
    return saved


def download_all_models(
    models: Iterable[ModelConfig],
    output_dir: Path,
    workers: int = 8,
    overwrite: bool = False,
) -> Dict[str, List[Path]]:
    """Fetch run manifests per model (beta API), pick latest, and download imagery."""
    output: Dict[str, List[Path]] = {}
    with requests.Session() as session:
        model_by_name = {m.name: m for m in models}
        plan_map = {m.name: m.list_forecast_images(session=session) for m in models}

    for model_name, plan in plan_map.items():
        model_cfg = model_by_name[model_name]
        if not plan:
            print(f"No runs available for {model_name}")
            continue

        run_hour = plan.get("run_hour", "")
        max_fh = int(plan.get("max_fh", 0))
        product_label = plan.get("product_label", model_name)
        tasks = plan.get("tasks", [])

        print(
            f"Downloading {model_name} run {run_hour} to {output_dir} "
            f"(product={product_label}, max_fh = {max_fh})"
        )
        saved = download_model_run(
            model=model_cfg,
            tasks=tasks,
            output_dir=output_dir,
            workers=workers,
            overwrite=overwrite,
        )
        output[model_name] = saved
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Pivotal Weather model imagery in parallel (beta API)")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/raw"),
        help="Output directory for downloaded images",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Thread pool size",
    )
    return parser.parse_args()


def _latest_run_dir(model_name: str) -> Optional[Path]:
    model_dir = DATA_DIR / model_name
    if not model_dir.exists():
        return None
    run_dirs = sorted([p for p in model_dir.iterdir() if p.is_dir()], reverse=True)
    return run_dirs[0] if run_dirs else None


def _frames_in_run(model_name: str, run_dir: Path) -> List[Dict[str, object]]:
    frames: List[Dict[str, object]] = []
    for file in sorted(run_dir.iterdir()):
        if not file.is_file():
            continue

        match = FRAME_TS_RE.search(file.stem)
        if not match:
            continue
        frame_time = match.group(1)

        frame_dt = datetime.strptime(frame_time, "%Y%m%d%H")
        timestamp = frame_dt.strftime("%Y%m%d%H")
        frames.append({"timestamp": timestamp, "url": f"data/raw/{model_name}/{run_dir.name}/{file.name}"})

    frames.sort(key=lambda f: f["timestamp"])
    return frames


def build_model_payload(model: ModelConfig) -> Optional[Dict[str, object]]:
    run_dir = _latest_run_dir(model.name)
    if not run_dir:
        return None
    run_name = run_dir.name
    frames = _frames_in_run(model.name, run_dir)
    if not frames:
        return None
    min_ts = min(f.get("timestamp", "") for f in frames)
    max_ts = max(f.get("timestamp", "") for f in frames)
    return {
        "id": f"{model.name}_{model.product}",
        "name": model.name,
        "run": run_name,
        "frames": frames,
        "min_timestamp": min_ts,
        "max_timestamp": max_ts,
        "hide": getattr(model, "hide", False),
    }


def _hours_between(start_ts: str, end_ts: str) -> int:
    start = datetime.strptime(start_ts, "%Y%m%d%H")
    end = datetime.strptime(end_ts, "%Y%m%d%H")
    delta = end - start
    return max(0, int(delta.total_seconds() // 3600))


def build_groups_payload() -> List[Dict[str, object]]:
    groups_payload: List[Dict[str, object]] = []
    for grp in MODEL_GROUPS:
        models_payload: List[Dict[str, object]] = []
        for m in grp.models:
            payload = build_model_payload(m)
            if payload:
                models_payload.append(payload)
        if models_payload:
            group_min_ts = min(m["min_timestamp"] for m in models_payload)
            group_max_ts = max(m["max_timestamp"] for m in models_payload)
            span_hours = _hours_between(group_min_ts, group_max_ts)
            common: Optional[set[str]] = None
            for model in models_payload:
                ts_set = {frame["timestamp"] for frame in model.get("frames", [])}
                common = ts_set if common is None else common & ts_set
                if common is not None and not common:
                    break

            start_ts = min(common) if common else group_min_ts
            groups_payload.append(
                {
                    "name": grp.name,
                    "models": models_payload,
                    "min_timestamp": group_min_ts,
                    "max_timestamp": group_max_ts,
                    "span_hours": span_hours,
                    "start_timestamp": start_ts,
                }
            )
    return groups_payload


def render_viewer_html(groups_payload: List[Dict[str, object]], template_path: Path = VIEWER_TEMPLATE, output_path: Path = VIEWER_RENDERED) -> Path:
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    template = template_path.read_text(encoding="utf-8")
    rendered = template.replace("{{ groups_json|safe }}", json.dumps(groups_payload))
    output_path.write_text(rendered, encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    download_all_models(
        models=MODELS,
        output_dir=args.out,
        workers=args.workers,
        overwrite=False,
    )

    try:
        groups_payload = build_groups_payload()
        render_viewer_html(groups_payload)
        print(f"Rendered viewer to {VIEWER_RENDERED}")
    except FileNotFoundError as exc:
        print(f"Skipping viewer render: {exc}")


if __name__ == "__main__":
    main()
