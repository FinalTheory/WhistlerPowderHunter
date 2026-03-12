"""
Basic framework to fetch model imagery from Pivotal Weather in parallel.
- Queries the status endpoint per model to discover available runs and lead time range.
- Downloads images for a chosen product across all lead times using a thread pool.

This is only the downloading layer. The later workflow (analysis, HTML generation, GPT orchestration)
can build on top of the saved files in the output directory.
"""
import argparse
import traceback
import base64
import concurrent.futures
import json
import os
import shutil
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, NamedTuple
import requests
from PIL import Image, ImageDraw
from openai import OpenAI
import concurrent.futures

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}

BASE_DIR = Path(__file__).parent.resolve()
VIEWER_TEMPLATE = BASE_DIR / "template.html"
LOG_PATH = BASE_DIR / "log.txt"
PROMPT_BASE_DIR = BASE_DIR / "prompt"
INIT_PROMPT_PATH = PROMPT_BASE_DIR / "prompt_init.txt"
TASK_PROMPT_PATH = PROMPT_BASE_DIR / "prompt_task.txt"
FRAME_TS_RE = re.compile(r"(\d{10})")
WHISTLER_REGIONAL_LOCATION = (608.1, 628.6)
WHISTLER_GLOBAL_LOCATION = (437, 374)
WHISTLER_AC_LOCATION = (907, 477)
TIME_ZONE = ZoneInfo("America/Vancouver")
PRODUCT_META = {
    "500h_anom.na": "500mb height abnormality of North America",
    "700wh.ca_w": "700mb Height Wind Speed (kt) of West Canada",
    "prateptype-met.ca_w": "Precipitation Type, 6-h Avg Rate (mm/hr), 1000-500mb thickness (dam) of West Canada",
    "nam.ref1km_ptype.ca_w": "1km AGL Reflectivity(dBZ), Type, 1000-500mb thickness (dam) of West Canada",
    "AC_GDPS_EPA_clds-th-500hts": "Avalanche Canada forecast graphic (PNW area) combining 6-hour precipitation, atmospheric thickness (1000–500 mb as a snow-line proxy), and geopotential height contours to diagnose storm structure and large-scale trends rather than precise snowfall amounts."
}

PROXY = {
    "http":  "socks5h://127.0.0.1:1080",
    "https": "socks5h://127.0.0.1:1080",
}

def log(msg, stdout=True) -> None:
    """Log to console and append to log.txt with UTC timestamp."""
    if stdout:
        print(msg)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    line = f"[{timestamp}] {msg}"
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        # Best-effort; avoid crashing on log failures.
        pass


class RunPlan(NamedTuple):
    run_hour: str
    tasks: List[Tuple[str, str, str]]
    product: str
    max_forecast_hour: int

class ModelConfig(ABC):
    """Abstract base for a single model source."""

    def __init__(self, name: str, product: str, hide: bool, annotate: Optional[Tuple[int, int]] = None) -> None:
        self.name = name
        self.product = product
        self.hide = hide
        self.annotate = annotate
        self._cached_plan: Optional[RunPlan] = None

    @property
    def id(self) -> str:
        return f"{self.name}.{self.product}"

    @property
    def product_dir(self) -> str:
        """Directory name that holds this model's imagery."""
        return self.product

    @abstractmethod
    def list_forecast_images(self):
        """Return a download plan or None if unavailable."""

    @abstractmethod
    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        """Return destination path for a given frame."""

    def select(
        self,
        data_root: Path,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> List[Path]:
        """
        Unified selection API:
        - If end is provided: return all forecast image paths within [start, end] (inclusive).
        - If end is None: return at most one path, the first whose valid time is >= start.
        """
        plan = self.list_forecast_images()
        if not plan:
            return []

        start_utc = self._to_utc(start)
        if end is None:
            for frame_time, _, run_hour in plan.tasks:
                valid_dt = datetime.strptime(frame_time, "%Y%m%d%H").replace(tzinfo=timezone.utc)
                if valid_dt >= start_utc:
                    return [self.dest_path(data_root, run_hour, frame_time)]
            return []

        end_utc = self._to_utc(end)
        if start_utc > end_utc:
            raise ValueError("start must be <= end")

        selected: List[Path] = []
        for frame_time, _, run_hour in plan.tasks:
            valid_dt = datetime.strptime(frame_time, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            if start_utc <= valid_dt <= end_utc:
                selected.append(self.dest_path(data_root, run_hour, frame_time))
        return selected

    @staticmethod
    def _to_utc(dt: datetime) -> datetime:
        """Convert naive/aware datetime to timezone-aware UTC."""
        if dt.tzinfo is None:
            local_tz = datetime.now().astimezone().tzinfo or timezone.utc
            dt = dt.replace(tzinfo=local_tz)
        return dt.astimezone(timezone.utc)

    def get_cached_plan(self) -> Optional[RunPlan]:
        return self._cached_plan

    def set_cached_plan(self, plan: RunPlan) -> None:
        self._cached_plan = plan

    def clear_cached_plan(self) -> None:
        self._cached_plan = None


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

    @property
    def id(self) -> str:
        return f"{self.name}.{self.product}.{self.region}"

    @property
    def product_dir(self) -> str:
        return f"{self.product}.{self.region}"

    @staticmethod
    def proxy_available() -> bool:
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
            return True
        except ImportError:
            return False

    @staticmethod
    def fetch_runs_beta(name: str, product: str, region: str) -> List[Dict]:
        """Fetch run manifests from the beta API for a given model/product/region."""
        sess = requests.Session()
        url = f"{PivotalWeatherModel.BETA_BASE}/models/{name}/{product}/{region}/runs"
        proxies = PROXY if PivotalWeatherModel.proxy_available() else None
        for attempt in range(3):
            try:
                resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=10, proxies=proxies)
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise
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

    def list_forecast_images(self):
        cached = self.get_cached_plan()
        if cached is not None:
            return cached

        runs = self.fetch_runs_beta(self.name, self.product, self.region)
        run = self.run_selector(runs) if runs else None
        if not run:
            return None

        run_hour = run.get("date", "")
        forecasts = run.get("forecasts", {}) or {}
        max_fh = max((int(k) for k, v in forecasts.items() if isinstance(v, dict) and v.get("available")), default=0)
        tasks = list(self._iter_forecast_tasks(run))
        plan = RunPlan(
            run_hour=run_hour,
            tasks=tasks,
            product=f"{self.product}.{self.region}",
            max_forecast_hour=max_fh,
        )
        self.set_cached_plan(plan)
        return plan

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        product_dir = f"{self.product}.{self.region}"
        return output_dir / self.name / run_hour / product_dir / f"{frame_time}.png"


class AvalancheCanadaModel(ModelConfig):
    """Static URL pattern fetcher for Avalanche Canada imagery."""

    BASE = "https://s3-us-west-2.amazonaws.com/mountain-weather-forecast"

    def __init__(
        self,
        name: str,
        product: str,
        max_forecast_hour: int,
        step_hours: int,
        hide: bool = False,
        annotate: Optional[Tuple[int, int]] = None,
    ) -> None:
        super().__init__(name, product, hide, annotate=annotate)
        self.max_forecast_hour = max_forecast_hour
        self.step_hours = step_hours

    def _select_run_dt(self, now: Optional[datetime] = None) -> datetime:
        now_utc = now or datetime.now(timezone.utc)
        hour = now_utc.hour
        run_hour = 12 if hour >= 12 else 0
        return now_utc.replace(hour=run_hour, minute=0, second=0, microsecond=0)

    def _iter_tasks(self, run_dt: datetime) -> List[Tuple[str, str, str]]:
        run_str = run_dt.strftime("%Y%m%d%H")
        day_path = run_dt.strftime("%Y/%m/%d")
        tasks: List[Tuple[str, str, str]] = []
        for fh in range(0, self.max_forecast_hour + self.step_hours, self.step_hours):
            frame_dt = run_dt + timedelta(hours=fh)
            frame_time = frame_dt.strftime("%Y%m%d%H")
            url = f"{self.BASE}/{day_path}/{self.product}_{run_str}_{fh:03d}HR.jpg"
            tasks.append((frame_time, url, run_str))
        return tasks

    def list_forecast_images(self):
        cached = self.get_cached_plan()
        if cached is not None:
            return cached

        sess = requests.Session()

        def _run_available(run_dt: datetime) -> bool:
            tasks = self._iter_tasks(run_dt)
            if not tasks:
                return False
            _, probe_url, _ = tasks[0]
            try:
                resp = sess.get(probe_url, headers=DEFAULT_HEADERS, timeout=10, stream=True)
                if resp.status_code != 200:
                    return False
                return True
            except:
                return False

        run_dt = self._select_run_dt()
        while not _run_available(run_dt):
            log(f"Avalanche Canada model run {run_dt} not available, step back for 12 hrs")
            run_dt -= timedelta(hours=12)

        run_hour = run_dt.strftime("%Y%m%d%H")
        tasks = self._iter_tasks(run_dt)
        plan = RunPlan(
            run_hour=run_hour,
            tasks=tasks,
            product=self.product,
            max_forecast_hour=self.max_forecast_hour,
        )
        self.set_cached_plan(plan)
        return plan

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        product_dir = self.product
        return output_dir / self.name / run_hour / product_dir / f"{frame_time}.jpg"


class ModelGroup:
    """Logical grouping of models for side-by-side comparison."""

    def __init__(self, name: str, models: List[ModelConfig]) -> None:
        self.name = name
        self.models = models

    def __getitem__(self, index):
        return self.models[index]

    def __len__(self) -> int:
        return len(self.models)

    def __iter__(self):
        return iter(self.models)

    def select(
        self,
        data_root: Path,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> List[Path]:
        selected: List[Path] = []
        for model in self.models:
            if not model.hide:
                selected.extend(model.select(data_root, start, end))
        return selected


MODEL_GROUPS: List[ModelGroup] = [
    ModelGroup(
        name="Regional Precipitation",
        models=[
            AvalancheCanadaModel(name="ac_gdps", product="AC_GDPS_EPA_clds-th-500hts", max_forecast_hour=144, step_hours=6, annotate=WHISTLER_AC_LOCATION),
            PivotalWeatherModel(name="rdps", product="prateptype-met", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="hrdps", product="prateptype-met", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="nam", product="ref1km_ptype", region="ca_w", hide=True, annotate=WHISTLER_REGIONAL_LOCATION),
        ],
    ),
    ModelGroup(
        name="Wind Speed",
        models=[
            PivotalWeatherModel(name="rdps", product="700wh", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="nam", product="700wh", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="hrdps", product="700wh", region="ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel(name="gfs", product="700wh", region="ca_w", hide=True, annotate=WHISTLER_REGIONAL_LOCATION),
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


def trunc_to_hour(dt, hour):
    return dt.replace(hour=hour, minute=0, second=0, microsecond=0)


def sort_images_by_valid_time(image_paths: List[Path]) -> List[Path]:
    def _extract_sort_key(p: Path):
        try:
            # Same deterministic parse strategy as image labeling:
            # .../data/<model>/<run>/<product>/<filename>
            rel_parts = p.relative_to(BASE_DIR).parts
            if len(rel_parts) < 5 or rel_parts[0] != "data":
                raise ValueError(f"Unexpected image path layout: {p}")
            model_name = rel_parts[1]
            product_name = rel_parts[3]
            valid_dt = datetime.strptime(Path(rel_parts[4]).stem, "%Y%m%d%H")
            return (0, valid_dt, model_name, product_name, str(p))
        except Exception:
            return (1, datetime.max, "zz_unknown", "zz_unknown", str(p))

    return sorted(list(dict.fromkeys(image_paths)), key=_extract_sort_key)


def select_init_images(data_root):
    now = datetime.now()
    model = MODEL_GROUPS[0][0]
    return model.select(data_root, now, now + timedelta(days=1))[:2] + model.select(data_root, now + timedelta(days=1)) + model.select(data_root, now + timedelta(days=2))


def select_precip_images(data_root):
    start = datetime.now(TIME_ZONE)
    end = trunc_to_hour(start + timedelta(days=1), 15)
    # images from now to tomorrow 15:00
    group1 =  MODEL_GROUPS[0][0].select(data_root, start, end) + MODEL_GROUPS[0][1].select(data_root, start, end) + MODEL_GROUPS[0][2].select(data_root, start, end)[::3]
    # images from tomorrow 18:00 to 12:00 after tomorrow
    start = end.replace(hour=18)
    end = (start + timedelta(days=1)).replace(hour=12)
    group2 =  MODEL_GROUPS[0][0].select(data_root, start, end) + MODEL_GROUPS[0][2].select(data_root, start, end)[::6]
    return group1 + group2


def select_wind_images(data_root):
    start = datetime.now(TIME_ZONE)
    end = trunc_to_hour(start + timedelta(days=1), 15)
    return MODEL_GROUPS[1][0].select(data_root, start, end) + MODEL_GROUPS[1][1].select(data_root, start, end) + MODEL_GROUPS[1][2].select(data_root, start, end)[::3]


def select_pattern_task_images(data_root):
    now = datetime.now(timezone.utc)
    # select avalanche canada images from 3 days later until end, and choose 4 frames
    images = MODEL_GROUPS[0][0].select(data_root, now + timedelta(days=3), now + timedelta(days=30))
    # and background pattern images from 5, 10, 14 days later
    dates = [trunc_to_hour(now + timedelta(days=d), 12) for d in (5, 10, 14)]
    return [p for d in dates for p in MODEL_GROUPS[2].select(data_root, d)] + images[::max(1, len(images) // 3)]


TASK_DEFINITION = {
    "PATTERN_TASK": select_pattern_task_images,
    "PRECIP_EVENT_TASK": select_precip_images,
    "THERMAL_PHASE_TASK": lambda _: [],
    "WIND_OPERATION_TASK": select_wind_images,
}


TASK_SELECTION_JSON_SCHEMA = {
    "name": "task_scheduler_output",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "tasks": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": list(TASK_DEFINITION.keys()),
                },
            },
            "reason": {"type": "string"},
            "debug": {"type": "string"},
        },
        "required": ["tasks", "reason", "debug"],
        "additionalProperties": False,
    },
}


TASK_OUTPUT_JSON_SCHEMA: Dict[str, object] = {
    "name": "task_analysis_output",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "status_code": {
                "type": "string",
                "enum": ["NO_EVENT", "SOME_SNOW", "DRY_POW", "WARM_STORM"],
            },
            "need": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
            "summary": {
                "type": "object",
                "properties": {
                    "en": {"type": "string"},
                    "zh": {"type": "string"},
                },
                "required": ["en", "zh"],
                "additionalProperties": False,
            },
            "debug": {"type": "string"},
        },
        "required": ["status_code", "need", "summary", "debug"],
        "additionalProperties": False,
    },
}


def download_image(
    url: str,
    dest: Path,
    overwrite: bool = False,
) -> bool:
    """Download a single image to dest. Returns True if saved, False otherwise."""
    if dest.exists() and not overwrite:
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    sess = requests.Session()
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
    dot_radius: int = 5,
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
            log(f"[{model.name} {run_hour} {frame_time}] error: {exc}")
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
    model_by_id = {m.id: m for m in models}
    plan_map = {m.id: m.list_forecast_images() for m in models}

    for model_name, plan in plan_map.items():
        model_cfg = model_by_id[model_name]
        if not plan:
            log(f"No runs available for {model_name}")
            continue

        run_hour = plan.run_hour
        max_fh = int(plan.max_forecast_hour)
        product_label = plan.product
        tasks = plan.tasks

        log(
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
        default=BASE_DIR,
        help="Output directory for downloaded images",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Thread pool size",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run once and exit instead of looping every 24h",
    )
    parser.add_argument(
        "--no-gpt",
        action="store_true",
        help="Do not call chatgpt.",
    )
    return parser.parse_args()


def _latest_run_dir(data_root: Path, model_name: str, product_dir: str) -> Optional[Path]:
    model_dir = data_root / model_name
    if not model_dir.exists():
        return None

    def _run_has_images(run_dir: Path) -> bool:
        for product_subdir in (p for p in run_dir.iterdir() if p.is_dir()):
            if product_subdir.name != product_dir:
                continue
            for file in product_subdir.iterdir():
                if file.is_file() and file.suffix.lower() in {".png", ".jpg", ".jpeg"}:
                    return True
        return False

    run_dirs = sorted([p for p in model_dir.iterdir() if p.is_dir()], reverse=True)
    for run_dir in run_dirs:
        if _run_has_images(run_dir):
            return run_dir
    return None


def _frames_in_run(run_dir: Path, data_root: Path, product_dir: str) -> List[Dict[str, object]]:
    frames: List[Dict[str, object]] = []
    for product_subdir in sorted(p for p in run_dir.iterdir() if p.is_dir()):
        if product_subdir.name != product_dir:
            continue
        for file in sorted(product_subdir.iterdir()):
            if not file.is_file():
                continue

            match = FRAME_TS_RE.search(file.stem)
            if not match:
                continue
            frame_time = match.group(1)

            frame_dt = datetime.strptime(frame_time, "%Y%m%d%H")
            timestamp = frame_dt.strftime("%Y%m%d%H")
            rel_path = file.relative_to(data_root)
            frames.append({"timestamp": timestamp, "url": str(Path("data") / rel_path)})

    frames.sort(key=lambda f: f["timestamp"])
    return frames


def build_model_payload(model: ModelConfig, data_root: Path) -> Optional[Dict[str, object]]:
    product_dir = model.product_dir
    run_dir = _latest_run_dir(data_root, model.name, product_dir)
    if not run_dir:
        return None
    run_name = run_dir.name
    frames = _frames_in_run(run_dir, data_root, product_dir)
    if not frames:
        return None
    min_ts = min(f.get("timestamp", "") for f in frames)
    max_ts = max(f.get("timestamp", "") for f in frames)
    return {
        "id": model.id,
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


def build_groups_payload(data_root: Path) -> List[Dict[str, object]]:
    groups_payload: List[Dict[str, object]] = []
    for grp in MODEL_GROUPS:
        models_payload: List[Dict[str, object]] = []
        for m in grp.models:
            payload = build_model_payload(m, data_root)
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


def build_data_inventory(data_root: Path) -> Dict[str, object]:
    inventory: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for model in MODELS:
        run_dir = _latest_run_dir(data_root, model.name, model.product_dir)
        if not run_dir:
            continue

        run_payload: Dict[str, List[str]] = {}
        for product_dir in sorted(p for p in run_dir.iterdir() if p.is_dir()):
            files = [f.name for f in sorted(product_dir.iterdir()) if f.is_file()]
            if files:
                run_payload[product_dir.name] = files

        if not run_payload:
            continue

        model_entry = inventory.setdefault(model.name, {})
        model_entry[run_dir.name] = run_payload

    return inventory


def format_generated_at_pst() -> str:
    now_pst = datetime.now(timezone.utc).astimezone(TIME_ZONE)
    return now_pst.strftime("%Y-%m-%d %H:%M %Z")


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


def prune_old_runs(root: Path, days: int = 2) -> None:
    """Delete run directories older than N days (based on run_hour dir name)."""
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


def fetch_sensor_data():
    LOCATIONS = ['horstman', 'peak', 'symphony', 'harmony', 'roundhouse', 'rendezvous', 'crystal']
    SENSOR_URLS = [f'https://whistlerpeak.com/temps/plot-{loc}.json' for loc in LOCATIONS]

    def fetch_one(url):
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def transform(data):
        result = []
        cnt = len(data['date'])
        date = data['date']
        temp = data['temp']
        winddir = data['winddir']
        direction = data['direction']
        maxwind = data['maxwind']
        avgwind = data['avgwind']
        for i in range(cnt):
            result.append(f"{date[i]} Temperature={temp[i]}C Wind Direction={winddir[i]} ({direction[i]}degree), Max Wind={maxwind[i]}km/h Avg Wind={avgwind[i]}km/h")
        return result
    sensor_data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        future_to_loc = {executor.submit(fetch_one, url): loc for url, loc in zip(SENSOR_URLS, LOCATIONS)}
        for future in concurrent.futures.as_completed(future_to_loc):
            loc = future_to_loc[future]
            try:
                data = future.result()
                sensor_data[loc] = transform(data)
            except Exception as exc:
                log(f"Sensor fetch failed: {loc}: {exc}")

    return sensor_data


def fetch_rwdi_forecast(url: str = "https://www.whistlerpeak.com/forecast/block-alpine-grid.php") -> Optional[Dict[str, object]]:
    headers = dict(DEFAULT_HEADERS)
    headers["referer"] = "https://www.whistlerpeak.com/"
    headers["accept"] = "text/html"
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"RWDI forecast fetch failed: status {resp.status_code}")

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")

    synopsis_text: Optional[str] = None
    summary = soup.select_one("#summaryContent, .summaryContent, .summaryContent__body")
    if summary:
        parts = [el.get_text(" ", strip=True) for el in summary.find_all(["p", "div", "span"])]
        merged = " ".join(p for p in parts if p)
        merged = re.sub(r"\s+", " ", merged).strip()
        synopsis_text = merged or None

    days: List[Dict[str, str]] = []
    for card in soup.select(".alpine__card"):
        heading = card.find(["h3", "h4"]) or card.select_one(".alpine__card__title")
        name = heading.get_text(" ", strip=True) if heading else ""
        paragraphs = [p.get_text(" ", strip=True) for p in card.find_all("p")]
        text = " ".join(p for p in paragraphs if p)
        text = re.sub(r"\s+", " ", text).strip()
        if name or text:
            days.append({"day": name, "text": text})

    if not days and synopsis_text is None:
        raise RuntimeError("RWDI forecast parse failure: no synopsis or day cards found")

    return {"synopsis": synopsis_text, "days": days}


def build_forecast_task_prompt(model_data):
    with TASK_PROMPT_PATH.open("r", encoding="utf-8") as p:
        return p.read().replace("{{MODEL_DATA}}", str(model_data)).replace("{{PRODUCT_META}}", str(PRODUCT_META)).replace("{{SENSOR_DATA}}", str(fetch_sensor_data()))


def build_forecast_init_input(rwdi_forecast):
    return f"""
    INIT INFORMATION

Now, we can start with given RWDI forecast information:

{rwdi_forecast}

Here are forecast images from Avalanche Canada.
"""


class ChatGPTSession:
    """Stateful helper to talk to ChatGPT with images while retaining context."""

    def __init__(self, model, data_root: Path) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.model = model
        self.messages: List[Dict[str, object]] = []
        self.token_used = 0
        self.token_limit = 300000

    def append_prompt(self, prompt: str):
        self.messages.append({"role": "system", "content": prompt})

    def append(self, message: str, image_paths: Optional[List[Path]] = None) -> None:
        images = sort_images_by_valid_time(image_paths) or []

        user_content = []
        user_content.append({"type": "text", "text": message})

        for img in images:
            if not img.exists():
                log(f"Image {img} not found, skip it.")
                continue

            try:
                # Build a deterministic image label from local path convention:
                # .../data/<model>/<run>/<product>/<valid_time>
                rel_parts = img.relative_to(BASE_DIR).parts
                if len(rel_parts) < 5 or rel_parts[0] != "data":
                    raise ValueError(f"Unexpected image path layout: {img}")
                model_name = rel_parts[1]
                run_time = rel_parts[2]
                product_key = rel_parts[3]
                valid_time = Path(rel_parts[4]).stem
                product_desc = PRODUCT_META.get(product_key)
                label_lines = [
                    f"Model: {model_name.upper()}",
                    f"Product: {product_key}",
                    f"Valid UTC: {valid_time}",
                    f"Run UTC: {run_time}",
                    f"Image Path: {'/'.join(rel_parts)}",
                ]
                if product_desc:
                    label_lines.append(f"Product Description: {product_desc}")
                label_lines.append("Note: Whistler location is marked by a red dot on the image.")
                user_content.append({"type": "text", "text": "\n".join(label_lines)})

            except Exception:
                log(f"Image metadata parse failed, skip this image: {img}")

            mime = "image/png"
            suffix = img.suffix.lower()
            if suffix in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            img_b64 = base64.b64encode(img.read_bytes()).decode("ascii")
            user_content.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}})

        self.messages.append({"role": "user", "content": user_content})

    def send(
        self,
        json_schema: Optional[Dict[str, object]] = None,
        max_retries: int = 3,
    ) -> Dict[str, object]:
        client = OpenAI(api_key=self.api_key)
        last_error = None

        for _ in range(max_retries):
            if self.token_used > self.token_limit:
                raise RuntimeError(f"Token budget exceeded: used {self.token_used} > limit {self.token_limit}")
            request_kwargs: Dict[str, object] = {
                "model": self.model,
                "messages": self.messages,
            }
            if json_schema is not None:
                request_kwargs["response_format"] = {
                    "type": "json_schema",
                    "json_schema": json_schema,
                }
            resp = client.chat.completions.create(**request_kwargs)
            choices = resp.choices or []
            reply = choices[0].message.content if choices else None
            if reply is None:
                last_error = ValueError("Empty reply from assistant")
                continue
            usage = getattr(resp, "usage", None) or {}
            tokens = getattr(usage, "total_tokens", None) or usage.get("total_tokens") if isinstance(usage, dict) else None
            if isinstance(tokens, int):
                self.token_used += tokens
            try:
                parsed = json.loads(reply)
                self.messages.append({"role": "assistant", "content": reply})
                return parsed
            except Exception as exc:
                last_error = exc
                continue

        raise RuntimeError("Failed to obtain valid JSON response") from last_error

    def history(self) -> List[Dict[str, object]]:
        """Return the current message history."""
        return list(self.messages)

    def dump_to(self, file_name):
        def transform(msg):
            if msg["role"] == 'user':
                for c in msg["content"]:
                    if c["type"] == "image_url":
                        c["image_url"]["url"] = ""
                return msg
            return msg
        messages = [transform(m) for m in self.messages if m]
        with open(file_name, 'w') as f:
            f.write(json.dumps(messages, indent=2))


def call_chatgpt_analysis(args: argparse.Namespace, data_root: Path) -> Dict[str, str]:
    init_images = select_init_images(data_root)
    gpt = ChatGPTSession("gpt-5.2", data_root=data_root)
    gpt.append_prompt(INIT_PROMPT_PATH.open("r", encoding="utf-8").read())
    gpt.append(build_forecast_init_input(fetch_rwdi_forecast()), image_paths=init_images)
    if not args.no_gpt:
        response = gpt.send(json_schema=TASK_SELECTION_JSON_SCHEMA)
    else:
        gpt.dump_to("init.txt")
        response = {
            "tasks": list(TASK_DEFINITION.keys())
        }
    tasks = response.get("tasks", [])
    reason = response.get("reason", "")
    debug = response.get("debug")
    log(f"Tasks = {tasks}")
    log(f"Reason: {reason}")
    if debug:
        log(debug)
    gpt.append_prompt(build_forecast_task_prompt(build_data_inventory(data_root)))
    for task in tasks:
        prompt_path = PROMPT_BASE_DIR / f"{task}.txt"
        try:
            get_images = TASK_DEFINITION[task]
            with open(prompt_path, "r") as f:
                gpt.append(f.read(), image_paths=get_images(data_root))
        except Exception as e:
            log(str(e))
    if not args.no_gpt:
        response = gpt.send(json_schema=TASK_OUTPUT_JSON_SCHEMA)
    else:
        gpt.dump_to("task.txt")
        response = {}
    log("status_code=" + str(response.get("status_code")))
    log(response.get("need", []))
    log(f"Used tokens: {gpt.token_used}")
    debug = response.get("debug")
    if debug:
        log(debug)
    return response.get("summary", {})


def run_once(args: argparse.Namespace) -> None:
    data_root = args.out / "data"
    for model in MODELS:
        model.clear_cached_plan()

    prune_old_runs(data_root, days=2)
    download_all_models(
        models=MODELS,
        output_dir=data_root,
        workers=args.workers,
        overwrite=False,
    )
    summary = call_chatgpt_analysis(args, data_root)
    index_path = args.out / "index.html"
    render_viewer_html(build_groups_payload(data_root), summary, format_generated_at_pst(), index_path)
    log(f"Rendered to {index_path}")


def seconds_until_next_run(now_utc: Optional[datetime] = None) -> int:
    now = (now_utc or datetime.now(timezone.utc)).astimezone(TIME_ZONE)
    target = now.replace(hour=18, minute=0, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


def main() -> None:
    args = parse_args()
    if args.debug:
        run_once(args)
        return

    while True:
        try:
            run_once(args)
        except Exception as e:
            err = traceback.format_exc()
            log(err)
        sleep_seconds = seconds_until_next_run()
        next_run = datetime.now(timezone.utc).astimezone(TIME_ZONE) + timedelta(seconds=sleep_seconds)
        log(f"Next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M %Z')} ({sleep_seconds}s)")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
