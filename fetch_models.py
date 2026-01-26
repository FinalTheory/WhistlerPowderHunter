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
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, NamedTuple
import requests
from PIL import Image, ImageDraw
from openai import OpenAI

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}

BASE_DIR = Path(__file__).parent.resolve()
VIEWER_TEMPLATE = BASE_DIR / "viewer.html"
LOG_PATH = BASE_DIR / "log.txt"
FRAME_TS_RE = re.compile(r"(\d{10})")
WHISTLER_REGIONAL_LOCATION = (608.1, 628.6)
WHISTLER_GLOBAL_LOCATION = (437, 374)
WHISTLER_AC_LOCATION = (907, 477)
PRODUCT_META = {
    "500h_anom.na": "500mb height abnormality of North America",
    "prateptype-met.ca_w": "Precipitation Type, 6-h Avg Rate (mm/hr), 1000-500mb thickness (dam) of West Canada",
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

    @abstractmethod
    def list_forecast_images(self, session: Optional[requests.Session] = None):
        """Return a download plan or None if unavailable."""

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
        try:
            resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=10, proxies=PROXY)
        except:
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

    def list_forecast_images(self, session: Optional[requests.Session] = None):
        runs = self.fetch_runs_beta(self.name, self.product, self.region, session=session)
        run = self.run_selector(runs) if runs else None
        if not run:
            return None

        run_hour = run.get("date", "")
        forecasts = run.get("forecasts", {}) or {}
        max_fh = max((int(k) for k, v in forecasts.items() if isinstance(v, dict) and v.get("available")), default=0)
        tasks = list(self._iter_forecast_tasks(run))
        return RunPlan(
            run_hour=run_hour,
            tasks=tasks,
            product=f"{self.product}.{self.region}",
            max_forecast_hour=max_fh,
        )

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

    def list_forecast_images(self, session: Optional[requests.Session] = None):
        sess = session or requests.Session()

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
        return RunPlan(
            run_hour=run_hour,
            tasks=tasks,
            product=self.product,
            max_forecast_hour=self.max_forecast_hour,
        )

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        product_dir = self.product
        return output_dir / self.name / run_hour / product_dir / f"{frame_time}.jpg"


class ModelGroup:
    """Logical grouping of models for side-by-side comparison."""

    def __init__(self, name: str, models: List[ModelConfig]) -> None:
        self.name = name
        self.models = models


MODEL_GROUPS: List[ModelGroup] = [
    ModelGroup(
        name="Regional Precipitation",
        models=[
            AvalancheCanadaModel(name="ac_gdps", product="AC_GDPS_EPA_clds-th-500hts", max_forecast_hour=144, step_hours=6, annotate=WHISTLER_AC_LOCATION),
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
    with requests.Session() as session:
        model_by_name = {m.name: m for m in models}
        plan_map = {m.name: m.list_forecast_images(session=session) for m in models}

    for model_name, plan in plan_map.items():
        model_cfg = model_by_name[model_name]
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


def _latest_run_dir(data_root: Path, model_name: str) -> Optional[Path]:
    model_dir = data_root / model_name
    if not model_dir.exists():
        return None

    def _run_has_images(run_dir: Path) -> bool:
        for product_dir in (p for p in run_dir.iterdir() if p.is_dir()):
            for file in product_dir.iterdir():
                if file.is_file() and file.suffix.lower() in {".png", ".jpg", ".jpeg"}:
                    return True
        return False

    run_dirs = sorted([p for p in model_dir.iterdir() if p.is_dir()], reverse=True)
    for run_dir in run_dirs:
        if _run_has_images(run_dir):
            return run_dir
    return None


def _frames_in_run(run_dir: Path, data_root: Path) -> List[Dict[str, object]]:
    frames: List[Dict[str, object]] = []
    for product_dir in sorted(p for p in run_dir.iterdir() if p.is_dir()):
        for file in sorted(product_dir.iterdir()):
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
    run_dir = _latest_run_dir(data_root, model.name)
    if not run_dir:
        return None
    run_name = run_dir.name
    frames = _frames_in_run(run_dir, data_root)
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
        run_dir = _latest_run_dir(data_root, model.name)
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


def render_viewer_html(groups_payload: List[Dict[str, object]], synopsis: Dict[str, str], output_path: Path) -> Path:
    if not VIEWER_TEMPLATE.exists():
        raise FileNotFoundError(f"Template not found: {VIEWER_TEMPLATE}")

    template = VIEWER_TEMPLATE.read_text(encoding="utf-8")
    rendered = template.replace("{{ groups_json|safe }}", json.dumps(groups_payload))
    rendered = rendered.replace("{{ synopsis_text_zh|safe }}", synopsis.get("zh", "NO Chinese Data"))
    rendered = rendered.replace("{{ synopsis_text_en|safe }}", synopsis.get("en", "NO DATA"))
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


def build_forecast_prompt(model_data):
    return f"""
You are a meteorology-focused decision engine for ski conditions in the Whistler region (PNW coastal mountains).

Given the available input information, you must execute the forecast-related task specified in the `TASK DEFINITION` section below.
Once the task is complete, summarize the analysis in the `summary` field of the output JSON, following the schema defined in the `OUTPUT DEFINITION` section.

Your analysis must adhere to the following rules:

- Produce TWO output versions: one in English and one in Chinese.
- Both versions must be filled into the format defined in OUTPUT DEFINITION.
- In the Chinese version, English technical terms may be used when necessary.
- Format the final output as HTML fragments suitable for direct embedding into an existing HTML document.
  Do NOT include document-level tags such as <html>, <head>, <body>, or <doctype>.
- Include a "TL; DR" section at the top, written for readers without a meteorology background, followed by a more detailed explanation.
- Use ONLY the provided inputs (forecast images and associated metadata).
  If required information is missing, request the minimum additional set of images necessary to proceed.
- The output MUST be valid JSON matching the schema provided by the user.
  Do NOT include any extra text outside the JSON object.
- Focus on trends and decision-relevant signals, including:
  - Detection of precipitation events,
  - Snowline / freezing level trends,
  - Wind impacts and alpine lift operation risk,
  - Timing and distribution of the strongest snowfall.
- Do NOT provide avalanche safety advice.
  You may note that upside-down snow or wind loading can increase in-bound avalanche risk, but do not offer backcountry travel recommendations.
- Keep uncertainty explicit.
  Provide an overall confidence level using LOW, MED, or HIGH.
- When uncertainty is high, request the smallest additional set of images required to reduce uncertainty.
- At any step, if requested images are insufficient to reach a final decision, you may request further images.
- The location of the Whistler area is marked on ALL forecast images with a red dot; use this marker for spatial reference.

======

DATASET DEFINITION

The JSON data below defines the available forecast model image data.
It is a three-level nested dictionary, where the keys represent, in order:
model → run_time → product → [filename, ...].

Each filename corresponds to the valid forecast time in UTC.

When requesting a specific image file, construct the file path using the following rule:

`data/<model>/<run_time>/<product>/<filename>`

where data is the root data directory prefix.

{model_data}

A separate product dictionary is provided to describe the physical meaning and interpretation of each product.
Use this information to correctly interpret the images and to decide which images are required for further analysis.

{PRODUCT_META}

Do not assume the content of any image unless it is explicitly requested and provided.
Use the JSON index to determine which models, runs, products, and valid times are available.
When additional images are required, explicitly request them by constructing the file path and return them in response.

======

OUTPUT DEFINITION

You are an analysis component in a weather-forecast workflow.

You MUST output ONLY ONE single valid JSON object.
Do NOT include explanations, markdown, or extra text.
The output MUST start with '{' and end with '}'.

The response format is strictly defined as:

{{
  "status_code": "NO_EVENT | DRY_POW | WARM_STORM",
  "need": ["data/<model>/<run_time>/<product>/<filename>", ...],
  "summary": {{
    "en": "analysis result in English...",
    "zh": "analysis result in Chinese..."
  }}
}}

Rules:

1. Exactly ONE of "need" or "summary" MUST be present.
   - If "summary" is present, "need" MUST be omitted.
   - If "need" is present and non-empty, "summary" MUST be omitted.

2. Use "need" ONLY when additional image data is required to continue the analysis.
   - Each entry in "need" MUST be a valid image path string.
   - Paths MUST follow the rule mentioned above.

3. Use "summary" ONLY when the analysis is complete and no further images are required.
   - summary.en must contain the analysis result in English.
   - summary.zh must contain the analysis result in Chinese.

4. The "status_code" MUST be chosen based on the final interpretation:
   - "NO_EVENT": no meaningful snowfall or ski-relevant event expected.
   - "DRY_POW": cold storm or snowfall likely to produce dry, skiable powder.
   - "WARM_STORM": warm system, high snow line, rain, or snow-quality degradation likely.

5. Do NOT assume the content of any image unless it has been explicitly requested and provided.

======

TASK DEFINITION

------

Task: NO_SNOW

Condition:
This task is triggered only when there is high confidence that no meaningful snowfall or snow-producing precipitation is expected in the short term (e.g., next 3-7 days).

Inputs:
Large-scale circulation diagnostics, primarily 500mb height and 500mb height anomaly sequences from multiple global and regional models (e.g., ECMWF, ECMWF-AIFS, GFS, ICON, GDPS, CFS), focused on the PNW / Whistler region.

Goal:
Assess whether the current synoptic-scale background favors a snow-free regime and whether that regime is likely to persist.
Specifically:
- Classify the current background state as RIDGE, TROUGH, or TRANSITION.
- Evaluate background pattern stability over the next 7–14 days.
- Identify whether there are credible signals of a pattern change.
- Estimate a model agreement score in the levels of LOW - MED - HIGH.

Heuristics:
- Use 500mb height anomalies as the primary diagnostic for background state and trend.
- Persistent positive height anomalies over the PNW indicate a RIDGE; persistent negative anomalies indicate a TROUGH.
- Weakening anomalies, displacement, or sign changes indicate a TRANSITION.
- For the 7–14 day window:
  - STABLE: anomaly pattern remains coherent with little positional or amplitude change.
  - UNSTABLE: increasing spread, weakening, or loss of coherence.
  - SHIFTING: systematic displacement, erosion, or sign reversal.
- Look for pattern change signals, including:
  - Sustained height falls or anomaly weakening over the PNW.
  - Upstream trough progression capable of eroding an existing ridge.
  - Breakdown of blocking structures in extended-range guidance.
- Model agreement should account for:
  - Consistency of anomaly sign and placement across models.
  - Consistency of trend direction through time.
  - The number of models available at a given valid time.
- IMPORTANT: Models have different maximum forecast horizons.
  - Prefer valid times with the greatest multi-model overlap.
  - Do not penalize shorter-range models; instead reduce confidence when fewer models are available at longer lead times.
- If lead time exceeds ~7 days, cap confidence unless multiple independent models show consistent evolution.

Interpretation guidance:
This task is diagnostic and trend-focused.
Prioritize large-scale structure, persistence, and evolution over precise timing.
Use very long-range models only to support or weaken confidence in pattern persistence, not as deterministic triggers.

Return intent:
Determine whether the background supports a NO_SNOW regime and summarize pattern state, stability, pattern-change likelihood, and model agreement.
If uncertainty is dominated by missing key time slices or missing model perspectives, indicate which additional large-scale fields would most reduce uncertainty.

"""


def build_forecast_init_input(rwdi_forecast):
    return f"""
    INIT INFORMATION

Now, we can start with given RWDI forecast information:

{rwdi_forecast}

Here are forecast images from Avalanche Canada:

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
        self.messages.append({"role": "system", "content": build_forecast_prompt(build_data_inventory(data_root))})

    def send(self, message: str, image_paths: Optional[List[Path]] = None, max_retries: int = 3) -> Dict[str, object]:
        images = image_paths or []

        for img in images:
            if not img.exists():
                raise FileNotFoundError(f"Image not found: {img}")

        user_content = []
        user_content.append({"type": "text", "text": message})

        for img in images:
            mime = "image/png"
            suffix = img.suffix.lower()
            if suffix in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            img_b64 = base64.b64encode(img.read_bytes()).decode("ascii")
            user_content.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}})

        self.messages.append({"role": "user", "content": user_content})

        client = OpenAI(api_key=self.api_key)
        last_error = None

        for _ in range(max_retries):
            if self.token_used > self.token_limit:
                raise RuntimeError(f"Token budget exceeded: used {self.token_used} > limit {self.token_limit}")
            resp = client.chat.completions.create(model=self.model, messages=self.messages)
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


def run_once(args: argparse.Namespace) -> None:
    data_root = args.out / "data"
    prune_old_runs(data_root, days=2)
    download_all_models(
        models=MODELS,
        output_dir=data_root,
        workers=args.workers,
        overwrite=False,
    )
    summary = {}
    if not args.no_gpt:
        gpt = ChatGPTSession("gpt-5.2", data_root=data_root)
        response = gpt.send(build_forecast_init_input(fetch_rwdi_forecast()))
        summary = response.get("summary", {})
        while not summary:
            log(response)
            needs = response["need"]
            abs_images = [args.out / Path(p) for p in needs]
            response = gpt.send("Here are the requested images:\n{}".format("\n".join(needs)), abs_images)
            summary = response.get("summary", {})
            log(f"Used tokens: f{gpt.token_used}")

    index_path = args.out / "index.html"
    render_viewer_html(build_groups_payload(data_root), summary, index_path)
    log(f"Rendered to {index_path}")


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
        log("Now sleep...")
        time.sleep(24 * 3600)


if __name__ == "__main__":
    main()
