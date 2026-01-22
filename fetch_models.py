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
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple
import requests

DEFAULT_BETA_BASE = "https://beta.pivotalweather.com/api"
DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}


def clean_output_dir(out_dir: Path) -> None:
    """Remove existing downloaded images so each run starts fresh."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


class ModelConfig:
    """Configuration for a single model source."""

    def __init__(
        self,
        name: str,
        product: str,
        region: str,
        separator: str = ".",
        run_selector = None,
    ) -> None:
        self.name = name
        self.product = product
        self.region = region
        self.separator = separator
        self.run_selector = run_selector

    def filename(self) -> str:
        return f"{self.product}{self.separator}{self.region}.png"


class ModelGroup:
    """Logical grouping of models for side-by-side comparison."""

    def __init__(self, name: str, models: List[ModelConfig]) -> None:
        self.name = name
        self.models = models


# Central place to pick which models to fetch and their default products (beta API).
MODEL_GROUPS: List[ModelGroup] = [
    ModelGroup(
        name="Regional Precipitation",
        models=[
            # https://m2o.pivotalweather.com/maps/models/rdps/2026012200/006/prateptype-met.ca_w.png
            ModelConfig(name="rdps", product="prateptype-met", region="ca_w"),
            # https://m1l.pivotalweather.com/maps/models/nam/2026012200/006/ref1km_ptype.ca_w.png
            ModelConfig(name="nam", product="ref1km_ptype", region="ca_w"),
            ModelConfig(name="hrdps", product="prateptype-met", region="ca_w"),
        ],
    ),
    ModelGroup(
        name="Global Trending",
        models=[
            ModelConfig(name="cfs", product="500h_anom", region="na"),
            ModelConfig(name="gfs", product="500h_anom", region="na"),
            ModelConfig(name="aigfs", product="500h_anom", region="na"),
            ModelConfig(name="ecmwf_full", product="500h_anom", region="na"),
            ModelConfig(name="ecmwf_aifs", product="500h_anom", region="na"),
            ModelConfig(name="gdps", product="500h_anom", region="na"),
        ],
    )
]

# Flattened list for download pipeline compatibility.
MODELS: List[ModelConfig] = [m for g in MODEL_GROUPS for m in g.models]


def fetch_runs_beta(model: ModelConfig, session: Optional[requests.Session] = None) -> List[Dict]:
    """Fetch run manifests from the beta API for a given model/product/region."""
    sess = session or requests.Session()
    url = f"{DEFAULT_BETA_BASE}/models/{model.name}/{model.product}/{model.region}/runs"
    resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("error"):
        return []
    return payload.get("data", {}).get("runs", [])


def latest_run(runs: List[Dict]) -> Optional[Dict]:
    """Pick the newest run by date string value."""
    if not runs:
        return None
    return sorted(runs, key=lambda r: r.get("date", ""), reverse=True)[0]


def longest_run(runs: List[Dict]) -> Optional[Dict]:
    """Pick the run with the most available forecast hours (break ties by date)."""
    if not runs:
        return None

    def _available_count(run: Dict) -> int:
        forecasts = run.get("forecasts", {}) or {}
        return sum(1 for v in forecasts.values() if isinstance(v, dict) and v.get("available"))

    return sorted(runs, key=lambda r: (_available_count(r), r.get("date", "")), reverse=True)[0]


def build_layer_url(server: str, src: str) -> str:
    """Construct full URL from server code and src path provided by beta API."""
    src_clean = src.lstrip("/")
    return f"https://{server}.pivotalweather.com/{src_clean}"


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


def download_model_run(
    model: ModelConfig,
    run_manifest: Dict,
    output_dir: Path,
    workers: int = 8,
    overwrite: bool = True,
) -> List[Path]:
    """Download available forecast images for one run using beta API manifest."""
    saved: List[Path] = []
    product_filename = model.filename()
    run_hour = run_manifest.get("date", "")
    forecasts = run_manifest.get("forecasts", {}) or {}

    tasks: List[Tuple[int, str]] = []  # (fhr, url)
    for fhr_str, fcst in forecasts.items():
        if not isinstance(fcst, dict):
            continue
        if not fcst.get("available"):
            continue
        layers = fcst.get("layers") or []
        if not layers:
            continue
        layer = layers[0]
        src = layer.get("src")
        server = layer.get("server")
        if not src or not server:
            continue
        try:
            fhr = int(fhr_str)
        except ValueError:
            continue
        tasks.append((fhr, build_layer_url(server, src)))

    tasks.sort(key=lambda t: t[0])

    def _task(item: Tuple[int, str]) -> Optional[Path]:
        fhr, url = item
        target = output_dir / model.name / run_hour / f"{model.name}_{run_hour}_f{fhr:03d}_{product_filename}"
        try:
            changed = download_image(url, target, overwrite=overwrite)
            return target if changed else None
        except Exception as exc:  # keep going if one forecast hour fails
            print(f"[{model.name} {run_hour} f{fhr}] error: {exc}")
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
    overwrite: bool = True,
) -> Dict[str, List[Path]]:
    """Fetch run manifests per model (beta API), pick latest, and download imagery."""
    output: Dict[str, List[Path]] = {}
    with requests.Session() as session:
        runs_map = {m.name: fetch_runs_beta(m, session=session) for m in models}

    model_by_name = {m.name: m for m in models}
    for model_name, runs in runs_map.items():
        model_cfg = model_by_name[model_name]
        selector = model_cfg.run_selector or longest_run
        run = selector(runs)
        if not run:
            print(f"No runs available for {model_name}")
            continue

        run_hour = run.get("date", "")
        forecasts = run.get("forecasts", {}) or {}
        max_fh = max((int(k) for k, v in forecasts.items() if isinstance(v, dict) and v.get("available")), default=0)

        print(
            f"Downloading {model_name} run {run_hour} to {output_dir} "
            f"(product={model_cfg.filename()}, max_fh = {max_fh})"
        )
        saved = download_model_run(
            model=model_cfg,
            run_manifest=run,
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


def main() -> None:
    args = parse_args()
    clean_output_dir(args.out)
    download_all_models(
        models=MODELS,
        output_dir=args.out,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
