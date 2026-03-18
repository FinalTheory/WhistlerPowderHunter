import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, NamedTuple, Optional, Tuple

import requests

from context.constant import (
    DEFAULT_HEADERS,
    PROXY,
    WHISTLER_AC_LOCATION,
    WHISTLER_GLOBAL_LOCATION,
    WHISTLER_REGIONAL_LOCATION,
    WHISTLER_SATELLITE_LOCATION
)
from context.util import log, parse_utc_timestamp


class RunPlan(NamedTuple):
    run_hour: str
    tasks: List[Tuple[str, str, str]]
    product: str
    max_forecast_hour: int


class ModelConfig(ABC):
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
        return self.product

    @abstractmethod
    def list_forecast_images(self):
        pass

    @abstractmethod
    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        pass

    def select(self, data_root: Path, start: datetime, end: Optional[datetime] = None) -> List[Path]:
        plan = self.list_forecast_images()
        if not plan:
            return []

        start_utc = self._to_utc(start)
        if end is None:
            for frame_time, _, run_hour in plan.tasks:
                valid_dt = parse_utc_timestamp(frame_time).replace(tzinfo=timezone.utc)
                if valid_dt >= start_utc:
                    return [self.dest_path(data_root, run_hour, frame_time)]
            return []

        end_utc = self._to_utc(end)
        if start_utc > end_utc:
            raise ValueError("start must be <= end")

        selected: List[Path] = []
        for frame_time, _, run_hour in plan.tasks:
            valid_dt = parse_utc_timestamp(frame_time).replace(tzinfo=timezone.utc)
            if start_utc <= valid_dt <= end_utc:
                selected.append(self.dest_path(data_root, run_hour, frame_time))
        return selected

    @staticmethod
    def _to_utc(dt: datetime) -> datetime:
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
    BETA_BASE = "https://beta.pivotalweather.com/api"

    def __init__(
        self,
        name: str,
        product: str,
        region: str,
        hide: bool = False,
        run_selector=None,
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
            from urllib3.contrib.socks import SOCKSProxyManager  # noqa: F401
            return True
        except ImportError:
            return False

    @staticmethod
    def fetch_runs_beta(name: str, product: str, region: str) -> List[Dict]:
        sess = requests.Session()
        url = f"{PivotalWeatherModel.BETA_BASE}/models/{name}/{product}/{region}/runs"
        proxies = PROXY if PivotalWeatherModel.proxy_available() else None
        for attempt in range(3):
            try:
                resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=10, proxies=proxies)
                break
            except Exception:
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data")
        if payload.get("error"):
            raise ValueError(f"API error for {name}/{product}/{region}: {payload.get('error')}")
        if data is None or not isinstance(data, dict) or "runs" not in data:
            raise ValueError(f"Invalid API response for {name}/{product}/{region}")
        runs = data["runs"]
        if not isinstance(runs, list):
            raise ValueError(f"'runs' is not a list for {name}/{product}/{region}")
        return runs

    @staticmethod
    def latest_run(runs: List[Dict]) -> Optional[Dict]:
        return sorted(runs, key=lambda r: r.get("date", ""), reverse=True)[0] if runs else None

    @staticmethod
    def longest_run(runs: List[Dict]) -> Optional[Dict]:
        if not runs:
            return None

        def available_count(run: Dict) -> int:
            forecasts = run.get("forecasts", {}) or {}
            return sum(1 for v in forecasts.values() if isinstance(v, dict) and v.get("available"))

        return sorted(runs, key=lambda r: (available_count(r), r.get("date", "")), reverse=True)[0]

    @staticmethod
    def build_layer_url(server: str, src: str) -> str:
        return f"https://{server}.pivotalweather.com/{src.lstrip('/')}"

    def _iter_forecast_tasks(self, run_manifest: Dict) -> Iterable[Tuple[str, str, str]]:
        run_hour = run_manifest.get("date", "")
        forecasts = run_manifest.get("forecasts")
        if forecasts is None or not isinstance(forecasts, dict):
            raise ValueError("Run manifest missing 'forecasts' dict")

        run_dt = datetime.strptime(run_hour, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        tasks: List[Tuple[str, str, str]] = []
        for fhr_str, fcst in forecasts.items():
            if not isinstance(fcst, dict) or not fcst.get("available"):
                continue
            layers = fcst.get("layers")
            if not layers:
                raise ValueError(f"Missing layers for fhr {fhr_str}")
            layer = layers[0]
            src = layer.get("src")
            server = layer.get("server")
            if not src or not server:
                raise ValueError(f"Layer missing src/server for fhr {fhr_str}")
            frame_dt = run_dt + timedelta(hours=int(fhr_str))
            tasks.append((frame_dt.strftime("%Y%m%d%H"), self.build_layer_url(server, src), run_hour))
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
        forecasts = run.get("forecasts", {}) or {}
        plan = RunPlan(
            run_hour=run.get("date", ""),
            tasks=list(self._iter_forecast_tasks(run)),
            product=f"{self.product}.{self.region}",
            max_forecast_hour=max(
                (int(k) for k, v in forecasts.items() if isinstance(v, dict) and v.get("available")),
                default=0,
            ),
        )
        self.set_cached_plan(plan)
        return plan

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        return output_dir / self.name / run_hour / f"{self.product}.{self.region}" / f"{frame_time}.png"


class AvalancheCanadaModel(ModelConfig):
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
        run_hour = 12 if now_utc.hour >= 12 else 0
        return now_utc.replace(hour=run_hour, minute=0, second=0, microsecond=0)

    def _iter_tasks(self, run_dt: datetime) -> List[Tuple[str, str, str]]:
        run_str = run_dt.strftime("%Y%m%d%H")
        day_path = run_dt.strftime("%Y/%m/%d")
        tasks: List[Tuple[str, str, str]] = []
        for fh in range(0, self.max_forecast_hour + self.step_hours, self.step_hours):
            frame_dt = run_dt + timedelta(hours=fh)
            tasks.append((frame_dt.strftime("%Y%m%d%H"), f"{self.BASE}/{day_path}/{self.product}_{run_str}_{fh:03d}HR.jpg", run_str))
        return tasks

    def list_forecast_images(self):
        cached = self.get_cached_plan()
        if cached is not None:
            return cached

        sess = requests.Session()

        def run_available(run_dt: datetime) -> bool:
            tasks = self._iter_tasks(run_dt)
            if not tasks:
                return False
            _, probe_url, _ = tasks[0]
            try:
                resp = sess.get(probe_url, headers=DEFAULT_HEADERS, timeout=10, stream=True)
                return resp.status_code == 200
            except Exception:
                return False

        run_dt = self._select_run_dt()
        while not run_available(run_dt):
            log(f"Avalanche Canada model run {run_dt} not available, step back for 12 hrs")
            run_dt -= timedelta(hours=12)

        plan = RunPlan(
            run_hour=run_dt.strftime("%Y%m%d%H"),
            tasks=self._iter_tasks(run_dt),
            product=self.product,
            max_forecast_hour=self.max_forecast_hour,
        )
        self.set_cached_plan(plan)
        return plan

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        return output_dir / self.name / run_hour / self.product / f"{frame_time}.jpg"


class SatelliteImage(ModelConfig):
    BASE = "https://weather.gc.ca/data/satellite"

    def __init__(
        self,
        name: str,
        product: str,
        lookback_hours: int = 3,
        step_minutes: int = 10,
        hide: bool = False,
        annotate: Optional[Tuple[int, int]] = None,
    ) -> None:
        super().__init__(name, product, hide, annotate=annotate)
        self.lookback_hours = lookback_hours
        self.step_minutes = step_minutes

    @staticmethod
    def _floor_to_step(now_utc: datetime, step_minutes: int) -> datetime:
        floored_minute = (now_utc.minute // step_minutes) * step_minutes
        return now_utc.replace(minute=floored_minute, second=0, microsecond=0)

    def _iter_tasks(self) -> List[Tuple[str, str, str]]:
        now_utc = datetime.now(timezone.utc) - timedelta(minutes=10)
        end_dt = self._floor_to_step(now_utc, self.step_minutes)
        start_dt = end_dt - timedelta(hours=self.lookback_hours)
        run_hour = end_dt.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d%H")

        tasks: List[Tuple[str, str, str]] = []
        frame_dt = start_dt
        while frame_dt <= end_dt:
            frame_stamp = frame_dt.strftime("%Y%m%d%H%M")
            url = (
                f"{self.BASE}/{self.product}_"
                f"{frame_dt.strftime('%Y@%m@%d_%Hh%Mm')}.jpg"
            )
            tasks.append((frame_stamp, url, run_hour))
            frame_dt += timedelta(minutes=self.step_minutes)
        return tasks

    def list_forecast_images(self):
        cached = self.get_cached_plan()
        if cached is not None:
            return cached

        tasks = self._iter_tasks()
        if not tasks:
            return None

        plan = RunPlan(
            run_hour=tasks[-1][2],
            tasks=tasks,
            product=self.product,
            max_forecast_hour=self.lookback_hours,
        )
        self.set_cached_plan(plan)
        return plan

    def dest_path(self, output_dir: Path, run_hour: str, frame_time: str) -> Path:
        return output_dir / self.name / run_hour / self.product / f"{frame_time}.jpg"


class ModelGroup:
    def __init__(self, name: str, models: List[ModelConfig]) -> None:
        self.name = name
        self.models = models

    def __getitem__(self, index):
        return self.models[index]

    def __len__(self) -> int:
        return len(self.models)

    def __iter__(self):
        return iter(self.models)

    def select(self, data_root: Path, start: datetime, end: Optional[datetime] = None) -> List[Path]:
        selected: List[Path] = []
        for model in self.models:
            if not model.hide:
                selected.extend(model.select(data_root, start, end))
        return selected


MODEL_GROUPS: List[ModelGroup] = [
    ModelGroup(
        name="Regional Precipitation",
        models=[
            AvalancheCanadaModel("ac_gdps", "AC_GDPS_EPA_clds-th-500hts", max_forecast_hour=144, step_hours=6, annotate=WHISTLER_AC_LOCATION),
            PivotalWeatherModel("rdps", "prateptype-met", "ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel("hrdps", "prateptype-met", "ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel("nam", "ref1km_ptype", "ca_w", hide=True, annotate=WHISTLER_REGIONAL_LOCATION),
        ],
    ),
    ModelGroup(
        name="Wind Speed",
        models=[
            PivotalWeatherModel("rdps", "700wh", "ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel("nam", "700wh", "ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel("hrdps", "700wh", "ca_w", annotate=WHISTLER_REGIONAL_LOCATION),
            PivotalWeatherModel("gfs", "700wh", "ca_w", hide=True, annotate=WHISTLER_REGIONAL_LOCATION),
        ],
    ),
    ModelGroup(
        name="Global Trending",
        models=[
            PivotalWeatherModel("cfs", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("gfs", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("aigfs", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("ecmwf_full", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("ecmwf_aifs", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("gdps", "500h_anom", "na", annotate=WHISTLER_GLOBAL_LOCATION),
            PivotalWeatherModel("icon", "500h_anom", "na", hide=True, annotate=WHISTLER_GLOBAL_LOCATION),
        ],
    ),
]

SATELLITE_IMAGE = SatelliteImage("weather_ca", "goes_wcan_1070_m", annotate=WHISTLER_SATELLITE_LOCATION)

MODELS: List[ModelConfig] = [m for g in MODEL_GROUPS for m in g.models] + [SATELLITE_IMAGE]
