from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List

from context.constant import TIME_ZONE
from context.model import MODEL_GROUPS, SATELLITE_IMAGE
from context.util import trunc_to_hour


def select_init_images(data_root: Path) -> List[Path]:
    now = datetime.now()
    model = MODEL_GROUPS[0][0]
    return (
        model.select(data_root, now, now + timedelta(days=1))[:2]
        + model.select(data_root, now + timedelta(days=1))
        + model.select(data_root, now + timedelta(days=2))
    )


def select_precip_images(data_root: Path) -> List[Path]:
    now = datetime.now(TIME_ZONE)
    start = now
    end = trunc_to_hour(start + timedelta(days=1), 15)
    group1 = (
        MODEL_GROUPS[0][0].select(data_root, start, end)
        + MODEL_GROUPS[0][1].select(data_root, start, end)
        + MODEL_GROUPS[0][2].select(data_root, start, end)[::3]
    )
    start = end.replace(hour=18)
    end = (start + timedelta(days=1)).replace(hour=12)
    group2 = MODEL_GROUPS[0][0].select(data_root, start, end) + MODEL_GROUPS[0][2].select(data_root, start, end)[::6]
    group3 = SATELLITE_IMAGE.select(data_root, now - timedelta(hours=3), now)
    group3 = [group3[0], group3[len(group3) // 2], group3[-1]]
    return group1 + group2 + group3


def select_wind_images(data_root: Path) -> List[Path]:
    start = datetime.now(TIME_ZONE)
    end = trunc_to_hour(start + timedelta(days=1), 15)
    # all 700wh images from now to tomorrow afternoon
    return (
        MODEL_GROUPS[1][0].select(data_root, start, end)
        + MODEL_GROUPS[1][1].select(data_root, start, end)
        + MODEL_GROUPS[1][2].select(data_root, start, end)[::3]
    )


def select_pattern_task_images(data_root: Path) -> List[Path]:
    now = datetime.now(timezone.utc)
    # Avalanche Canada images, +3d until end, keep 4 images in total
    images = MODEL_GROUPS[0][0].select(data_root, now + timedelta(days=3), now + timedelta(days=30))
    # 500h_anom after 5, 10, 14 days
    dates = [trunc_to_hour(now + timedelta(days=d), 12) for d in (5, 10, 14)]
    return [p for d in dates for p in MODEL_GROUPS[2].select(data_root, d)] + images[::max(1, len(images) // 3)]


def select_decision_task_images(data_root: Path) -> List[Path]:
    start = datetime.now(TIME_ZONE)
    end = trunc_to_hour(start + timedelta(days=1), 11)
    # HRDPS images from now to tomorrow 11am
    return MODEL_GROUPS[0][2].select(data_root, start, end)


TASK_DEFINITION: Dict[str, Callable[[Path], List[Path]]] = {
    "PATTERN_TASK": select_pattern_task_images,
    "PRECIP_EVENT_TASK": select_precip_images,
    "THERMAL_PHASE_TASK": lambda _: [],
    "WIND_OPERATION_TASK": select_wind_images,
    "DECISION_TASK": select_decision_task_images,
}
