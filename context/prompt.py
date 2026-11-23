from datetime import datetime
from typing import Dict

from context.constant import INIT_PROMPT_PATH, PRODUCT_META, TASK_PROMPT_PATH, TIME_ZONE
from context.whistler import (
    fetch_lift_history,
    fetch_rwdi_forecast,
    fetch_sensor_data,
)


def build_task_prompt(model_data: Dict[str, object]) -> str:
    with TASK_PROMPT_PATH.open("r", encoding="utf-8") as prompt_file:
        return (
            prompt_file.read()
            .replace("{{PRODUCT_META}}", str(PRODUCT_META))
            .replace("{{MODEL_DATA}}", str(model_data))
            .replace("{{SENSOR_DATA}}", str(fetch_sensor_data()))
        )


def build_router_body() -> str:
    local_now = datetime.now(TIME_ZONE)
    return f"""

======

Reference date: today in Whistler is {local_now:%Y-%m-%d}. The current local hour began at {local_now:%H}:00.
Use Whistler local time (America/Vancouver, UTC{local_now:%z}) for all decision windows and
time labels. Use the stated offset when converting UTC; refer to local times as Whistler time.
Snow/lift history includes today and the previous local days.
All tactical decisions should be made for the next 2 local days after this reference date.

Now, we can start with given RWDI forecast information:

{fetch_rwdi_forecast()}

Here are forecast images from Avalanche Canada.
"""


def build_router_prompt() -> str:
    return INIT_PROMPT_PATH.open("r", encoding="utf-8").read().replace("{{LIFT_HISTORY}}", str(fetch_lift_history()))
