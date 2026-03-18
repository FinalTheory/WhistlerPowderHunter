from typing import Dict

from context.constant import INIT_PROMPT_PATH, PRODUCT_META, TASK_PROMPT_PATH
from context.whistler import fetch_sensor_data, fetch_rwdi_forecast, fetch_lift_history


def build_task_prompt(model_data: Dict[str, object]) -> str:
    with TASK_PROMPT_PATH.open("r", encoding="utf-8") as prompt_file:
        return (
            prompt_file.read()
            .replace("{{PRODUCT_META}}", str(PRODUCT_META))
            .replace("{{MODEL_DATA}}", str(model_data))
            .replace("{{SENSOR_DATA}}", str(fetch_sensor_data()))
            .replace("{{LIFT_HISTORY}}", str(fetch_lift_history()))
        )


def build_router_body() -> str:
    return f"""

======

Now, we can start with given RWDI forecast information:

{fetch_rwdi_forecast()}

Here are forecast images from Avalanche Canada.
"""


def build_router_prompt() -> str:
    return INIT_PROMPT_PATH.open("r", encoding="utf-8").read()
