import re
from pathlib import Path
from zoneinfo import ZoneInfo

TIME_ZONE = ZoneInfo("America/Vancouver")

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}

BASE_DIR = Path(__file__).resolve().parent.parent
VIEWER_TEMPLATE = BASE_DIR / "template.html"
LOG_PATH = BASE_DIR / "log.txt"
PROMPT_BASE_DIR = BASE_DIR / "prompt"
INIT_PROMPT_PATH = PROMPT_BASE_DIR / "prompt_init.txt"
TASK_PROMPT_PATH = PROMPT_BASE_DIR / "prompt_task.txt"

WHISTLER_REGIONAL_LOCATION = (608.1, 628.6)
WHISTLER_GLOBAL_LOCATION = (437, 374)
WHISTLER_AC_LOCATION = (907, 477)
WHISTLER_SATELLITE_LOCATION = (369, 646)

FRAME_TS_RE = re.compile(r"(\d{10}|\d{12})")

PRODUCT_META = {
    "500h_anom.na": "500mb height abnormality of North America",
    "700wh.ca_w": "700mb Height Wind Speed (kt) of West Canada",
    "prateptype-met.ca_w": "Precipitation Type, 6-h Avg Rate (mm/hr), 1000-500mb thickness (dam) of West Canada",
    "nam.ref1km_ptype.ca_w": "1km AGL Reflectivity(dBZ), Type, 1000-500mb thickness (dam) of West Canada",
    "AC_GDPS_EPA_clds-th-500hts": "Avalanche Canada forecast graphic (PNW area) combining 6-hour precipitation, atmospheric thickness (1000–500 mb as a snow-line proxy), and geopotential height contours to diagnose storm structure and large-scale trends rather than precise snowfall amounts.",
    "goes_wcan_1070_m": "Satellite image for Western Canada - IR (10.7 µm)"
}

PROXY = {
    "http": "socks5h://127.0.0.1:1080",
    "https": "socks5h://127.0.0.1:1080",
}

ALPINE_LIFTS = {
    "Peak Express",
    "Harmony 6 Express",
    "Symphony Express",
    "7th Heaven",
    "Showcase T-bar",
    "Glacier Express",
}
