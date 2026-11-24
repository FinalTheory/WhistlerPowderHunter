import concurrent.futures
import re
from datetime import date, datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from context.constant import DEFAULT_HEADERS, ALPINE_LIFTS, TIME_ZONE
from context.util import log


WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def fetch_sensor_data() -> Dict[str, List[str]]:
    locations = ["horstman", "peak", "symphony", "harmony", "roundhouse", "rendezvous", "pigalley"]
    sensor_urls = [f"https://whistlerpeak.com/temps/plot-{loc}.json" for loc in locations]

    def fetch_one(url: str) -> Dict[str, List[str]]:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def transform(data: Dict[str, List[str]]) -> List[str]:
        result: List[str] = []
        cnt = len(data["date"])
        for i in range(cnt):
            result.append(
                f"{data['date'][i]} Temperature={data['temp'][i]}C "
                f"Wind Direction={data['winddir'][i]} ({data['direction'][i]}degree), "
                f"Max Wind={data['maxwind'][i]}km/h Avg Wind={data['avgwind'][i]}km/h"
            )
        return result

    sensor_data: Dict[str, List[str]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        future_to_loc = {executor.submit(fetch_one, url): loc for url, loc in zip(sensor_urls, locations)}
        for future in concurrent.futures.as_completed(future_to_loc):
            loc = future_to_loc[future]
            sensor_data[loc] = transform(future.result())
    return sensor_data


def fetch_rwdi_forecast(url: str = "https://www.whistlerpeak.com/forecast/block-alpine-grid.php") -> Optional[Dict[str, object]]:
    headers = dict(DEFAULT_HEADERS)
    headers["referer"] = "https://www.whistlerpeak.com/"
    headers["accept"] = "text/html"
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"RWDI forecast fetch failed: status {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")

    synopsis_text: Optional[str] = None
    issued_at: Optional[str] = None
    summary = soup.select_one(".rwdi-synopsis-full")
    if summary:
        synopsis_text = " ".join(p.get_text(" ", strip=True) for p in summary.select("p"))
        timestamp = summary.select_one("time[datetime]")
        if timestamp:
            issued_at = timestamp["datetime"]
    else:
        summary = soup.select_one("#summaryContent, .summaryContent, .summaryContent__body")
        if summary:
            synopsis_text = summary.get_text(" ", strip=True)
    synopsis_text = re.sub(r"\s+", " ", synopsis_text).strip() if synopsis_text else None

    days: List[Dict[str, str]] = []
    for card in soup.select(".rwdi-period-card, .alpine__card"):
        heading = card.find(["h3", "h4"]) or card.select_one(".alpine__card__title")
        name = heading.get_text(" ", strip=True) if heading else ""
        if "rwdi-period-card" in card.get("class", []):
            text = " | ".join(
                item.get_text(" ", strip=True)
                for item in card.select("p, .om-card-stats .om-card-row")
            )
        else:
            text = " ".join(p.get_text(" ", strip=True) for p in card.find_all("p"))
        if name or text:
            days.append({"day": name, "text": re.sub(r"\s+", " ", text).strip()})

    if not days and synopsis_text is None:
        raise RuntimeError("RWDI forecast parse failure: no synopsis or day cards found")

    return {"issued_at": issued_at, "synopsis": synopsis_text, "days": days}


def fetch_lift_history(url: str = "https://whistlerpeak.com/lift-history/read_json_switch.php", keep_days: int = 5) -> List[Dict[str, object]]:
    def _cell_text(div):
        parts = [" ".join(s.strip().split()) for s in div.stripped_strings]
        return " ".join(p for p in parts if p)

    def _infer_recent_weekday_date(day_label: str) -> date:
        weekday_name = day_label.strip().lower()
        today = datetime.now(TIME_ZONE).date()
        delta_days = (today.weekday() - WEEKDAY_INDEX[weekday_name]) % 7
        return today.fromordinal(today.toordinal() - delta_days)

    headers = dict(DEFAULT_HEADERS)
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Lift history fetch failed: status {resp.status_code}")

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    days = []
    first_btn = soup.select_one("#accordionFirst")
    first_content = soup.select_one(".accordion-content-first")
    if first_btn and first_content:
        days.append((first_btn.get_text(strip=True), first_content))

    for btn in soup.select("button.accordion"):
        if first_btn is not None and btn is first_btn:
            continue
        content = btn.find_next_sibling("div", class_="accordion-content")
        if content:
            days.append((btn.get_text(strip=True), content))

    out = []
    for day_label, content in days[:keep_days]:
        lifts = []
        names = content.select(".liftName.lift-entry")
        for name_div in names:
            name = name_div.get_text(" ", strip=True)
            if name not in ALPINE_LIFTS:
                continue

            open_div = name_div.find_next_sibling("div", class_="openTime")
            close_div = open_div.find_next_sibling("div", class_="closeTime") if open_div else None
            if not open_div or not close_div:
                continue

            classes = set(name_div.get("class", []))
            mountain = "whistler" if "whistlerLift" in classes else "blackcomb" if "blackcombLift" in classes else ""

            lifts.append({
                "lift": name,
                "mountain": mountain,
                "last_opened": _cell_text(open_div),
                "last_closed": _cell_text(close_div),
                "opened_today": bool(open_div.select_one(".underTimeClass")),
            })

        if lifts:
            out.append({"day": day_label, "lifts": lifts})

    snow_history_by_day: Dict[str, str] = {}
    try:
        snow_history = fetch_snow_history()
    except (RuntimeError, requests.RequestException) as exc:
        log(f"Snowfall history unavailable; retaining lift observations: {exc}")
        snow_history = []
    for snow_report in snow_history:
        snow_history_by_day.setdefault(snow_report.partition(":")[0], snow_report)

    for day_entry in out:
        inferred_date = _infer_recent_weekday_date(str(day_entry["day"]))
        day_entry["day"] = str(inferred_date)
        day_entry["snow_report"] = snow_history_by_day.get(f"{inferred_date:%b} {inferred_date.day}", "Unavailable")
    return out


def fetch_snow_history(url: str = "https://whistlerpeak.com/snow/history/block-history-grid.php") -> List[str]:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    dates = soup.select(".day-container > .dayData")
    if not dates:
        raise RuntimeError("Snow history parse failure: no dated rows")

    def measurement(cell):
        text = cell.get_text(strip=True) if cell else ""
        return text if re.fullmatch(r"\d+(?:\.\d+)?cm", text) else "Unavailable"

    history = []
    for day in dates:
        # The site gives zero and blank snow cells the class 'units'.
        snow = measurement(day.parent.select_one(":scope > .snowData, :scope > .units"))
        base = measurement(day.parent.select_one(":scope > .baseData"))
        if snow != "Unavailable" or base != "Unavailable":
            history.append(f"{day.get_text(strip=True)}: {snow} new snow, {base} base")
    return history
