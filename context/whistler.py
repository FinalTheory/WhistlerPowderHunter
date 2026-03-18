import concurrent.futures
import re
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from context.constant import DEFAULT_HEADERS, ALPINE_LIFTS
from context.util import log


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
            try:
                sensor_data[loc] = transform(future.result())
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

def fetch_lift_history(url: str = "https://whistlerpeak.com/lift-history/read_json_switch.php", keep_days: int = 3) -> List[Dict[str, object]]:
    def _cell_text(div):
        parts = [" ".join(s.strip().split()) for s in div.stripped_strings]
        return " ".join(p for p in parts if p)

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
                "opened": _cell_text(open_div),
                "closed": _cell_text(close_div),
                "opened_today": bool(open_div.select_one(".underTimeClass")),
            })

        if lifts:
            out.append({"day": day_label, "lifts": lifts})
    return out
