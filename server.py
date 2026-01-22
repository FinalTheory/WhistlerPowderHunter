"""
FastAPI server for local testing with templated viewer.
- Serves viewer template at root with dynamically discovered runs.
- Exposes /static for assets (data/raw imagery included).
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from fetch_models import MODELS, MODEL_GROUPS, ModelConfig, ModelGroup

BASE_DIR = Path(__file__).parent.resolve()
VIEWER_FILE = BASE_DIR / "viewer.html"
DATA_DIR = BASE_DIR / "data" / "raw"

app = FastAPI(title="Whistler Epic Weather Viewer", version="0.2.0")

# Serve everything in the repo as static assets so the data/raw tree is reachable.
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR))


def _latest_run_dir(model_name: str) -> Optional[Path]:
    model_dir = DATA_DIR / model_name
    if not model_dir.exists():
        return None
    run_dirs = sorted([p for p in model_dir.iterdir() if p.is_dir()], reverse=True)
    return run_dirs[0] if run_dirs else None


_FHR_RE = re.compile(r"_f(\d{3})_")


def _frames_in_run(model_name: str, run_dir: Path) -> List[Dict[str, object]]:
    frames: List[Dict[str, object]] = []
    for file in sorted(run_dir.iterdir()):
        if not file.is_file():
            continue
        m = _FHR_RE.search(file.name)
        if not m:
            continue
        try:
            fhr = int(m.group(1))
        except ValueError:
            continue
        frames.append({"fhr": fhr, "url": f"/static/data/raw/{model_name}/{run_dir.name}/{file.name}"})
    frames.sort(key=lambda f: f["fhr"])
    return frames


def build_model_payload(model: ModelConfig) -> Optional[Dict[str, object]]:
    run_dir = _latest_run_dir(model.name)
    if not run_dir:
        return None
    run_name = run_dir.name
    frames = _frames_in_run(model.name, run_dir)
    if not frames:
        return None
    min_fhr = min(f.get("fhr", 0) for f in frames)
    max_fhr = max(f.get("fhr", 0) for f in frames)
    return {
        "id": f"{model.name}_{model.product}",
        "name": model.name,
        "run": run_name,
        "frames": frames,
        "min_fhr": min_fhr,
        "max_fhr": max_fhr,
    }


@app.get("/")
async def root(request: Request):
    if not VIEWER_FILE.exists():
        raise HTTPException(status_code=404, detail="viewer.html not found")
    groups_payload: List[Dict[str, object]] = []
    for grp in MODEL_GROUPS:
        models_payload: List[Dict[str, object]] = []
        for m in grp.models:
            payload = build_model_payload(m)
            if payload:
                models_payload.append(payload)
        if models_payload:
            groups_payload.append({"name": grp.name, "models": models_payload})
    return templates.TemplateResponse(
        "viewer.html",
        {"request": request, "groups_json": json.dumps(groups_payload)},
    )


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/runs")
async def list_runs() -> JSONResponse:
    """List available model run directories under data/raw for quick inspection."""
    if not DATA_DIR.exists():
        return JSONResponse({"runs": []})
    runs: Dict[str, List[str]] = {}
    for model_dir in sorted(p for p in DATA_DIR.iterdir() if p.is_dir()):
        runs[model_dir.name] = [p.name for p in sorted(model_dir.iterdir()) if p.is_dir()]
    return JSONResponse({"runs": runs})


# To run: uvicorn server:app --reload
