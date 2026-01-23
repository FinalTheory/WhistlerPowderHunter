"""
Minimal FastAPI server to serve pre-rendered viewer and static assets.
Run fetch_models.py first to generate viewer_rendered.html.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent.resolve()
RENDERED_VIEW = BASE_DIR / "index.html"
DATA_DIR = BASE_DIR / "data" / "raw"

app = FastAPI(title="Whistler Epic Weather Viewer", version="0.2.0")

# Serve everything in the repo as static assets so the data/raw tree is reachable.
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")
app.mount("/data", StaticFiles(directory=BASE_DIR / "data"), name="data")


@app.get("/")
async def root():
    target = RENDERED_VIEW
    if not target.exists():
        raise HTTPException(status_code=404, detail="viewer file not found; run fetch_models.py to generate it")
    return FileResponse(target)


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
