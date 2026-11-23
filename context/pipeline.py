import argparse
import json
from pathlib import Path
from typing import Dict, Tuple

from context.constant import PROMPT_BASE_DIR
from context.download import download_all_models
from context.inventory import build_data_inventory, build_groups_payload, render_viewer_html
from context.model import MODELS
from context.prompt import build_router_body, build_task_prompt, build_router_prompt
from context.session import ChatGPTSession
from context.task import TASK_DEFINITION, select_init_images
from context.util import format_generated_at_local, log, prune_old_runs

TASK_SELECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "tasks": {"type": "array", "items": {"type": "string", "enum": list(TASK_DEFINITION.keys())}},
        "reason": {"type": "string"},
        "debug": {"type": "string"},
    },
    "required": ["tasks", "reason", "debug"],
    "additionalProperties": False,
}

TASK_OUTPUT_SCHEMA: Dict[str, object] = {
    "type": "object",
    "properties": {
        "need": {"type": "array", "items": {"type": "string"}},
        "run_again": {"type": "boolean"},
        "summary": {
            "type": "object",
            "properties": {"en": {"type": "string"}, "zh": {"type": "string"}},
            "required": ["en", "zh"],
            "additionalProperties": False,
        },
        "debug": {"type": "string"},
    },
    "required": ["need", "run_again", "summary", "debug"],
    "additionalProperties": False,
}

def call_chatgpt_analysis(args: argparse.Namespace, data_root: Path) -> Tuple[Dict[str, str], bool]:
    init_images = select_init_images(data_root)
    gpt = ChatGPTSession(data_root=data_root)
    gpt.append_prompt(build_router_prompt())
    gpt.append(build_router_body(), image_paths=init_images)
    if not args.no_gpt:
        response = gpt.send(TASK_SELECTION_SCHEMA)
    else:
        response = {"tasks": list(TASK_DEFINITION.keys())}
    gpt.append_prompt("Completed routing result:\n" + json.dumps(response, ensure_ascii=False))
    tasks = list(dict.fromkeys(response["tasks"]))
    if reason := response.get("reason", ""):
        log(f"Reason: {reason}")
    if debug := response.get("debug"):
        log(debug)
    log(f"Tasks = {tasks}")
    gpt.append_prompt(build_task_prompt(build_data_inventory(data_root)))
    for task in tasks:
        prompt_path = PROMPT_BASE_DIR / f"{task}.txt"
        with open(prompt_path, "r") as prompt_file:
            gpt.append(prompt_file.read(), image_paths=TASK_DEFINITION[task](data_root))
    if not args.no_gpt:
        response = gpt.send(TASK_OUTPUT_SCHEMA)
    else:
        gpt.dump_to(args.out / "debug")
        response = {}
    log(response.get("need", []))
    log(f"Used tokens: {gpt.token_used}")
    if debug := response.get("debug"):
        log(debug)
    return response.get("summary", {}), bool(response.get("run_again", False))


def run_analysis(args: argparse.Namespace) -> bool:
    data_root = args.out / "data"
    for model in MODELS:
        model.clear_cached_plan()

    download_all_models(models=MODELS, output_dir=data_root, workers=args.workers, overwrite=False)
    groups = build_groups_payload(data_root)
    if not groups:
        raise RuntimeError("No forecast images available to publish")

    summary, run_again = call_chatgpt_analysis(args, data_root)
    index_path = args.out / "index.html"
    render_viewer_html(groups, summary, format_generated_at_local(), index_path)
    published_runs = {(model["name"], model["run"]) for group in groups for model in group["models"]}
    prune_old_runs(data_root, days=2, keep=published_runs)
    log(f"Run again: {run_again}")
    log(f"Rendered to {index_path}")
    return run_again
