import argparse
import asyncio
import json
from contextlib import nullcontext
from pathlib import Path

from context.codex import TOKEN_LIMIT, open_codex
from context.constant import PROMPT_BASE_DIR
from context.download import download_all_models
from context.inventory import DataInventory, build_data_inventory, build_groups_payload, render_viewer_html
from context.model import MODELS
from context.prompt import build_router_body, build_task_prompt, build_router_prompt
from context.session import ForecastInput
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

TASK_OUTPUT_SCHEMA = {
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

FINAL_REPORT_SCHEMA = {
    **TASK_OUTPUT_SCHEMA,
    "properties": {key: value for key, value in TASK_OUTPUT_SCHEMA["properties"].items() if key != "need"},
    "required": ["run_again", "summary", "debug"],
}
MAX_SUPPLEMENTAL_ROUNDS = 3
MAX_SUPPLEMENTAL_IMAGES = 6


def select_supplemental_images(
    requested: list[str], inventory: DataInventory, evidence: ForecastInput,
) -> list[Path]:
    available = {
        f"data/{model}/{run}/{product}/{filename}": evidence.data_root / model / run / product / filename
        for model, runs in inventory.items()
        for run, products in runs.items()
        for product, files in products.items()
        for filename in files
    }
    paths = []
    for name in dict.fromkeys(requested):
        path = available.get(name)
        if path is not None and evidence.can_attach(path):
            paths.append(path)
            if len(paths) == MAX_SUPPLEMENTAL_IMAGES:
                break
    return paths


async def call_chatgpt_analysis(args: argparse.Namespace, data_root: Path) -> tuple[dict[str, str], bool]:
    evidence = ForecastInput(data_root)
    evidence.append_prompt(build_router_prompt())
    evidence.append(build_router_body(), image_paths=select_init_images(data_root))
    async with (nullcontext() if args.no_gpt else open_codex()) as codex:
        if codex is None:
            tasks = list(TASK_DEFINITION)
        else:
            log(f"Codex model: {codex.model}")
            routing = await evidence.send(codex, TASK_SELECTION_SCHEMA, stage="routing")
            tasks = list(dict.fromkeys(routing["tasks"]))
            if routing["reason"]:
                log(f"Routing reason: {routing['reason']}")
            if routing["debug"]:
                log(f"Routing debug: {routing['debug']}")

        log(f"Tasks = {tasks}")
        inventory = build_data_inventory(data_root)
        evidence.append_prompt(
            "Routing is complete. Start the forecast analysis stage using the prior "
            f"routing evidence and only these selected tasks: {json.dumps(tasks)}."
        )
        evidence.append_prompt(build_task_prompt(inventory))
        for task in tasks:
            prompt = (PROMPT_BASE_DIR / f"{task}.txt").read_text(encoding="utf-8")
            evidence.append(prompt, image_paths=TASK_DEFINITION[task](data_root))

        if codex is None:
            evidence.dump_to(args.out / "debug")
            return {}, False

        response = await evidence.send(codex, TASK_OUTPUT_SCHEMA, stage="analysis")
        if response["debug"]:
            log(f"Analysis debug: {response['debug']}")
        for round_number in range(1, MAX_SUPPLEMENTAL_ROUNDS + 1):
            requested = response["need"]
            log(f"Requested images: {json.dumps(requested)}")
            if not requested or codex.token_used >= TOKEN_LIMIT:
                break
            extra = select_supplemental_images(requested, inventory, evidence)
            final_round = round_number == MAX_SUPPLEMENTAL_ROUNDS
            instruction = (
                "This is the final turn. Return the complete bilingual forecast and run_again decision. "
                "Describe remaining evidence gaps in debug. Image requests are closed."
                if final_round else
                "Review the new charts and revise the complete bilingual forecast and run_again decision. "
                "Request additional unseen charts only if they would materially change the forecast."
            )
            attached = evidence.append(instruction, image_paths=extra)
            if unfulfilled := [name for name in dict.fromkeys(requested) if name not in attached]:
                log(f"Unfulfilled need: {json.dumps(unfulfilled)}")
            if not attached:
                break
            log(f"Supplement {round_number} images: {json.dumps(attached)}")
            schema = FINAL_REPORT_SCHEMA if final_round else TASK_OUTPUT_SCHEMA
            response = await evidence.send(codex, schema, stage=f"supplement {round_number}")
            if response["debug"]:
                log(f"Supplement {round_number} debug: {response['debug']}")
        log(f"Remaining need: {json.dumps(response.get('need', []))}")
        log(f"Used tokens: {codex.token_used}")
        return response["summary"], response["run_again"]


def run_analysis(args: argparse.Namespace) -> bool:
    data_root = args.out / "data"
    for model in MODELS:
        model.clear_cached_plan()

    download_all_models(models=MODELS, output_dir=data_root, workers=args.workers, overwrite=False)
    groups = build_groups_payload(data_root)
    if not groups:
        raise RuntimeError("No forecast images available to publish")

    summary, run_again = asyncio.run(call_chatgpt_analysis(args, data_root))
    index_path = args.out / "index.html"
    render_viewer_html(groups, summary, format_generated_at_local(), index_path)
    published_runs = {(model["name"], model["run"]) for group in groups for model in group["models"]}
    prune_old_runs(data_root, days=2, keep=published_runs)
    log(f"Run again: {run_again}")
    log(f"Rendered to {index_path}")
    return run_again
