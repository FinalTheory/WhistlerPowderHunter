import argparse
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

from context import pipeline
from context.session import ForecastInput


class IncrementalAnalysisTests(unittest.IsolatedAsyncioTestCase):
    async def test_routes_analyzes_and_sends_only_new_inventory_images_once(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            data_root = output / "data"
            product = data_root / "rdps" / "2026112400" / "700wh.ca_w"
            product.mkdir(parents=True)
            initial, task, extra, second, third = [product / f"20261124{hour}.png" for hour in ("00", "03", "06", "12", "15")]
            for path in (initial, task, extra, second, third):
                path.touch()
            missing = product / "2026112409.png"
            inventory = {"rdps": {"2026112400": {"700wh.ca_w": [
                initial.name, task.name, extra.name, second.name, third.name, missing.name,
            ]}}}

            def chart(path):
                return "data/" + path.relative_to(data_root).as_posix()

            need = [chart(extra), chart(extra), chart(initial), chart(missing), "data/../private.png"]
            route = {"tasks": ["WIND_OPERATION_TASK"], "reason": "", "debug": ""}
            draft = {"need": need, "summary": {"en": "Draft", "zh": "初稿"}, "run_again": False, "debug": ""}
            final = { "summary": {"en": "Final", "zh": "终稿"}, "run_again": True, "debug": ""}
            turns = []
            responses = iter([route, draft, {**draft, "need": [chart(second)]}, {**draft, "need": [chart(third)]}, final])

            async def record_turn(inputs, schema):
                turns.append((list(inputs), schema))
                return next(responses)

            @asynccontextmanager
            async def open_test_codex():
                yield argparse.Namespace(model="test", token_used=10, turn=AsyncMock(side_effect=record_turn))

            with (
                patch.object(pipeline, "open_codex", open_test_codex),
                patch.object(pipeline, "build_router_prompt", return_value="ROUTER PROMPT"),
                patch.object(pipeline, "build_router_body", return_value="ROUTER EVIDENCE"),
                patch.object(pipeline, "select_init_images", return_value=[initial]),
                patch.object(pipeline, "build_data_inventory", return_value=inventory),
                patch.object(pipeline, "build_task_prompt", return_value="ANALYSIS PROMPT"),
                patch.dict(pipeline.TASK_DEFINITION, {"WIND_OPERATION_TASK": lambda _: [initial, task]}, clear=True),
            ):
                result = await pipeline.call_chatgpt_analysis(argparse.Namespace(out=output, no_gpt=False), data_root)

            self.assertNotIn("need", turns[-1][1]["properties"])
            self.assertFalse(turns[-1][1]["additionalProperties"])
            self.assertEqual(result, (final["summary"], True))
            self.assertEqual([schema for _, schema in turns], [
                pipeline.TASK_SELECTION_SCHEMA, *[pipeline.TASK_OUTPUT_SCHEMA] * 3, pipeline.FINAL_REPORT_SCHEMA,
            ])
            self.assertEqual([
                [item["path"] for item in inputs if item["type"] == "localImage"]
                for inputs, _ in turns
            ], [[str(path.resolve())] for path in (initial, task, extra, second, third)])
            self.assertIn("ROUTER PROMPT", [item.get("text") for item in turns[0][0]])
            self.assertNotIn("ROUTER PROMPT", [item.get("text") for item in turns[1][0]])
            self.assertNotIn("ANALYSIS PROMPT", [item.get("text") for item in turns[2][0]])

    def test_supplemental_images_are_capped_and_cannot_leave_data_root(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            data_root = output / "data"
            product = data_root / "rdps" / "2026112400" / "700wh.ca_w"
            product.mkdir(parents=True)
            images = [product / f"20261124{hour:02}.png" for hour in range(9)]
            for image in images[1:]:
                image.touch()
            outside = output / "outside.png"
            outside.touch()
            images[0].symlink_to(outside)
            inventory = {"rdps": {"2026112400": {"700wh.ca_w": [image.name for image in images]}}}
            requests = ["data/" + image.relative_to(data_root).as_posix() for image in images]
            self.assertEqual(
                pipeline.select_supplemental_images(requests, inventory, ForecastInput(data_root)),
                images[1:1 + pipeline.MAX_SUPPLEMENTAL_IMAGES],
            )
