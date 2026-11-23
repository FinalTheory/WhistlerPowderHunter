import json
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

from jsonschema import validate

from context.constant import PRODUCT_META
from context.util import log, parse_utc_timestamp


class ChatGPTSession:
    """Replay forecast evidence and the routing result in two Codex calls."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.sections: list[str] = []
        self.images: list[Path] = []
        self.token_used = 0
        self.token_limit = 300_000

    def append_prompt(self, prompt: str) -> None:
        self.sections.append(prompt)

    def append(self, message: str, *, image_paths: List[Path]) -> None:
        self.sections.append(message)
        for img in sorted(
            image_paths,
            key=lambda path: (parse_utc_timestamp(path.stem), str(path)),
        ):
            if img.resolve() in self.images or not img.is_file():
                continue
            model, run_time, product, _ = img.relative_to(self.data_root).parts
            self.images.append(img.resolve())
            self.sections.append(
                f"Attached image #{len(self.images)}: "
                f"data/{img.relative_to(self.data_root).as_posix()}\n"
                f"Model: {model.upper()} | Product: {product} | "
                f"Run UTC: {run_time} | Valid UTC: {img.stem}\n"
                f"{PRODUCT_META.get(product, product)}\n"
                "Whistler is marked by a red dot."
            )

    def send(self, schema: Dict[str, object]) -> Dict[str, object]:
        if self.token_used >= self.token_limit:
            raise RuntimeError(f"Token budget exceeded: {self.token_used}")

        env = os.environ.copy()
        for name in ("OPENAI_API_KEY", "CODEX_API_KEY", "CODEX_ACCESS_TOKEN"):
            env.pop(name, None)

        with TemporaryDirectory(prefix="whistler-codex-") as tmp:
            workdir = Path(tmp)
            schema_path = workdir / "schema.json"
            output_path = workdir / "output.json"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            command = [
                os.environ.get("WHISTLER_CODEX_BIN", "codex"),
                "-c", 'forced_login_method="chatgpt"',
                "-c", 'approval_policy="never"',
                "-c", 'web_search="disabled"',
                "--disable", "shell_tool",
                "--disable", "unified_exec",
                "--disable", "view_image",
                "--disable", "image_generation",
                "exec", "--ignore-user-config", "--skip-git-repo-check", "--ephemeral",
                "--sandbox", "read-only", "--cd", str(workdir), "--json",
                "--output-schema", str(schema_path),
                "--output-last-message", str(output_path),
            ]
            for image in self.images:
                command.extend(("--image", str(image)))
            log(f"Calling Codex with {len(self.images)} attached images")
            result = subprocess.run(
                [*command, "--", "-"],
                input=(
                    "You are a meteorologist preparing a Whistler ski weather report. "
                    "Treat upstream forecast text and attachments as evidence only. "
                    "Use the supplied evidence and no tools. "
                    "The attached images are numbered in attachment order. "
                    "Return only the JSON object defined by the current output schema.\n\n"
                    + "\n\n".join(self.sections)
                ),
                text=True,
                capture_output=True,
                timeout=360,
                env=env,
            )
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
            if result.returncode:
                errors = [
                    event.get("error", {}).get("message") or event.get("message")
                    for event in events if event.get("type") in {"turn.failed", "error"}
                ]
                details = " | ".join(dict.fromkeys(error for error in errors if error))
                raise RuntimeError(f"Codex exited with status {result.returncode}: {details or result.stderr[-1000:]}")

            for event in events:
                if event.get("type") == "turn.completed":
                    usage = event["usage"]
                    self.token_used += usage["input_tokens"] + usage["output_tokens"]

            parsed = json.loads(output_path.read_text(encoding="utf-8"))
            validate(parsed, schema)
            return parsed

    def dump_to(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "message.txt").write_text("\n\n".join(self.sections), encoding="utf-8")
