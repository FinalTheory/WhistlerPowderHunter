from pathlib import Path

from context.codex import CodexThread
from context.constant import PRODUCT_META
from context.util import log


class ForecastInput:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root.absolute()
        self.resolved_data_root = data_root.resolve()
        self.pending: list[dict[str, str]] = []
        self.attached_paths: set[Path] = set()

    def can_attach(self, path: Path) -> bool:
        resolved = path.resolve()
        return (
            resolved.is_relative_to(self.resolved_data_root)
            and resolved not in self.attached_paths
            and resolved.is_file()
        )

    def append_prompt(self, prompt: str) -> None:
        self.pending.append({"type": "text", "text": prompt})

    def append(self, prompt: str, *, image_paths: list[Path]) -> list[str]:
        self.append_prompt(prompt)
        attached = []
        for path in sorted(image_paths, key=lambda path: (path.stem.ljust(12, "0"), str(path))):
            if not self.can_attach(path):
                continue
            relative = path.absolute().relative_to(self.data_root)
            model, run, product, _ = relative.parts
            self.attached_paths.add(path.resolve())
            chart = f"data/{relative.as_posix()}"
            self.append_prompt(
                f"Attached image #{len(self.attached_paths)}: {chart}\n"
                f"Model: {model.upper()} | Product: {product} | "
                f"Run UTC: {run} | Valid UTC: {path.stem}\n"
                f"{PRODUCT_META.get(product, product)}\n"
                "Whistler is marked by a red dot."
            )
            self.pending.append({"type": "localImage", "path": str(path.resolve())})
            attached.append(chart)
        return attached

    async def send(self, codex: CodexThread, schema: dict, *, stage: str) -> dict:
        count = sum(item["type"] == "localImage" for item in self.pending)
        log(f"Codex {stage}: sending {count} new images")
        response = await codex.turn(self.pending, schema)
        self.pending.clear()
        return response

    def dump_to(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        text = "\n\n".join(item["text"] for item in self.pending if item["type"] == "text")
        (directory / "message.txt").write_text(text, encoding="utf-8")
