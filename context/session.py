import base64
import copy
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from openai import OpenAI

from context.constant import BASE_DIR, PRODUCT_META
from context.util import log, sort_images_by_valid_time


class ChatGPTSession:
    """Stateful helper to talk to ChatGPT with images while retaining context."""

    def __init__(self, model: str, data_root: Path) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.model = model
        self.messages: List[Dict[str, object]] = []
        self._sent_images: set[Path] = set()
        self.token_used = 0
        self.token_limit = 300000

    def append_prompt(self, prompt: str) -> None:
        self.messages.append({"role": "system", "content": prompt})

    def append(self, message: str, image_paths: Optional[List[Path]] = None) -> None:
        images = sort_images_by_valid_time(image_paths) or []
        user_content: List[Dict[str, object]] = [{"type": "text", "text": message}]

        for img in images:
            if img in self._sent_images or not img.exists():
                continue
            try:
                rel_parts = img.relative_to(BASE_DIR).parts
                if len(rel_parts) < 5 or rel_parts[0] != "data":
                    raise ValueError(f"Unexpected image path layout: {img}")
                model_name = rel_parts[1]
                run_time = rel_parts[2]
                product_key = rel_parts[3]
                valid_time = Path(rel_parts[4]).stem
                product_desc = PRODUCT_META.get(product_key)
                label_lines = [
                    f"Model: {model_name.upper()}",
                    f"Product: {product_key}",
                    f"Valid UTC: {valid_time}",
                    f"Run UTC: {run_time}",
                    f"Image Path: {'/'.join(rel_parts)}",
                ]
                if product_desc:
                    label_lines.append(f"Product Description: {product_desc}")
                label_lines.append("Note: Whistler location is marked by a red dot on the image.")
                user_content.append({"type": "text", "text": "\n".join(label_lines)})
            except Exception:
                log(f"Image metadata parse failed, skip this image: {img}")

            mime = "image/png"
            if img.suffix.lower() in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            img_b64 = base64.b64encode(img.read_bytes()).decode("ascii")
            user_content.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}})
            self._sent_images.add(img)

        self.messages.append({"role": "user", "content": user_content})

    def send(self, json_schema: Optional[Dict[str, object]] = None, max_retries: int = 3) -> Dict[str, object]:
        client = OpenAI(api_key=self.api_key)
        last_error = None

        for _ in range(max_retries):
            if self.token_used > self.token_limit:
                raise RuntimeError(f"Token budget exceeded: used {self.token_used} > limit {self.token_limit}")
            request_kwargs: Dict[str, object] = {"model": self.model, "messages": self.messages}
            if json_schema is not None:
                request_kwargs["response_format"] = {"type": "json_schema", "json_schema": json_schema}
            resp = client.chat.completions.create(**request_kwargs)
            choices = resp.choices or []
            reply = choices[0].message.content if choices else None
            if reply is None:
                last_error = ValueError("Empty reply from assistant")
                continue
            usage = getattr(resp, "usage", None) or {}
            if isinstance(usage, dict):
                tokens = usage.get("total_tokens")
            else:
                tokens = getattr(usage, "total_tokens", None)
            try:
                self.token_used += int(tokens)
            except:
                log(f"Unable to parse used tokens: '{tokens}'")
            try:
                parsed = json.loads(reply)
                self.messages.append({"role": "assistant", "content": reply})
                return parsed
            except Exception as exc:
                last_error = exc

        raise RuntimeError("Failed to obtain valid JSON response") from last_error

    def history(self) -> List[Dict[str, object]]:
        return list(self.messages)

    def dump_to(self, file_name: str) -> None:
        debug_dir = BASE_DIR / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        messages = copy.deepcopy([m for m in self.messages if m])
        image_index = 1
        for msg in messages:
            if msg.get("role") != "user":
                continue
            for content in msg.get("content", []):
                if content.get("type") != "image_url":
                    continue
                url = content.get("image_url", {}).get("url")
                if url and url.startswith("data:"):
                    header, payload = url.split(",", 1)
                    mime = header.split(";", 1)[0][5:]
                    suffix = ".jpg" if mime == "image/jpeg" else ".png"
                    (debug_dir / f"{image_index}{suffix}").write_bytes(base64.b64decode(payload))
                    image_index += 1
                content["image_url"]["url"] = ""

        with open(debug_dir / Path(file_name).name, "w") as f:
            f.write(json.dumps(messages, indent=2))
