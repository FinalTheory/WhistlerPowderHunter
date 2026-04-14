import base64
import copy
import json
from pathlib import Path
from typing import Dict, List, Optional

from openai import OpenAI

from context.constant import BASE_DIR, PRODUCT_META
from context.util import log, sort_images_by_valid_time

CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"
HERMES_AUTH_PATH = Path.home() / ".hermes" / "auth.json"


class ChatGPTSession:
    """Stateful helper to talk to ChatGPT with images while retaining context."""

    def __init__(self, model: str, data_root: Path) -> None:
        self.api_key = self._resolve_api_key()
        self.client = OpenAI(api_key=self.api_key, base_url=CODEX_BASE_URL)
        self.model = model
        self.data_root = data_root
        self.messages: List[Dict[str, object]] = []
        self._sent_images: set[Path] = set()
        self.conversation_id: Optional[str] = None
        self.token_used = 0
        self.token_limit = 300000

    def _resolve_api_key(self) -> str:
        if not HERMES_AUTH_PATH.exists():
            raise RuntimeError("OPENAI_API_KEY is not set and ~/.hermes/auth.json was not found")
        auth_payload = json.loads(HERMES_AUTH_PATH.read_text(encoding="utf-8"))
        return auth_payload["providers"]["openai-codex"]["tokens"]["access_token"]

    def append_prompt(self, prompt: str) -> None:
        message = {
            "role": "developer",
            "content": [{"type": "input_text", "text": prompt}],
        }
        self.messages.append(message)

    def append(self, message: str, image_paths: Optional[List[Path]] = None) -> None:
        images = sort_images_by_valid_time(image_paths) if image_paths else []
        user_content: List[Dict[str, object]] = [{"type": "input_text", "text": message}]

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
                user_content.append({"type": "input_text", "text": "\n".join(label_lines)})
            except Exception:
                log(f"Image metadata parse failed, skip this image: {img}")

            mime = "image/png"
            if img.suffix.lower() in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            img_b64 = base64.b64encode(img.read_bytes()).decode("ascii")
            user_content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:{mime};base64,{img_b64}",
                    "detail": "auto",
                }
            )
            self._sent_images.add(img)

        user_message = {"role": "user", "content": user_content}
        self.messages.append(user_message)

    def _extract_tool_payload(
        self,
        response,
        streamed_tool_calls: List[object],
        expected_tool_name: str,
    ) -> Dict[str, object]:
        for tool_call in streamed_tool_calls:
            if getattr(tool_call, "name", None) == expected_tool_name:
                return json.loads(getattr(tool_call, "arguments", "{}"))
        for item in getattr(response, "output", []):
            if getattr(item, "type", None) != "function_call":
                continue
            if getattr(item, "name", None) != expected_tool_name:
                continue
            return json.loads(getattr(item, "arguments", "{}"))
        raise RuntimeError(f"Model did not call expected tool: {expected_tool_name}")

    def send(self, tool_schema: Dict[str, object], max_retries: int = 3) -> Dict[str, object]:
        last_error = None

        for _ in range(max_retries):
            if self.token_used > self.token_limit:
                raise RuntimeError(f"Token budget exceeded: used {self.token_used} > limit {self.token_limit}")
            request_kwargs: Dict[str, object] = {
                "model": self.model,
                "input": self.messages,
                "instructions": "You are a meteorologist.",
                "store": False,
                "tools": [tool_schema],
                "tool_choice": {"type": "function", "name": tool_schema["name"]},
            }
            if self.conversation_id:
                request_kwargs["conversation"] = self.conversation_id
                print("Sending to to conversation_id=[{}]".format(self.conversation_id))
            streamed_tool_calls: List[object] = []
            with self.client.responses.stream(**request_kwargs) as stream:
                for event in stream:
                    if getattr(event, "type", "") != "response.output_item.done":
                        continue
                    item = getattr(event, "item", None)
                    if getattr(item, "type", None) != "function_call":
                        continue
                    streamed_tool_calls.append(item)
                resp = stream.get_final_response()
            usage = getattr(resp, "usage", None)
            tokens = getattr(usage, "total_tokens", None)
            try:
                self.token_used += int(tokens)
            except:
                log(f"Unable to parse used tokens: '{tokens}'")
            try:
                parsed = self._extract_tool_payload(resp, streamed_tool_calls, tool_schema["name"])
                self.messages.append(
                    {
                        "role": "assistant",
                        "content": json.dumps(
                            {"tool_name": tool_schema["name"], "arguments": parsed}, ensure_ascii=False,
                        ),
                    }
                )
                conversation = getattr(resp, "conversation", None)
                conversation_id = getattr(conversation, "id", None)
                if conversation_id:
                    self.conversation_id = conversation_id
                    print("Allocated conversation_id=[{}]".format(conversation_id))
                return parsed
            except Exception as exc:
                last_error = exc

        raise RuntimeError("Failed to obtain valid JSON response") from last_error

    def history(self) -> List[Dict[str, object]]:
        return list(self.messages)

    def dump_to(self, dir_name: str) -> None:
        debug_dir = BASE_DIR / dir_name
        debug_dir.mkdir(parents=True, exist_ok=True)
        messages = copy.deepcopy([m for m in self.messages if m])
        image_index = 1
        for msg in messages:
            if msg.get("role") != "user":
                continue
            for content in msg.get("content", []):
                if content.get("type") != "input_image":
                    continue
                url = content.get("image_url")
                if url and url.startswith("data:"):
                    header, payload = url.split(",", 1)
                    mime = header.split(";", 1)[0][5:]
                    suffix = ".jpg" if mime == "image/jpeg" else ".png"
                    (debug_dir / f"{image_index}{suffix}").write_bytes(base64.b64decode(payload))
                    image_index += 1
                content["image_url"] = ""

        with open(debug_dir / Path("message.txt").name, "w") as f:
            f.write(json.dumps(messages, indent=2))
