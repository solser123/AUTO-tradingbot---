from __future__ import annotations

import logging
from html import escape

try:
    import requests
except Exception:
    requests = None


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id

    def send(self, message: str, parse_mode: str | None = None) -> None:
        self.send_to_chat(self.chat_id, message, parse_mode=parse_mode)

    def send_to_chat(self, chat_id: str | int, message: str, parse_mode: str | None = None) -> None:
        if not self.token or not self.chat_id or requests is None:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            payload: dict[str, object] = {
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": True,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            response = requests.post(
                url,
                json=payload,
                timeout=10,
            )
            if not response.ok:
                logging.warning(
                    "Telegram send failed: status=%s body=%s",
                    response.status_code,
                    response.text[:300],
                )
        except Exception as exc:
            logging.warning("Telegram send failed: %s", exc)

    def send_lines(self, title: str, lines: list[str]) -> None:
        rendered = [f"<b>{escape(str(title).strip())}</b>"]
        for line in lines:
            text = str(line).strip()
            if text:
                rendered.append(escape(text))
        self.send("\n".join(rendered), parse_mode="HTML")

    def validate_chat(self) -> tuple[bool, str]:
        if not self.token or not self.chat_id:
            return False, "Telegram token/chat id is missing."
        if requests is None:
            return False, "requests dependency is missing."

        url = f"https://api.telegram.org/bot{self.token}/getChat"
        try:
            response = requests.get(url, params={"chat_id": self.chat_id}, timeout=10)
            if response.ok:
                return True, "Telegram chat is reachable."
            return False, f"Telegram getChat failed: {response.status_code} {response.text[:200]}"
        except Exception as exc:
            return False, f"Telegram validation failed: {exc}"

    def fetch_updates(self, offset: int | None = None, timeout_seconds: int = 0) -> list[dict]:
        if not self.token or requests is None:
            return []

        params: dict[str, int | list[str]] = {"timeout": max(timeout_seconds, 0), "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        try:
            response = requests.get(url, params=params, timeout=max(timeout_seconds, 0) + 10)
            if not response.ok:
                logging.warning(
                    "Telegram getUpdates failed: status=%s body=%s",
                    response.status_code,
                    response.text[:300],
                )
                return []
            payload = response.json()
            if not payload.get("ok"):
                logging.warning("Telegram getUpdates returned not ok: %s", str(payload)[:300])
                return []
            result = payload.get("result", [])
            if isinstance(result, list):
                return [item for item in result if isinstance(item, dict)]
            return []
        except Exception as exc:
            logging.warning("Telegram getUpdates failed: %s", exc)
            return []
