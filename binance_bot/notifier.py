from __future__ import annotations

import logging

try:
    import requests
except Exception:
    requests = None


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id

    def send(self, message: str) -> None:
        if not self.token or not self.chat_id or requests is None:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            response = requests.post(
                url,
                json={"chat_id": self.chat_id, "text": message},
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
