"""Telegram notification service with global send queue (0.5s between all messages)."""
import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Send notifications to Telegram. All messages queued, 0.5s apart."""

    def __init__(self, token: str, chat_ids: list[str]):
        self.token = token
        self.chat_ids = list(chat_ids)
        self.enabled = bool(token and chat_ids)
        self._session: Optional[aiohttp.ClientSession] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None

    def _ensure_worker(self):
        if self._worker_task is None or self._worker_task.done():
            try:
                self._worker_task = asyncio.create_task(self._queue_worker())
            except RuntimeError:
                pass

    async def _queue_worker(self):
        while True:
            msg = await self._queue.get()
            try:
                await self._do_send(msg)
            except Exception as e:
                logger.warning(f"TG queue worker error: {e}")
            self._queue.task_done()
            await asyncio.sleep(0.5)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._worker_task:
            self._worker_task.cancel()

    def add_chat_id(self, chat_id: str):
        if chat_id not in self.chat_ids:
            self.chat_ids.append(chat_id)
            self.enabled = bool(self.token and self.chat_ids)

    def remove_chat_id(self, chat_id: str):
        if chat_id in self.chat_ids:
            self.chat_ids.remove(chat_id)

    def set_token(self, token: str):
        self.token = token
        self.enabled = bool(self.token and self.chat_ids)

    async def send(self, message: str, **kwargs):
        """Queue message for sending."""
        if not self.enabled:
            return
        self._ensure_worker()
        await self._queue.put(message)

    async def _do_send(self, message: str):
        session = await self._get_session()
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        for chat_id in self.chat_ids:
            try:
                async with session.post(url, json={
                    "chat_id": chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                }) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(2)
                    elif resp.status != 200:
                        data = await resp.json()
                        logger.warning(f"TG send failed to {chat_id}: {data}")
            except Exception as e:
                logger.warning(f"TG send error to {chat_id}: {e}")

    def get_config(self) -> dict:
        return {
            "token": self.token[:10] + "..." if self.token else "",
            "chat_ids": self.chat_ids,
            "enabled": self.enabled,
        }
