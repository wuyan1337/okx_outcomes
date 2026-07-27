import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import Request
from fastapi.responses import JSONResponse

from backend import app as app_module
from backend.core import logging_config


def make_request(query_string: bytes, path: str = "/api/test") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 12345),
            "root_path": "",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": query_string,
            "headers": [],
        }
    )


class HttpAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_4xx_and_5xx_are_mirrored_to_error_log(self):
        logging_config.shutdown_logging(restore_previous=True)
        with tempfile.TemporaryDirectory() as temporary:
            log_dir = Path(temporary) / "logs"
            logging_config.configure_logging(
                {"logging": {"console": False}}, log_dir=log_dir
            )

            async def call_with_status(status_code):
                async def call_next(_request):
                    return JSONResponse({"status": status_code}, status_code=status_code)

                return await app_module.request_audit_middleware(
                    make_request(b"", path=f"/status/{status_code}"), call_next
                )

            try:
                responses = [
                    await call_with_status(200),
                    await call_with_status(404),
                    await call_with_status(503),
                ]
                self.assertEqual(
                    [response.status_code for response in responses], [200, 404, 503]
                )
                self.assertTrue(logging_config.flush_logging(timeout=3.0))

                completed = {
                    item["data"]["status_code"]: item
                    for item in (
                        json.loads(line)
                        for line in (log_dir / logging_config.AUDIT_LOG_NAME)
                        .read_text("utf-8")
                        .splitlines()
                    )
                    if item["event"] == "http.request.completed"
                }
                self.assertEqual(completed[200]["level"], "INFO")
                self.assertEqual(completed[404]["level"], "WARNING")
                self.assertEqual(completed[503]["level"], "ERROR")

                error_text = "".join(
                    path.read_text("utf-8") for path in log_dir.glob("error.log*")
                )
                self.assertIn("/status/404", error_text)
                self.assertIn("/status/503", error_text)
                self.assertNotIn("/status/200", error_text)
            finally:
                logging_config.shutdown_logging(restore_previous=True)

    async def test_query_values_are_never_logged_and_metadata_is_bounded(self):
        long_key = "k" * 200
        query = (
            b"token=never-log-this-query-value&normal=another-secret&"
            + long_key.encode("ascii")
            + b"=third-secret"
        )
        request = make_request(query)
        events = []

        async def call_next(_request):
            return JSONResponse({"ok": True})

        with mock.patch.object(
            app_module,
            "audit_event",
            side_effect=lambda event, **fields: events.append((event, fields)),
        ):
            response = await app_module.request_audit_middleware(request, call_next)

        self.assertEqual(response.status_code, 200)
        started = next(fields for event, fields in events if event == "http.request.started")
        self.assertNotIn("query", started)
        self.assertEqual(started["query_key_count"], 3)
        self.assertEqual(started["query_keys"][:2], ["token", "normal"])
        self.assertEqual(len(started["query_keys"][2]), 65)  # 64 chars + ellipsis
        self.assertTrue(started["query_truncated"])
        self.assertEqual(started["query_length"], len(query))
        serialised = json.dumps(events)
        for secret in (
            "never-log-this-query-value",
            "another-secret",
            "third-secret",
        ):
            self.assertNotIn(secret, serialised)


class RepeatedLifespanLoggingTests(unittest.IsolatedAsyncioTestCase):
    async def test_second_lifespan_reinitialises_logging(self):
        logging_config.shutdown_logging(restore_previous=True)
        with tempfile.TemporaryDirectory() as temporary:
            log_dir = Path(temporary) / "logs"
            config = {
                "logging": {
                    "directory": str(log_dir),
                    "console": False,
                },
                "telegram": {},
            }
            manager = SimpleNamespace(
                instances={},
                init=mock.AsyncMock(),
                close_all=mock.AsyncMock(),
            )
            with (
                mock.patch.object(app_module, "config", config),
                mock.patch.object(app_module, "manager", manager),
                mock.patch.object(app_module, "init_db"),
            ):
                async with app_module.lifespan(app_module.app):
                    first_status = logging_config.get_logging_status()
                    self.assertTrue(first_status["active"])
                    self.assertEqual(Path(first_status["directory"]), log_dir)
                self.assertFalse(logging_config.get_logging_status()["active"])

                async with app_module.lifespan(app_module.app):
                    second_status = logging_config.get_logging_status()
                    self.assertTrue(second_status["active"])
                    self.assertEqual(Path(second_status["directory"]), log_dir)
                self.assertFalse(logging_config.get_logging_status()["active"])

            self.assertEqual(manager.init.await_count, 2)
            self.assertEqual(manager.close_all.await_count, 2)
            events = [
                json.loads(line)["event"]
                for line in (log_dir / logging_config.AUDIT_LOG_NAME)
                .read_text("utf-8")
                .splitlines()
            ]
            self.assertEqual(events.count("application.started"), 2)
            self.assertEqual(events.count("application.stopped"), 2)


if __name__ == "__main__":
    unittest.main()
