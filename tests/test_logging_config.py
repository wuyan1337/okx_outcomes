import json
import logging
import logging.handlers
import queue
import re
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from backend.core import logging_config


class LoggingPathTests(unittest.TestCase):
    def test_path_precedence_and_relative_resolution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = {"logging": {"directory": "from-config"}}

            configured = logging_config.resolve_log_directory(
                config=config,
                environ={},
                project_root=root,
            )
            self.assertEqual(configured, (root / "from-config").resolve())

            from_environment = logging_config.resolve_log_directory(
                config=config,
                environ={"OKX_BOT_LOG_DIR": "from-environment"},
                project_root=root,
            )
            self.assertEqual(
                from_environment, (root / "from-environment").resolve()
            )

            explicit = logging_config.resolve_log_directory(
                "explicit",
                config=config,
                environ={"OKX_BOT_LOG_DIR": "from-environment"},
                project_root=root,
            )
            self.assertEqual(explicit, (root / "explicit").resolve())


class DetailedLoggingTests(unittest.TestCase):
    def setUp(self):
        logging_config.shutdown_logging(restore_previous=True)
        self.temporary = tempfile.TemporaryDirectory()
        self.log_dir = Path(self.temporary.name) / "logs"
        self.config = {
            "logging": {
                "console": False,
                "level": "DEBUG",
                "queues": {
                    "system": 256,
                    "audit": 256,
                    "market": 512,
                },
                "files": {
                    "general": {"max_bytes": 700, "backup_count": 2},
                    "error": {"max_bytes": 700, "backup_count": 2},
                    "audit": {"max_bytes": 2048, "backup_count": 2},
                    "market": {"max_bytes": 2048, "backup_count": 3},
                },
            }
        }

    def tearDown(self):
        logging_config.shutdown_logging(restore_previous=True)
        self.temporary.cleanup()

    def test_configuration_is_idempotent_and_uses_async_queues(self):
        first = logging_config.configure_logging(
            self.config, log_dir=self.log_dir
        )
        root = logging.getLogger()
        audit_logger = logging.getLogger(logging_config.AUDIT_LOGGER_NAME)
        market_logger = logging.getLogger(logging_config.MARKET_LOGGER_NAME)
        root_handlers = tuple(root.handlers)
        audit_handlers = tuple(audit_logger.handlers)
        market_handlers = tuple(market_logger.handlers)

        second = logging_config.configure_logging(
            self.config, log_dir=self.log_dir / "ignored-on-idempotent-call"
        )

        self.assertEqual(first, second)
        self.assertEqual(tuple(root.handlers), root_handlers)
        self.assertEqual(tuple(audit_logger.handlers), audit_handlers)
        self.assertEqual(tuple(market_logger.handlers), market_handlers)
        self.assertEqual(len(root_handlers), 1)
        self.assertIsInstance(root_handlers[0], logging.handlers.QueueHandler)
        self.assertIsInstance(audit_handlers[0], logging.handlers.QueueHandler)
        self.assertIsInstance(market_handlers[0], logging.handlers.QueueHandler)
        self.assertFalse(audit_logger.propagate)
        self.assertFalse(market_logger.propagate)

        status = logging_config.get_logging_status()
        self.assertTrue(status["active"])
        self.assertEqual(status["queues"]["market"]["capacity"], 512)
        self.assertEqual(status["files"]["general"]["max_bytes"], 700)
        self.assertEqual(status["files"]["market"]["backup_count"], 3)
        for name in ("general", "error", "audit", "market"):
            self.assertTrue(Path(status["files"][name]["path"]).exists())

    def test_structured_json_has_high_resolution_time_context_and_redaction(self):
        logging_config.configure_logging(self.config, log_dir=self.log_dir)

        @dataclass
        class Snapshot:
            price: Decimal
            observed_at: datetime
            private_key: str

        with logging_config.log_context(
            account_id="account-1", api_key="context-secret"
        ):
            logging_config.market_event(
                "poly",
                "book.update",
                strategy="sniper",
                bid=Decimal("0.123"),
                levels=[{"price": Decimal("0.123"), "size": Decimal("42.5")}],
                snapshot=Snapshot(
                    Decimal("0.123"),
                    datetime(2026, 7, 27, tzinfo=timezone.utc),
                    "never-write-this",
                ),
                exception=RuntimeError("password=also-secret"),
            )
            logging_config.audit_event(
                "order.request",
                account_id="account-1",
                request={
                    "asset_id": "public-asset-id",
                    "token_id": "public-poly-token-id",
                    "secret_key": "secret-value",
                    "authorization": "Bearer abc.def.ghi",
                },
            )

        self.assertTrue(logging_config.flush_logging(timeout=3.0))

        market_lines = [
            json.loads(line)
            for line in (self.log_dir / logging_config.MARKET_LOG_NAME)
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        market = next(item for item in market_lines if item["event"] == "book.update")
        self.assertRegex(
            market["timestamp"],
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$",
        )
        self.assertIsInstance(market["timestamp_unix_ns"], int)
        self.assertIsInstance(market["monotonic_ns"], int)
        self.assertIsInstance(market["sequence"], int)
        self.assertEqual(market["schema_version"], 1)
        self.assertRegex(market["process_instance_id"], r"^[0-9a-f]{32}$")
        self.assertIsInstance(market["channel_sequence"], int)
        self.assertEqual(market["source"], "poly")
        self.assertEqual(market["context"]["account_id"], "account-1")
        self.assertEqual(market["context"]["api_key"], logging_config.REDACTED)
        self.assertEqual(market["data"]["bid"], "0.123")
        self.assertEqual(
            market["data"]["snapshot"]["private_key"], logging_config.REDACTED
        )
        self.assertEqual(
            market["data"]["exception"]["message"],
            f"password={logging_config.REDACTED}",
        )

        audit_lines = [
            json.loads(line)
            for line in (self.log_dir / logging_config.AUDIT_LOG_NAME)
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        audit = next(item for item in audit_lines if item["event"] == "order.request")
        request = audit["data"]["request"]
        self.assertEqual(request["asset_id"], "public-asset-id")
        self.assertEqual(request["token_id"], "public-poly-token-id")
        self.assertEqual(request["secret_key"], logging_config.REDACTED)
        self.assertEqual(request["authorization"], logging_config.REDACTED)

        raw = (
            (self.log_dir / logging_config.MARKET_LOG_NAME).read_text("utf-8")
            + (self.log_dir / logging_config.AUDIT_LOG_NAME).read_text("utf-8")
        )
        for secret in (
            "context-secret",
            "never-write-this",
            "also-secret",
            "secret-value",
            "abc.def.ghi",
        ):
            self.assertNotIn(secret, raw)

    def test_general_and_error_files_redact_and_rotation_is_bounded(self):
        logging_config.configure_logging(self.config, log_dir=self.log_dir)
        logger = logging.getLogger("backend.tests.logging")
        for index in range(80):
            logger.info(
                "event=%s password=hunter2 payload=%s",
                index,
                "x" * 80,
            )
        logger.error("request failed Authorization: Bearer top.secret.token")

        self.assertTrue(logging_config.flush_logging(timeout=3.0))
        general_files = sorted(self.log_dir.glob("bot.log*"))
        error_files = sorted(self.log_dir.glob("error.log*"))
        self.assertGreater(len(general_files), 1)
        self.assertLessEqual(len(general_files), 3)
        self.assertLessEqual(len(error_files), 3)

        combined = "".join(path.read_text("utf-8") for path in general_files + error_files)
        self.assertNotIn("hunter2", combined)
        self.assertNotIn("top.secret.token", combined)
        self.assertIn(logging_config.REDACTED, combined)
        timestamp = combined.split(" ", 1)[0]
        self.assertTrue(
            re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$",
                timestamp,
            )
        )

    def test_official_okx_headers_and_quoted_credentials_are_redacted(self):
        logging_config.configure_logging(self.config, log_dir=self.log_dir)
        logging.getLogger("backend.tests.credentials").error(
            "OK-ACCESS-KEY: key-value OK-ACCESS-SIGN='signed value' "
            'OK-ACCESS-PASSPHRASE="pass phrase" '
            "X-API-KEY=external-key"
        )
        logging_config.audit_event(
            "credential.scan",
            level=logging.WARNING,
            headers={
                "OK-ACCESS-KEY": "nested-key",
                "OK-ACCESS-SIGN": "nested-signature",
                "OK-ACCESS-PASSPHRASE": "nested-passphrase",
            },
            telegram_url=(
                "https://api.telegram.org/"
                "bot1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi_123456/sendMessage"
            ),
        )
        self.assertTrue(logging_config.flush_logging(timeout=3.0))

        combined = "".join(
            path.read_text("utf-8")
            for pattern in ("bot.log*", "error.log*", "audit.jsonl*")
            for path in self.log_dir.glob(pattern)
        )
        for secret in (
            "key-value",
            "signed value",
            "pass phrase",
            "external-key",
            "nested-key",
            "nested-signature",
            "nested-passphrase",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi_123456",
        ):
            self.assertNotIn(secret, combined)
        self.assertIn(logging_config.REDACTED, combined)

    def test_warning_audit_is_also_written_to_error_log_with_fields(self):
        logging_config.configure_logging(self.config, log_dir=self.log_dir)
        logging_config.audit_event(
            "risk.warning",
            level=logging.WARNING,
            account_id="account-warning",
            stage="cancel-verification",
        )
        self.assertTrue(logging_config.flush_logging(timeout=3.0))

        error_text = "".join(
            path.read_text("utf-8") for path in self.log_dir.glob("error.log*")
        )
        self.assertIn("risk.warning", error_text)
        self.assertIn("account-warning", error_text)
        self.assertIn("cancel-verification", error_text)

    def test_full_queue_is_nonblocking_and_next_record_contains_gap_marker(self):
        record_queue = queue.Queue(maxsize=1)
        handler = logging_config._NonBlockingQueueHandler(record_queue, "market")
        logger = logging.getLogger("tests.queue-gap")
        record_queue.put_nowait(object())
        dropped = logger.makeRecord(
            logger.name,
            logging.INFO,
            __file__,
            1,
            "dropped",
            (),
            None,
            extra={"structured_payload": {"event": "dropped"}},
        )
        started = __import__("time").perf_counter()
        handler.handle(dropped)
        elapsed = __import__("time").perf_counter() - started
        self.assertLess(elapsed, 0.1)
        self.assertEqual(handler.dropped_records, 1)

        record_queue.get_nowait()
        record_queue.task_done()
        recovered = logger.makeRecord(
            logger.name,
            logging.INFO,
            __file__,
            2,
            "recovered",
            (),
            None,
            extra={"structured_payload": {"event": "recovered"}},
        )
        handler.handle(recovered)
        queued = record_queue.get_nowait()
        self.assertEqual(queued.structured_payload["queue_dropped_before"], 1)
        self.assertEqual(handler._unreported_drops, 0)
        handler.close()

    def test_shutdown_drains_and_restores_logger_state(self):
        root = logging.getLogger()
        previous_handlers = tuple(root.handlers)
        previous_level = root.level
        logging_config.configure_logging(self.config, log_dir=self.log_dir)
        logging_config.audit_event("before.shutdown", value="kept")

        self.assertTrue(logging_config.shutdown_logging(restore_previous=True))
        self.assertEqual(tuple(root.handlers), previous_handlers)
        self.assertEqual(root.level, previous_level)
        self.assertFalse(logging_config.get_logging_status()["active"])

        audit_events = [
            json.loads(line)["event"]
            for line in (self.log_dir / logging_config.AUDIT_LOG_NAME)
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertIn("before.shutdown", audit_events)
        self.assertIn("logging.shutdown", audit_events)


if __name__ == "__main__":
    unittest.main()
