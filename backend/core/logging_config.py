"""Central, non-blocking logging for the OKX Outcomes Bot.

The trading hot path only enqueues records.  Dedicated listener threads write
the records to bounded, rotating UTF-8 files.  Audit and market-data records
are kept as one-line JSON so events from different feeds can be correlated by
wall-clock time, monotonic time and an in-process sequence number.

Call :func:`configure_logging` once during process startup and
:func:`shutdown_logging` from the application lifespan shutdown hook.
"""

from __future__ import annotations

import asyncio
import atexit
import contextvars
import dataclasses
import json
import logging
import logging.handlers
import math
import os
import queue
import re
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"

AUDIT_LOGGER_NAME = "okx_bot.audit"
MARKET_LOGGER_NAME = "okx_bot.market"

GENERAL_LOG_NAME = "bot.log"
ERROR_LOG_NAME = "error.log"
AUDIT_LOG_NAME = "audit.jsonl"
MARKET_LOG_NAME = "market_data.jsonl"

REDACTED = "[REDACTED]"


@dataclass(frozen=True)
class RotationPolicy:
    """Size-based retention policy for one log file."""

    max_bytes: int
    backup_count: int

    def __post_init__(self) -> None:
        if self.max_bytes <= 0:
            raise ValueError("max_bytes must be greater than zero")
        if self.backup_count < 0:
            raise ValueError("backup_count cannot be negative")


DEFAULT_ROTATION: dict[str, RotationPolicy] = {
    "general": RotationPolicy(32 * 1024 * 1024, 10),
    "error": RotationPolicy(16 * 1024 * 1024, 12),
    "audit": RotationPolicy(32 * 1024 * 1024, 12),
    # Market traces are intentionally larger: a running strategy can produce
    # many book updates per second, but the size cap still bounds disk usage.
    "market": RotationPolicy(128 * 1024 * 1024, 16),
}

DEFAULT_QUEUE_SIZES = {
    "system": 20_000,
    "audit": 20_000,
    "market": 100_000,
}


@dataclass(frozen=True)
class LoggingPaths:
    directory: Path
    general: Path
    error: Path
    audit: Path
    market: Path


@dataclass(frozen=True)
class LoggingSettings:
    paths: LoggingPaths
    file_level: int = logging.DEBUG
    console_level: int = logging.INFO
    console_enabled: bool = True
    rotation: Mapping[str, RotationPolicy] = field(
        default_factory=lambda: dict(DEFAULT_ROTATION)
    )
    queue_sizes: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_QUEUE_SIZES)
    )


_SENSITIVE_KEYS = frozenset(
    {
        "apikey",
        "apisecret",
        "secret",
        "secretkey",
        "passphrase",
        "password",
        "passwd",
        "privatekey",
        "agentprivatekey",
        "signingkey",
        "bottoken",
        "token",
        "accesstoken",
        "refreshtoken",
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "signature",
        "sign",
        "okaccesskey",
        "okaccesssign",
        "okaccesspassphrase",
        "xapikey",
        "xapisecret",
    }
)

_SENSITIVE_KEY_PATTERN = (
    r"api[_-]?key|api[_-]?secret|secret(?:[_-]?key)?|passphrase|password|passwd|"
    r"private[_-]?key|agent[_-]?private[_-]?key|signing[_-]?key|bot[_-]?token|"
    r"access[_-]?token|refresh[_-]?token|authorization|proxy-authorization|"
    r"cookie|set-cookie|signature|ok-access-key|ok-access-sign|"
    r"ok-access-passphrase|x-api-key|x-api-secret"
)

_QUOTED_SECRET_RE = re.compile(
    rf"(?i)(?P<prefix>[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']\s*:\s*)"
    rf"(?P<quote>[\"'])(?P<value>.*?)(?<!\\)(?P=quote)"
)
_ASSIGNED_QUOTED_SECRET_RE = re.compile(
    rf"(?i)(?P<prefix>\b(?:{_SENSITIVE_KEY_PATTERN})\b\s*(?:=|:)\s*)"
    rf"(?P<quote>[\"'])(?P<value>.*?)(?<!\\)(?P=quote)"
)
_ASSIGNED_SECRET_RE = re.compile(
    rf"(?i)(?P<prefix>\b(?:{_SENSITIVE_KEY_PATTERN})\b\s*(?:=|:)\s*)"
    rf"(?P<value>[^\s,;&}}]+)"
)
_QUERY_SECRET_RE = re.compile(
    rf"(?i)(?P<prefix>[?&](?:{_SENSITIVE_KEY_PATTERN})=)(?P<value>[^&#\s]+)"
)
_BEARER_RE = re.compile(r"(?i)(\bBearer\s+)[A-Za-z0-9._~+/=-]+")
_URL_QUERY_RE = re.compile(
    r"(?i)(?P<path>(?:https?://[^\s\"'<>?]+|/[^\s\"'<>?]+))"
    r"\?[^\s\"'<>]+"
)
_TELEGRAM_TOKEN_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9_])(?:bot)?\d{6,}:[A-Za-z0-9_-]{20,}\b"
)


def _normalise_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def _is_sensitive_key(key: Any) -> bool:
    return _normalise_key(key) in _SENSITIVE_KEYS


def redact_text(value: Any) -> str:
    """Redact common credential forms from an arbitrary text value."""

    text = str(value)
    # Query values are never useful enough to justify persisting arbitrary
    # credentials/search input that an HTTP exception may echo in its URL.
    text = _URL_QUERY_RE.sub(
        lambda match: f"{match.group('path')}?<query-redacted>", text
    )
    text = _QUOTED_SECRET_RE.sub(
        lambda match: f"{match.group('prefix')}\"{REDACTED}\"", text
    )
    text = _ASSIGNED_QUOTED_SECRET_RE.sub(
        lambda match: (
            f"{match.group('prefix')}{match.group('quote')}"
            f"{REDACTED}{match.group('quote')}"
        ),
        text,
    )
    # Remove a complete Authorization bearer value before the generic
    # ``key: value`` rule sees only the word "Bearer".
    text = _BEARER_RE.sub(lambda match: f"{match.group(1)}{REDACTED}", text)
    text = _QUERY_SECRET_RE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", text
    )
    text = _ASSIGNED_SECRET_RE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", text
    )
    return _TELEGRAM_TOKEN_RE.sub(REDACTED, text)


def sanitise_for_log(
    value: Any,
    *,
    key: Any = None,
    _seen: Optional[set[int]] = None,
    _depth: int = 0,
) -> Any:
    """Return a JSON-safe, recursively redacted snapshot of ``value``.

    A snapshot is made before a record enters the asynchronous queue.  This
    prevents a mutable order-book object from changing before it is written.
    """

    if key is not None and _is_sensitive_key(key):
        return REDACTED
    if _depth > 24:
        return "<maximum-log-depth>"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return sanitise_for_log(
            value.value, _seen=_seen, _depth=_depth + 1
        )
    if isinstance(value, bytes):
        return {"type": "bytes", "length": len(value)}
    if isinstance(value, BaseException):
        return {
            "type": type(value).__name__,
            "message": redact_text(value),
        }

    if _seen is None:
        _seen = set()
    object_id = id(value)
    if object_id in _seen:
        return "<circular-reference>"

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        _seen.add(object_id)
        try:
            return {
                item.name: sanitise_for_log(
                    getattr(value, item.name),
                    key=item.name,
                    _seen=_seen,
                    _depth=_depth + 1,
                )
                for item in dataclasses.fields(value)
            }
        finally:
            _seen.discard(object_id)

    if isinstance(value, Mapping):
        _seen.add(object_id)
        try:
            return {
                redact_text(map_key): sanitise_for_log(
                    map_value,
                    key=map_key,
                    _seen=_seen,
                    _depth=_depth + 1,
                )
                for map_key, map_value in value.items()
            }
        finally:
            _seen.discard(object_id)

    if isinstance(value, (list, tuple, set, frozenset)):
        _seen.add(object_id)
        try:
            return [
                sanitise_for_log(item, _seen=_seen, _depth=_depth + 1)
                for item in value
            ]
        finally:
            _seen.discard(object_id)

    try:
        return redact_text(value)
    except Exception:
        return f"<{type(value).__name__}>"


class _UtcFormatter(logging.Formatter):
    """Human-readable formatter with UTC millisecond timestamps."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        moment = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def formatException(self, exc_info: Any) -> str:
        return redact_text(super().formatException(exc_info))

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        payload = getattr(record, "structured_payload", None)
        if isinstance(payload, Mapping):
            # WARNING/ERROR audit events are mirrored into error.log.  Keep
            # their structured diagnostic fields there instead of reducing
            # the record to only its event name.
            rendered += " structured=" + json.dumps(
                sanitise_for_log(payload),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        return redact_text(rendered)


class _JsonLineFormatter(logging.Formatter):
    """Serialize structured records to exactly one UTF-8 JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        original = getattr(record, "structured_payload", None)
        if isinstance(original, Mapping):
            payload = dict(original)
        else:
            wall_ns = int(record.created * 1_000_000_000)
            payload = {
                "timestamp": _utc_timestamp_ns(wall_ns),
                "timestamp_unix_ns": wall_ns,
                "event": redact_text(record.getMessage()),
            }
        payload.setdefault("level", record.levelname)
        payload.setdefault("logger", record.name)
        payload.setdefault("process_id", record.process)
        payload.setdefault("thread", record.threadName)
        if record.exc_info:
            payload["exception"] = redact_text(
                logging.Formatter().formatException(record.exc_info)
            )
        elif record.exc_text:
            payload["exception"] = redact_text(record.exc_text)
        safe_payload = sanitise_for_log(payload)
        return json.dumps(
            safe_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )


class _NonBlockingQueueHandler(logging.handlers.QueueHandler):
    """Queue handler that never waits on disk and records bounded-queue loss."""

    def __init__(self, record_queue: queue.Queue[logging.LogRecord], channel: str):
        super().__init__(record_queue)
        self.channel = channel
        self.accepting = True
        self.dropped_records = 0
        self._unreported_drops = 0
        self._last_warning_ns = 0
        self._drop_lock = threading.Lock()
        self._okx_bot_managed = True

    def enqueue(self, record: logging.LogRecord) -> None:
        if not self.accepting:
            self._record_drop()
            return
        try:
            with self._drop_lock:
                unreported = self._unreported_drops
                if unreported:
                    payload = getattr(record, "structured_payload", None)
                    if isinstance(payload, dict):
                        payload["queue_dropped_before"] = unreported
                    else:
                        record.msg = f"[logging dropped {unreported} earlier records] {record.getMessage()}"
                        record.args = None
                self.queue.put_nowait(record)
                if unreported:
                    self._unreported_drops -= unreported
        except queue.Full:
            self._record_drop()

    def _record_drop(self) -> None:
        now_ns = time.monotonic_ns()
        warn = False
        with self._drop_lock:
            self.dropped_records += 1
            self._unreported_drops += 1
            if self._last_warning_ns == 0 or now_ns - self._last_warning_ns >= 60_000_000_000:
                self._last_warning_ns = now_ns
                warn = True
            count = self.dropped_records
        if warn:
            try:
                sys.stderr.write(
                    f"logging queue full: channel={self.channel} dropped={count}\n"
                )
            except Exception:
                pass


class _BlockingStopQueueListener(logging.handlers.QueueListener):
    """Let shutdown enqueue its sentinel after a full queue has drained."""

    def enqueue_sentinel(self) -> None:
        self.queue.put(self._sentinel, block=True, timeout=10.0)


@dataclass
class _ChannelRuntime:
    name: str
    record_queue: queue.Queue[logging.LogRecord]
    queue_handler: _NonBlockingQueueHandler
    listener: _BlockingStopQueueListener
    output_handlers: tuple[logging.Handler, ...]


@dataclass
class _LoggingRuntime:
    settings: LoggingSettings
    channels: tuple[_ChannelRuntime, ...]
    root_handlers_before: tuple[logging.Handler, ...]
    root_level_before: int
    audit_handlers_before: tuple[logging.Handler, ...]
    audit_level_before: int
    audit_propagate_before: bool
    market_handlers_before: tuple[logging.Handler, ...]
    market_level_before: int
    market_propagate_before: bool


_runtime: Optional[_LoggingRuntime] = None
_lifecycle_lock = threading.RLock()
_sequence_lock = threading.Lock()
_sequence = 0
_channel_sequences: dict[str, int] = {}
_process_instance_id = uuid.uuid4().hex
_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "okx_bot_log_context", default={}
)


def _as_level(value: Any, fallback: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        resolved = getattr(logging, value.upper(), None)
        if isinstance(resolved, int):
            return resolved
    return fallback


def _as_positive_int(value: Any, fallback: int, *, allow_zero: bool = False) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return fallback
    minimum = 0 if allow_zero else 1
    return resolved if resolved >= minimum else fallback


def _as_bool(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"1", "true", "yes", "on"}:
            return True
        if normalised in {"0", "false", "no", "off"}:
            return False
    return fallback


def _logging_section(config: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not isinstance(config, Mapping):
        return {}
    nested = config.get("logging")
    if isinstance(nested, Mapping):
        return nested
    return config


def resolve_log_directory(
    log_dir: str | os.PathLike[str] | None = None,
    *,
    config: Optional[Mapping[str, Any]] = None,
    environ: Optional[Mapping[str, str]] = None,
    project_root: str | os.PathLike[str] | None = None,
) -> Path:
    """Resolve log location using explicit, environment, config, default order."""

    env = os.environ if environ is None else environ
    section = _logging_section(config)
    selected: Any = log_dir
    if selected is None:
        selected = env.get("OKX_BOT_LOG_DIR")
    if selected is None:
        selected = section.get("directory", section.get("path"))
    root = Path(project_root).expanduser() if project_root else PROJECT_ROOT
    if selected in (None, ""):
        return (root / "logs").resolve()
    path = Path(os.path.expandvars(str(selected))).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def build_logging_settings(
    config: Optional[Mapping[str, Any]] = None,
    *,
    log_dir: str | os.PathLike[str] | None = None,
    environ: Optional[Mapping[str, str]] = None,
    project_root: str | os.PathLike[str] | None = None,
) -> LoggingSettings:
    """Build validated settings from the application config and environment."""

    env = os.environ if environ is None else environ
    section = _logging_section(config)
    directory = resolve_log_directory(
        log_dir,
        config=config,
        environ=env,
        project_root=project_root,
    )
    paths = LoggingPaths(
        directory=directory,
        general=directory / GENERAL_LOG_NAME,
        error=directory / ERROR_LOG_NAME,
        audit=directory / AUDIT_LOG_NAME,
        market=directory / MARKET_LOG_NAME,
    )

    file_level = _as_level(
        env.get("OKX_BOT_LOG_LEVEL", section.get("level")), logging.DEBUG
    )
    console_level = _as_level(section.get("console_level"), logging.INFO)
    console_enabled = _as_bool(section.get("console", True), True)

    queue_section = section.get("queues", {})
    if not isinstance(queue_section, Mapping):
        queue_section = {}
    queue_sizes = {
        name: _as_positive_int(
            queue_section.get(f"{name}_size", queue_section.get(name)), default
        )
        for name, default in DEFAULT_QUEUE_SIZES.items()
    }

    files_section = section.get("files", {})
    if not isinstance(files_section, Mapping):
        files_section = {}
    rotation: dict[str, RotationPolicy] = {}
    for name, default in DEFAULT_ROTATION.items():
        raw_policy = files_section.get(name, {})
        if not isinstance(raw_policy, Mapping):
            raw_policy = {}
        rotation[name] = RotationPolicy(
            max_bytes=_as_positive_int(raw_policy.get("max_bytes"), default.max_bytes),
            backup_count=_as_positive_int(
                raw_policy.get("backup_count"),
                default.backup_count,
                allow_zero=True,
            ),
        )

    return LoggingSettings(
        paths=paths,
        file_level=file_level,
        console_level=console_level,
        console_enabled=console_enabled,
        rotation=rotation,
        queue_sizes=queue_sizes,
    )


def _rotating_handler(
    path: Path,
    policy: RotationPolicy,
    formatter: logging.Formatter,
    level: int,
) -> logging.handlers.RotatingFileHandler:
    handler = logging.handlers.RotatingFileHandler(
        path,
        mode="a",
        maxBytes=policy.max_bytes,
        backupCount=policy.backup_count,
        encoding="utf-8",
        delay=False,
        errors="backslashreplace",
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    handler._okx_bot_managed = True
    try:
        os.chmod(path, 0o640)
    except OSError:
        pass
    return handler


def _new_channel(
    name: str,
    size: int,
    handlers: tuple[logging.Handler, ...],
) -> _ChannelRuntime:
    record_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=size)
    queue_handler = _NonBlockingQueueHandler(record_queue, name)
    queue_handler.setLevel(logging.DEBUG)
    listener = _BlockingStopQueueListener(
        record_queue, *handlers, respect_handler_level=True
    )
    return _ChannelRuntime(name, record_queue, queue_handler, listener, handlers)


def configure_logging(
    config: Optional[Mapping[str, Any]] = None,
    *,
    log_dir: str | os.PathLike[str] | None = None,
    force: bool = False,
) -> LoggingPaths:
    """Configure all application logging and return the concrete file paths.

    Repeated calls are idempotent.  Pass ``force=True`` only when deliberately
    changing the logging configuration in a running process.
    """

    global _runtime
    with _lifecycle_lock:
        if _runtime is not None and not force:
            return _runtime.settings.paths
        if _runtime is not None:
            _shutdown_locked(restore_previous=True)

        settings = build_logging_settings(config, log_dir=log_dir)
        settings.paths.directory.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(settings.paths.directory, 0o750)
        except OSError:
            pass

        human_formatter = _UtcFormatter(
            "%(asctime)s %(levelname)s [%(name)s] "
            "[pid=%(process)d thread=%(threadName)s] %(message)s"
        )
        json_formatter = _JsonLineFormatter()
        created_handlers: list[logging.Handler] = []
        started_channels: list[_ChannelRuntime] = []
        try:
            general_handler = _rotating_handler(
                settings.paths.general,
                settings.rotation["general"],
                human_formatter,
                settings.file_level,
            )
            created_handlers.append(general_handler)
            error_handler = _rotating_handler(
                settings.paths.error,
                settings.rotation["error"],
                human_formatter,
                logging.WARNING,
            )
            created_handlers.append(error_handler)
            audit_handler = _rotating_handler(
                settings.paths.audit,
                settings.rotation["audit"],
                json_formatter,
                logging.DEBUG,
            )
            created_handlers.append(audit_handler)
            market_handler = _rotating_handler(
                settings.paths.market,
                settings.rotation["market"],
                json_formatter,
                logging.DEBUG,
            )
            created_handlers.append(market_handler)

            system_outputs: list[logging.Handler] = [
                general_handler,
                error_handler,
            ]
            if settings.console_enabled:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(settings.console_level)
                console_handler.setFormatter(human_formatter)
                console_handler._okx_bot_managed = True
                system_outputs.append(console_handler)
                created_handlers.append(console_handler)

            channels = (
                _new_channel(
                    "system", settings.queue_sizes["system"], tuple(system_outputs)
                ),
                # A single shared error handler is safe here because Handler
                # serialises writes with its own lock.  This mirrors audit
                # WARNING/ERROR records into error.log without opening the
                # same rotating file twice.
                _new_channel(
                    "audit",
                    settings.queue_sizes["audit"],
                    (audit_handler, error_handler),
                ),
                _new_channel(
                    "market", settings.queue_sizes["market"], (market_handler,)
                ),
            )
            for channel in channels:
                channel.listener.start()
                started_channels.append(channel)

            root = logging.getLogger()
            audit_logger = logging.getLogger(AUDIT_LOGGER_NAME)
            market_logger = logging.getLogger(MARKET_LOGGER_NAME)
            runtime = _LoggingRuntime(
                settings=settings,
                channels=channels,
                root_handlers_before=tuple(root.handlers),
                root_level_before=root.level,
                audit_handlers_before=tuple(audit_logger.handlers),
                audit_level_before=audit_logger.level,
                audit_propagate_before=audit_logger.propagate,
                market_handlers_before=tuple(market_logger.handlers),
                market_level_before=market_logger.level,
                market_propagate_before=market_logger.propagate,
            )

            root.handlers[:] = [channels[0].queue_handler]
            # error.log always keeps WARNING+ even when the general file is
            # intentionally configured at ERROR.
            enabled_levels = [settings.file_level, logging.WARNING]
            if settings.console_enabled:
                enabled_levels.append(settings.console_level)
            root.setLevel(min(enabled_levels))

            audit_logger.handlers[:] = [channels[1].queue_handler]
            audit_logger.setLevel(logging.DEBUG)
            audit_logger.propagate = False
            market_logger.handlers[:] = [channels[2].queue_handler]
            market_logger.setLevel(logging.DEBUG)
            market_logger.propagate = False
            _runtime = runtime
        except Exception:
            for channel in started_channels:
                try:
                    channel.listener.stop()
                except Exception:
                    pass
            for handler in created_handlers:
                try:
                    handler.close()
                except Exception:
                    pass
            raise

        audit_event(
            "logging.initialized",
            log_directory=settings.paths.directory,
            file_level=logging.getLevelName(settings.file_level),
            queue_sizes=settings.queue_sizes,
            rotation={
                name: dataclasses.asdict(policy)
                for name, policy in settings.rotation.items()
            },
        )
        return settings.paths


def _utc_timestamp_ns(wall_ns: int) -> str:
    seconds, nanoseconds = divmod(wall_ns, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{nanoseconds:09d}Z"


def _next_sequence() -> int:
    global _sequence
    with _sequence_lock:
        _sequence += 1
        return _sequence


def _next_channel_sequence(channel: str) -> int:
    with _sequence_lock:
        value = _channel_sequences.get(channel, 0) + 1
        _channel_sequences[channel] = value
        return value


def _current_task_name() -> Optional[str]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    try:
        return task.get_name()
    except AttributeError:
        return None


def _emit_structured(
    logger_name: str,
    channel: str,
    event: str,
    *,
    level: int = logging.INFO,
    source: Optional[str] = None,
    fields: Mapping[str, Any],
) -> None:
    wall_ns = time.time_ns()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "process_instance_id": _process_instance_id,
        "timestamp": _utc_timestamp_ns(wall_ns),
        "timestamp_unix_ns": wall_ns,
        "monotonic_ns": time.monotonic_ns(),
        "sequence": _next_sequence(),
        "channel_sequence": _next_channel_sequence(channel),
        "channel": channel,
        "event": redact_text(event),
    }
    if source is not None:
        payload["source"] = redact_text(source)
    task_name = _current_task_name()
    if task_name:
        payload["task"] = redact_text(task_name)
    context = _log_context.get()
    if context:
        payload["context"] = sanitise_for_log(context)
    if fields:
        payload["data"] = sanitise_for_log(fields)
    logging.getLogger(logger_name).log(
        level,
        redact_text(event),
        extra={"structured_payload": payload},
    )


def market_event(
    source: str,
    event: str = "book_update",
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Enqueue one immutable Poly/OKX market or strategy timing snapshot."""

    _emit_structured(
        MARKET_LOGGER_NAME,
        "market",
        event,
        level=level,
        source=source,
        fields=fields,
    )


def audit_event(
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Enqueue one structured lifecycle/order/risk-control audit event."""

    _emit_structured(
        AUDIT_LOGGER_NAME,
        "audit",
        event,
        level=level,
        fields=fields,
    )


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Attach account/strategy/correlation fields to nested structured events."""

    merged = dict(_log_context.get())
    merged.update(sanitise_for_log(fields))
    token = _log_context.set(merged)
    try:
        yield
    finally:
        _log_context.reset(token)


def get_logging_status() -> dict[str, Any]:
    """Return paths, retention and queue/drop health without exposing secrets."""

    with _lifecycle_lock:
        if _runtime is None:
            return {"active": False}
        runtime = _runtime
        settings = runtime.settings
        return {
            "active": True,
            "schema_version": 1,
            "process_instance_id": _process_instance_id,
            "directory": str(settings.paths.directory),
            "files": {
                name: {
                    "path": str(getattr(settings.paths, name)),
                    "max_bytes": settings.rotation[name].max_bytes,
                    "backup_count": settings.rotation[name].backup_count,
                }
                for name in ("general", "error", "audit", "market")
            },
            "queues": {
                channel.name: {
                    "size": channel.record_queue.qsize(),
                    "capacity": channel.record_queue.maxsize,
                    "dropped_records": channel.queue_handler.dropped_records,
                    "unreported_drops": channel.queue_handler._unreported_drops,
                }
                for channel in runtime.channels
            },
        }


def _flush_runtime(
    runtime: _LoggingRuntime,
    *,
    timeout: float,
    fsync: bool,
) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    drained = False
    while True:
        pending = sum(
            int(getattr(channel.record_queue, "unfinished_tasks", 0))
            for channel in runtime.channels
        )
        if pending == 0:
            drained = True
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(0.005)

    for channel in runtime.channels:
        for handler in channel.output_handlers:
            try:
                handler.flush()
                stream = getattr(handler, "stream", None)
                if fsync and stream is not None and hasattr(stream, "fileno"):
                    os.fsync(stream.fileno())
            except (OSError, ValueError):
                drained = False
    return drained


def flush_logging(timeout: float = 5.0, *, fsync: bool = False) -> bool:
    """Wait for queued records and flush file buffers; return whether drained."""

    with _lifecycle_lock:
        runtime = _runtime
    if runtime is None:
        return True
    return _flush_runtime(runtime, timeout=timeout, fsync=fsync)


def _restore_handlers(
    logger: logging.Logger,
    managed_handler: logging.Handler,
    previous: tuple[logging.Handler, ...],
) -> None:
    logger.handlers[:] = [
        handler for handler in logger.handlers if handler is not managed_handler
    ]
    for handler in previous:
        if handler not in logger.handlers:
            logger.addHandler(handler)


def _shutdown_locked(*, restore_previous: bool) -> bool:
    global _runtime
    runtime = _runtime
    if runtime is None:
        return True

    audit_event("logging.shutdown")
    root = logging.getLogger()
    audit_logger = logging.getLogger(AUDIT_LOGGER_NAME)
    market_logger = logging.getLogger(MARKET_LOGGER_NAME)
    _restore_handlers(
        root, runtime.channels[0].queue_handler, runtime.root_handlers_before
    )
    _restore_handlers(
        audit_logger,
        runtime.channels[1].queue_handler,
        runtime.audit_handlers_before,
    )
    _restore_handlers(
        market_logger,
        runtime.channels[2].queue_handler,
        runtime.market_handlers_before,
    )
    for channel in runtime.channels:
        channel.queue_handler.accepting = False

    drained = _flush_runtime(runtime, timeout=10.0, fsync=True)
    for channel in runtime.channels:
        try:
            channel.listener.stop()
        except Exception:
            drained = False
        for handler in channel.output_handlers:
            try:
                handler.close()
            except Exception:
                drained = False
        channel.queue_handler.close()

    if restore_previous:
        root.setLevel(runtime.root_level_before)
        audit_logger.setLevel(runtime.audit_level_before)
        audit_logger.propagate = runtime.audit_propagate_before
        market_logger.setLevel(runtime.market_level_before)
        market_logger.propagate = runtime.market_propagate_before
    else:
        root.handlers.clear()
        audit_logger.handlers.clear()
        market_logger.handlers.clear()
    _runtime = None
    return drained


def shutdown_logging(*, restore_previous: bool = True) -> bool:
    """Drain queues, fsync files, stop listeners and close owned handlers."""

    with _lifecycle_lock:
        return _shutdown_locked(restore_previous=restore_previous)


atexit.register(shutdown_logging)
