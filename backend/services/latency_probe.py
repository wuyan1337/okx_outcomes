"""Synthetic network probes for OKX routes and the shared Poly direct route.

These measurements are deliberately kept separate from live Sniper execution
latency.  A read RTT cannot measure Poly event -> OKX order acknowledgement.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import math
import socket
import statistics
import time
from typing import Any, Callable, Mapping, Optional

import aiohttp
from aiohttp_socks import ProxyConnector
from yarl import URL


OKX_BASE_URLS = {
    "AU": "https://us.okx.com",
    "CN": "https://www.okx.com",
}
POLY_CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_WARM_SAMPLES = 5
MAX_WARM_SAMPLES = 10
PROBE_TIMEOUT = aiohttp.ClientTimeout(
    total=6,
    connect=3,
    sock_connect=3,
    sock_read=4,
)


class ProbeFailure(Exception):
    """A short, safe-to-return description of a failed probe request."""

    def __init__(
        self,
        error_type: str,
        message: str,
        *,
        http_status: Optional[int] = None,
        upstream_code: Optional[str] = None,
    ):
        super().__init__(message)
        self.error_type = error_type
        self.message = message
        self.http_status = http_status
        self.upstream_code = upstream_code

    def as_dict(self) -> dict:
        result = {"type": self.error_type, "message": self.message}
        if self.http_status is not None:
            result["http_status"] = self.http_status
        if self.upstream_code not in (None, ""):
            result["upstream_code"] = self.upstream_code
        return result


HeaderFactory = Callable[[Mapping[str, Any]], Mapping[str, str]]


def normalise_region(region: str) -> str:
    value = str(region or "AU").strip().upper()
    if value not in OKX_BASE_URLS:
        raise ValueError("region must be AU or CN")
    return value


def normalise_samples(samples: Any) -> int:
    try:
        value = int(samples)
    except (TypeError, ValueError) as exc:
        raise ValueError("samples must be an integer") from exc
    if not 1 <= value <= MAX_WARM_SAMPLES:
        raise ValueError(f"samples must be between 1 and {MAX_WARM_SAMPLES}")
    return value


def _safe_identifier(value: str, field: str) -> str:
    result = str(value or "").strip()
    if len(result) > 512:
        raise ValueError(f"{field} is too long")
    return result


def _okx_target(region: str, yes_asset_id: str = "") -> dict:
    base_url = OKX_BASE_URLS[normalise_region(region)]
    asset_id = _safe_identifier(yes_asset_id, "yes_asset_id")
    if asset_id:
        params = {"instId": asset_id, "sz": "10"}
        endpoint_path = "/api/v5/market/pm-books"
        request_url = URL(base_url + endpoint_path).with_query(params)
        return {
            "url": base_url + endpoint_path,
            "params": params,
            "scope": "market_read",
            "endpoint_path": endpoint_path,
            # Use the same URL implementation as aiohttp. Characters such as
            # ':' are encoded differently by urllib.parse.urlencode, which
            # would make the HMAC path differ from the request target.
            "signed_path": request_url.raw_path_qs,
            "validator": "okx_book",
        }
    return {
        "url": base_url + "/api/v5/public/time",
        "params": None,
        "scope": "connectivity_only",
        "endpoint_path": "/api/v5/public/time",
        "validator": "okx_time",
    }


def _poly_target(poly_token_id: str = "") -> dict:
    token_id = _safe_identifier(poly_token_id, "poly_token_id")
    if token_id:
        return {
            "url": POLY_CLOB_BASE_URL + "/book",
            "params": {"token_id": token_id},
            "scope": "market_read",
            "endpoint_path": "/book",
            "validator": "poly_book",
        }
    return {
        "url": POLY_CLOB_BASE_URL + "/time",
        "params": None,
        "scope": "connectivity_only",
        "endpoint_path": "/time",
        "validator": "poly_time",
    }


def _parse_json(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProbeFailure("invalid_json", "upstream returned invalid JSON") from exc


def _validate_payload(validator: str, body: bytes) -> None:
    payload = _parse_json(body)
    if validator.startswith("okx_"):
        if not isinstance(payload, dict):
            raise ProbeFailure("invalid_response", "OKX response must be an object")
        code = str(payload.get("code", ""))
        if code != "0":
            raise ProbeFailure(
                "upstream_api",
                str(payload.get("msg") or "OKX returned an error")[:160],
                upstream_code=code,
            )
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise ProbeFailure("empty_data", "OKX response data is empty")
        if validator == "okx_book":
            book = data[0]
            if (
                not isinstance(book, dict)
                or not isinstance(book.get("bids"), list)
                or not isinstance(book.get("asks"), list)
            ):
                raise ProbeFailure("invalid_response", "OKX book snapshot is invalid")
        return

    if validator == "poly_book":
        if not isinstance(payload, dict):
            raise ProbeFailure("invalid_response", "Poly book response must be an object")
        if not isinstance(payload.get("bids"), list) or not isinstance(payload.get("asks"), list):
            raise ProbeFailure("invalid_response", "Poly book response is missing bids or asks")
        return

    if validator == "poly_time":
        if isinstance(payload, bool):
            raise ProbeFailure("invalid_response", "Poly time response is invalid")
        try:
            server_time = float(payload)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ProbeFailure(
                "invalid_response",
                "Poly time response is not a timestamp",
            ) from exc
        if not math.isfinite(server_time) or server_time <= 0:
            raise ProbeFailure("invalid_response", "Poly time response is invalid")
        return

    raise ProbeFailure("internal", "unknown probe validator")


def _classify_exception(exc: Exception) -> ProbeFailure:
    if isinstance(exc, ProbeFailure):
        return exc
    if isinstance(exc, asyncio.TimeoutError):
        return ProbeFailure("timeout", "request timed out")
    dns_error = getattr(aiohttp, "ClientConnectorDNSError", None)
    if dns_error is not None and isinstance(exc, dns_error):
        return ProbeFailure("dns", "DNS lookup failed")
    if isinstance(exc, socket.gaierror) or isinstance(
        getattr(exc, "os_error", None), socket.gaierror
    ):
        return ProbeFailure("dns", "DNS lookup failed")
    tls_types = tuple(
        cls
        for cls in (
            getattr(aiohttp, "ClientConnectorCertificateError", None),
            getattr(aiohttp, "ClientSSLError", None),
        )
        if cls is not None
    )
    if tls_types and isinstance(exc, tls_types):
        return ProbeFailure("tls", "TLS connection failed")
    name = type(exc).__name__.lower()
    if "proxy" in name or "socks" in name:
        return ProbeFailure("proxy_connect", "SOCKS5 proxy connection failed")
    if isinstance(exc, (aiohttp.ClientConnectionError, OSError)):
        return ProbeFailure("connect", "connection failed")
    return ProbeFailure("internal", "probe failed unexpectedly")


async def _request_once(
    session: aiohttp.ClientSession,
    target: Mapping[str, Any],
    header_factory: Optional[HeaderFactory] = None,
) -> float:
    request_url: Any = target["url"]
    request_params = target.get("params")
    signed_path = str(target.get("signed_path") or "")
    if signed_path:
        prepared_url = URL(str(request_url)).with_query(request_params or {})
        if prepared_url.raw_path_qs != signed_path:
            raise ProbeFailure(
                "internal",
                "signed path does not match the outgoing request URL",
            )
        # Passing the already prepared URL prevents a second query encoder
        # from changing the exact path that was signed.
        request_url = prepared_url
        request_params = None
    headers = dict(header_factory(target)) if header_factory is not None else None
    started = time.perf_counter_ns()
    try:
        request_options = {
            "params": request_params,
            "timeout": PROBE_TIMEOUT,
        }
        if headers is not None:
            request_options["headers"] = headers
        async with session.get(request_url, **request_options) as response:
            body = await response.read()
            if response.status != 200:
                raise ProbeFailure(
                    "http_status",
                    f"upstream returned HTTP {response.status}",
                    http_status=response.status,
                )
        _validate_payload(str(target["validator"]), body)
        return round((time.perf_counter_ns() - started) / 1_000_000, 1)
    except Exception as exc:
        raise _classify_exception(exc) from exc


def _empty_warm_stats(requested: int) -> dict:
    return {
        "requested": requested,
        "attempted": 0,
        "skipped": requested,
        "successes": 0,
        "failures": 0,
        "success_rate": 0.0,
        "samples_ms": [],
        "median_ms": None,
        "min_ms": None,
        "max_ms": None,
        "jitter_ms": None,
        "error_counts": {},
    }


def _warm_stats(requested: int, samples: list[float], failures: int) -> dict:
    result = _empty_warm_stats(requested)
    result.update({
        "attempted": len(samples) + failures,
        "skipped": max(0, requested - len(samples) - failures),
        "successes": len(samples),
        "failures": failures,
        "success_rate": round(len(samples) / requested, 3) if requested else 0.0,
        "samples_ms": samples,
    })
    if samples:
        result.update({
            "median_ms": round(statistics.median(samples), 1),
            "min_ms": round(min(samples), 1),
            "max_ms": round(max(samples), 1),
            "jitter_ms": round(max(samples) - min(samples), 1),
        })
    return result


def _probe_status(cold_ms: Optional[float], warm: Mapping[str, Any]) -> str:
    requested = int(warm.get("requested", 0))
    successes = int(warm.get("successes", 0))
    if cold_ms is not None and successes == requested:
        return "ok"
    minimum = max(1, math.ceil(requested * 0.6))
    if successes >= minimum:
        return "degraded"
    return "failed"


async def _benchmark_session(
    session: aiohttp.ClientSession,
    target: Mapping[str, Any],
    warm_samples: int,
    header_factory: Optional[HeaderFactory] = None,
) -> dict:
    errors = []
    cold_ms: Optional[float] = None
    try:
        cold_ms = await _request_once(session, target, header_factory)
    except ProbeFailure as exc:
        error = exc.as_dict()
        error.update({"phase": "cold", "sample": 0})
        errors.append(error)

    warm_values: list[float] = []
    warm_failures = 0
    # A failed cold connection usually means a dead route.  Short-circuiting
    # prevents every unavailable node from multiplying the endpoint timeout.
    if cold_ms is not None:
        for index in range(1, warm_samples + 1):
            try:
                warm_values.append(
                    await _request_once(session, target, header_factory)
                )
            except ProbeFailure as exc:
                warm_failures += 1
                error = exc.as_dict()
                error.update({"phase": "warm", "sample": index})
                errors.append(error)

    warm = _warm_stats(warm_samples, warm_values, warm_failures)
    warm["error_counts"] = dict(Counter(
        str(error.get("type") or "unknown")
        for error in errors
        if error.get("phase") == "warm"
    ))
    return {
        "scope": target["scope"],
        "endpoint_path": target["endpoint_path"],
        "cold_ms": cold_ms,
        "warm": warm,
        "status": _probe_status(cold_ms, warm),
        "errors": errors,
    }


def _node_metadata(node_id: str, node: Mapping[str, Any]) -> dict:
    host = node.get("host")
    route = str(node.get("route") or ("direct" if host is None else "socks5"))
    return {
        "node_id": node_id,
        "label": str(node.get("label") or node_id),
        "location": str(node.get("location") or "Unknown"),
        "route": route,
    }


def _failed_node_result(
    node_id: str,
    node: Mapping[str, Any],
    target: Mapping[str, Any],
    warm_samples: int,
    failure: ProbeFailure,
) -> dict:
    okx = {
        "scope": target["scope"],
        "endpoint_path": target["endpoint_path"],
        "cold_ms": None,
        "warm": _empty_warm_stats(warm_samples),
        "status": "failed",
        "errors": [{**failure.as_dict(), "phase": "setup", "sample": 0}],
    }
    return {
        **_node_metadata(node_id, node),
        "status": "failed",
        "okx_ms": -1,
        "okx": okx,
    }


async def benchmark_proxy_node(
    node_id: str,
    node: Mapping[str, Any],
    *,
    region: str = "AU",
    samples: int = DEFAULT_WARM_SAMPLES,
    yes_asset_id: str = "",
    header_factory: Optional[HeaderFactory] = None,
) -> dict:
    region = normalise_region(region)
    samples = normalise_samples(samples)
    target = _okx_target(region, yes_asset_id)
    if target["scope"] == "market_read" and header_factory is None:
        raise ValueError("authenticated OKX market probes require an account")
    try:
        connector = None
        if node.get("host") is not None:
            port = int(node.get("port"))
            connector = ProxyConnector.from_url(f"socks5://{node['host']}:{port}")
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=PROBE_TIMEOUT,
            trust_env=False,
        ) as session:
            okx = await _benchmark_session(
                session,
                target,
                samples,
                header_factory,
            )
    except Exception as exc:
        failed = _failed_node_result(
            node_id,
            node,
            target,
            samples,
            _classify_exception(exc),
        )
        failed["okx"]["authenticated"] = header_factory is not None
        return failed

    okx["authenticated"] = header_factory is not None
    median = okx["warm"].get("median_ms")
    usable_median = median if okx["status"] != "failed" else None
    return {
        **_node_metadata(node_id, node),
        "status": okx["status"],
        # The legacy scalar must still represent the warm route. Falling
        # back to a successful cold handshake would hide total warm failure.
        "okx_ms": int(round(usable_median)) if usable_median is not None else -1,
        "okx": okx,
    }


async def benchmark_poly_direct(
    *,
    samples: int = DEFAULT_WARM_SAMPLES,
    poly_token_id: str = "",
) -> dict:
    samples = normalise_samples(samples)
    target = _poly_target(poly_token_id)
    try:
        async with aiohttp.ClientSession(
            timeout=PROBE_TIMEOUT,
            trust_env=False,
        ) as session:
            result = await _benchmark_session(session, target, samples)
    except Exception as exc:
        failure = _classify_exception(exc)
        result = {
            "scope": target["scope"],
            "endpoint_path": target["endpoint_path"],
            "cold_ms": None,
            "warm": _empty_warm_stats(samples),
            "status": "failed",
            "errors": [{**failure.as_dict(), "phase": "setup", "sample": 0}],
        }
    return {
        "shared": True,
        "route": "direct",
        **result,
    }


async def benchmark_proxy_nodes(
    nodes: Mapping[str, Mapping[str, Any]],
    *,
    region: str = "AU",
    samples: int = DEFAULT_WARM_SAMPLES,
    yes_asset_id: str = "",
    poly_token_id: str = "",
    concurrency: int = 4,
    header_factory: Optional[HeaderFactory] = None,
) -> dict:
    region = normalise_region(region)
    samples = normalise_samples(samples)
    if yes_asset_id and header_factory is None:
        raise ValueError("authenticated OKX market probes require an account")
    concurrency = max(1, min(int(concurrency), 8))
    semaphore = asyncio.Semaphore(concurrency)

    async def run_node(node_id: str, node: Mapping[str, Any]):
        async with semaphore:
            return await benchmark_proxy_node(
                node_id,
                node,
                region=region,
                samples=samples,
                yes_asset_id=yes_asset_id,
                header_factory=header_factory,
            )

    node_items = list(nodes.items())
    combined_results = await asyncio.gather(
        benchmark_poly_direct(samples=samples, poly_token_id=poly_token_id),
        *(run_node(node_id, node) for node_id, node in node_items),
    )
    poly_result, *node_results = combined_results
    return {
        "schema_version": 2,
        "region": region,
        "okx_base_url": OKX_BASE_URLS[region],
        "okx_authenticated": header_factory is not None,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "sample_policy": {
            "cold_requests": 1,
            "warm_requests": samples,
            "warm_statistic": "median_min_max",
            "clock": "perf_counter",
            "node_concurrency": concurrency,
        },
        "poly_direct": poly_result,
        "execution_path": {
            "status": "not_measured",
            "reason": "Synthetic read RTTs do not measure Poly event to OKX order acknowledgement",
        },
        "latencies": {
            node_id: result for (node_id, _node), result in zip(node_items, node_results)
        },
    }


def summarise_error_types(result: Mapping[str, Any]) -> dict:
    """Return compact error counts for CLI/UI consumers."""
    errors = result.get("errors") or []
    return dict(Counter(str(error.get("type") or "unknown") for error in errors))
