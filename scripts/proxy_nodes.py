#!/usr/bin/env python3
"""Manage safe local proxy-node overrides and run latency benchmarks."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "data" / "proxy_nodes.json"
SCHEMA_VERSION = 1
STABLE_ID_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,31}$")
CANONICAL_NAME_RE = re.compile(r"^[A-Z]{2}-[A-Z]{3}-[0-9]{2}$")
SAFE_ASSET_RE = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")
ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
NODE_FIELDS = {"name", "location", "route", "host", "port"}


class RegistryError(RuntimeError):
    """Raised when the registry cannot be used without losing information."""


class RegistryLock:
    """Cross-platform exclusive lock for registry reads and writes."""

    def __init__(self, registry_path: Path) -> None:
        self.path = registry_path.with_name(registry_path.name + ".lock")
        self.handle: Any = None

    def __enter__(self) -> "RegistryLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        if os.name != "nt":
            os.chmod(self.path, 0o600)
        self.handle.seek(0, os.SEEK_END)
        if self.handle.tell() == 0:
            self.handle.write(b"\0")
            self.handle.flush()
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self.handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is None:
            return
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()
        self.handle = None


def _backend_registry_api():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from backend.core.proxy_nodes import (
            BUILTIN_PROXY_NODES,
            RETIRED_PROXY_NODE_IDS,
            load_proxy_nodes,
            normalise_proxy_node,
        )
    except ImportError as exc:
        raise RegistryError(f"backend proxy-node registry is unavailable: {exc}") from exc
    return (
        BUILTIN_PROXY_NODES,
        RETIRED_PROXY_NODE_IDS,
        load_proxy_nodes,
        normalise_proxy_node,
    )


def _empty_registry() -> dict[str, Any]:
    return {"version": SCHEMA_VERSION, "nodes": {}}


def _text(value: Any, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(f"{field} must be a non-empty string")
    result = value.strip()
    if len(result) > maximum or any(ord(char) < 32 for char in result):
        raise RegistryError(f"{field} is too long or contains control characters")
    return result


def _normalise_custom_node(node_id: str, raw: Any) -> dict[str, Any]:
    if not STABLE_ID_RE.fullmatch(node_id):
        raise RegistryError(f"invalid stable node id: {node_id!r}")
    if not isinstance(raw, Mapping):
        raise RegistryError(f"node {node_id} must be an object")
    unknown = sorted(set(raw) - NODE_FIELDS)
    missing = sorted(NODE_FIELDS - set(raw))
    if unknown:
        raise RegistryError(f"node {node_id} has unknown fields: {', '.join(unknown)}")
    if missing:
        raise RegistryError(f"node {node_id} is missing fields: {', '.join(missing)}")

    name = _text(raw.get("name"), f"{node_id}.name", 32).upper()
    if not CANONICAL_NAME_RE.fullmatch(name):
        raise RegistryError(f"{node_id}.name must use CC-CCC-NN")
    location = _text(raw.get("location"), f"{node_id}.location", 64)
    route = str(raw.get("route") or "").strip().lower()
    if route not in {"direct", "tunnel"}:
        raise RegistryError(
            f"{node_id}.route must be direct or tunnel; new public SOCKS nodes are forbidden"
        )

    if route == "direct":
        if raw.get("host") is not None or raw.get("port") is not None:
            raise RegistryError(f"{node_id} direct route must have null host and port")
        host = None
        port = None
    else:
        host = raw.get("host")
        if host != "127.0.0.1":
            raise RegistryError(f"{node_id} tunnel must bind exactly to 127.0.0.1")
        port = raw.get("port")
        if type(port) is not int or not 1024 <= port <= 65535:
            raise RegistryError(f"{node_id}.port must be an integer from 1024 to 65535")

    return {
        "name": name,
        "location": location,
        "route": route,
        "host": host,
        "port": port,
    }


def validate_registry(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RegistryError("registry root must be an object")
    unknown = sorted(set(payload) - {"version", "nodes"})
    if unknown:
        raise RegistryError(f"registry has unknown fields: {', '.join(unknown)}")
    if payload.get("version") != SCHEMA_VERSION:
        raise RegistryError(f"unsupported registry version: {payload.get('version')!r}")
    raw_nodes = payload.get("nodes")
    if not isinstance(raw_nodes, dict):
        raise RegistryError("registry nodes must be an object")

    nodes: dict[str, dict[str, Any]] = {}
    names: dict[str, str] = {}
    loopback_ports: dict[int, str] = {}
    direct_owner: str | None = None
    for raw_id, raw_node in raw_nodes.items():
        node_id = str(raw_id or "").strip().upper()
        if node_id != raw_id:
            raise RegistryError(f"registry key {raw_id!r} must already be uppercase and trimmed")
        node = _normalise_custom_node(node_id, raw_node)
        owner = names.get(node["name"])
        if owner is not None:
            raise RegistryError(f"canonical name {node['name']} is shared by {owner} and {node_id}")
        names[node["name"]] = node_id
        if node["route"] == "direct":
            if direct_owner is not None:
                raise RegistryError(f"direct route is shared by {direct_owner} and {node_id}")
            direct_owner = node_id
        else:
            owner = loopback_ports.get(node["port"])
            if owner is not None:
                raise RegistryError(f"loopback port {node['port']} is shared by {owner} and {node_id}")
            loopback_ports[node["port"]] = node_id
        nodes[node_id] = node
    return {"version": SCHEMA_VERSION, "nodes": nodes}


def load_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _empty_registry()
    if not path.is_file() or path.is_symlink():
        raise RegistryError(f"registry must be a regular, non-symlink file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read registry {path}: {exc}") from exc
    return validate_registry(payload)


def save_registry(path: Path, payload: dict[str, Any]) -> None:
    payload = validate_registry(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        if os.name != "nt":
            path.chmod(0o600)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _absolute_path(path: Path) -> Path:
    """Return an absolute path without resolving a final symlink."""
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _stable_id(value: str) -> str:
    result = value.strip().upper()
    if not STABLE_ID_RE.fullmatch(result):
        raise argparse.ArgumentTypeError("id may contain only A-Z, 0-9, _ or -")
    return result


def _canonical_name(value: str) -> str:
    result = value.strip().upper()
    if not CANONICAL_NAME_RE.fullmatch(result):
        raise argparse.ArgumentTypeError("name must use CC-CCC-NN")
    return result


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1024 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1024 and 65535")
    return port


def _bounded_integer(minimum: int, maximum: int):
    def parse(value: str) -> int:
        try:
            number = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("must be an integer") from exc
        if not minimum <= number <= maximum:
            raise argparse.ArgumentTypeError(f"must be between {minimum} and {maximum}")
        return number

    return parse


def _asset_id(value: str) -> str:
    result = value.strip()
    if result and not SAFE_ASSET_RE.fullmatch(result):
        raise argparse.ArgumentTypeError("asset/token id contains unsupported characters")
    return result


def _account_id(value: str) -> str:
    result = value.strip()
    if not ACCOUNT_ID_RE.fullmatch(result):
        raise argparse.ArgumentTypeError("account id contains unsupported characters")
    return result


def _effective_nodes(path: Path) -> dict[str, dict[str, Any]]:
    _builtins, _retired, load_proxy_nodes, _normalise = _backend_registry_api()
    nodes = load_proxy_nodes(path)
    if not isinstance(nodes, dict):
        raise RegistryError("backend proxy-node loader returned an invalid result")
    return nodes


def _validate_effective(candidate: dict[str, Any]) -> None:
    builtins, retired, _loader, normalise_proxy_node = _backend_registry_api()
    retired_overrides = sorted(set(candidate["nodes"]) & set(retired))
    if retired_overrides:
        raise RegistryError(
            "retired node id(s) cannot be registered: "
            + ", ".join(retired_overrides)
        )
    merged: dict[str, Mapping[str, Any]] = dict(builtins)
    merged.update(candidate["nodes"])
    names: dict[str, str] = {}
    local_endpoints: dict[tuple[str, int], str] = {}
    direct_owner: str | None = None
    for node_id, raw in merged.items():
        try:
            node = normalise_proxy_node(node_id, raw)
        except ValueError as exc:
            raise RegistryError(f"backend rejected node {node_id}: {exc}") from exc
        owner = names.get(node["name"])
        if owner is not None:
            raise RegistryError(f"effective name {node['name']} is shared by {owner} and {node_id}")
        names[node["name"]] = node_id
        if node["route"] == "direct":
            if direct_owner is not None:
                raise RegistryError(f"effective direct route is shared by {direct_owner} and {node_id}")
            direct_owner = node_id
        if node["route"] == "tunnel":
            endpoint = (node["host"], node["port"])
            owner = local_endpoints.get(endpoint)
            if owner is not None:
                raise RegistryError(
                    f"effective tunnel endpoint {node['host']}:{node['port']} "
                    f"is shared by {owner} and {node_id}"
                )
            local_endpoints[endpoint] = node_id


def command_add(args: argparse.Namespace) -> int:
    path = _absolute_path(args.file)
    node_id: str = args.id
    name = args.name or node_id
    if not CANONICAL_NAME_RE.fullmatch(name):
        raise RegistryError("legacy stable ids require an explicit canonical --name")
    if args.route == "direct":
        if args.port is not None:
            raise RegistryError("direct route does not accept --port")
        host = None
        port = None
    else:
        if args.port is None:
            raise RegistryError("tunnel route requires --port")
        host = "127.0.0.1"
        port = args.port
    node = _normalise_custom_node(
        node_id,
        {
            "name": name,
            "location": args.location,
            "route": args.route,
            "host": host,
            "port": port,
        },
    )

    builtins, retired, _loader, _normalise = _backend_registry_api()
    if node_id in retired:
        raise RegistryError(f"node {node_id} is retired and cannot be registered")
    with RegistryLock(path):
        payload = load_registry(path)
        if (node_id in payload["nodes"] or node_id in builtins) and not args.replace:
            raise RegistryError(f"node {node_id} already exists; use --replace explicitly")
        candidate = json.loads(json.dumps(payload))
        candidate["nodes"][node_id] = node
        candidate = validate_registry(candidate)
        _validate_effective(candidate)
        save_registry(path, candidate)

    loaded = _effective_nodes(path).get(node_id)
    if not loaded or any(loaded.get(field) != node[field] for field in NODE_FIELDS):
        raise RegistryError("backend did not load the saved node exactly")
    print(f"saved {node_id} ({name}) to {path}")
    return 0


def command_remove(args: argparse.Namespace) -> int:
    """Atomically remove one local-only node without touching builtins."""
    path = _absolute_path(args.file)
    node_id: str = args.id
    builtins, retired, _loader, _normalise = _backend_registry_api()

    with RegistryLock(path):
        payload = load_registry(path)
        if node_id not in payload["nodes"]:
            if node_id in builtins:
                raise RegistryError(
                    f"node {node_id} is builtin and has no custom override to remove"
                )
            status = "retired" if node_id in retired else "unknown"
            raise RegistryError(
                f"node {node_id} is {status} and is not a custom registry entry"
            )
        candidate = json.loads(json.dumps(payload))
        removed = candidate["nodes"].pop(node_id)
        candidate = validate_registry(candidate)

        # Explicit remove is also the safe repair path for a legacy registry
        # containing retired IDs.  Ignore any *other* retired entries only for
        # this removal validation; list/add/benchmark remain strict and will
        # continue to report them until the operator removes each one.
        remaining_retired = {
            retired_id: candidate["nodes"].pop(retired_id)
            for retired_id in sorted(set(candidate["nodes"]) & set(retired))
        }
        try:
            _validate_effective(candidate)
        finally:
            candidate["nodes"].update(remaining_retired)
        save_registry(path, candidate)
        saved = load_registry(path)
        if node_id in saved["nodes"]:
            raise RegistryError("registry still contains the removed node")

    effective = _effective_nodes(path)
    if node_id in builtins:
        if node_id not in effective:
            raise RegistryError("removing the override also removed its builtin node")
        result = "custom override; builtin node remains active"
    else:
        if node_id in effective:
            raise RegistryError("backend still exposes the removed custom node")
        result = "custom node"
    print(f"removed {result} {node_id} ({removed['name']}) from {path}")
    return 0


def _snapshot(path: Path) -> dict[str, Any]:
    with RegistryLock(path):
        payload = load_registry(path)
    # The backend deliberately ignores invalid local entries and keeps its
    # safe builtins alive.  The administration CLI is stricter: surface stale
    # retired overrides so an operator cannot mistake an ignored entry for an
    # active route.
    _validate_effective(payload)
    return payload


def command_list(args: argparse.Namespace) -> int:
    path = _absolute_path(args.file)
    registry = _snapshot(path)
    effective = _effective_nodes(path)
    custom_ids = set(registry["nodes"])
    if args.json:
        print(json.dumps(
            {
                "version": SCHEMA_VERSION,
                "nodes": effective,
                "sources": {
                    node_id: "override" if node_id in custom_ids else "builtin"
                    for node_id in effective
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ))
        return 0
    print("ID\tNAME\tSOURCE\tROUTE\tENDPOINT\tLOCATION")
    for node_id in sorted(effective):
        node = effective[node_id]
        endpoint = "direct" if node["host"] is None else f"{node['host']}:{node['port']}"
        source = "override" if node_id in custom_ids else "builtin"
        print(
            f"{node_id}\t{node['name']}\t{source}\t{node['route']}\t"
            f"{endpoint}\t{node['location']}"
        )
    return 0


async def _run_benchmark(args: argparse.Namespace, nodes: dict[str, Any]) -> Any:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from backend.services.latency_probe import benchmark_proxy_nodes
    except ImportError as exc:
        raise RegistryError(f"latency benchmark module is unavailable: {exc}") from exc
    header_factory = _benchmark_header_factory(args)
    result = benchmark_proxy_nodes(
        nodes,
        region=args.region,
        samples=args.samples,
        yes_asset_id=args.yes_asset_id,
        poly_token_id=args.poly_token_id,
        concurrency=args.concurrency,
        header_factory=header_factory,
    )
    if inspect.isawaitable(result):
        result = await result
    return result


def _benchmark_header_factory(args: argparse.Namespace):
    if not args.yes_asset_id:
        if args.account_id:
            raise RegistryError("--account-id requires --yes-asset-id")
        return None
    if not args.account_id:
        raise RegistryError(
            "--account-id is required to authenticate an OKX book benchmark"
        )
    try:
        from backend.core.auth import OKXAuth
        from backend.core.database import get_account_decrypted
        from backend.services.latency_probe import OKX_BASE_URLS, _okx_target

        account = get_account_decrypted(args.account_id)
    except Exception as exc:
        raise RegistryError("could not load the benchmark account credentials") from exc
    if not account:
        raise RegistryError(f"account not found: {args.account_id}")
    account_base_url = str(account.get("base_url") or "").strip().rstrip("/")
    account_regions = {url.rstrip("/"): region for region, url in OKX_BASE_URLS.items()}
    account_region = account_regions.get(account_base_url)
    if account_region is None:
        raise RegistryError("account has an unsupported OKX base URL")
    if account_region != args.region:
        raise RegistryError(
            f"account region is {account_region}, not requested {args.region}"
        )
    credentials = tuple(
        str(account.get(field) or "")
        for field in ("api_key", "secret_key", "passphrase")
    )
    if not all(credentials):
        raise RegistryError("benchmark account credentials are incomplete")
    auth = OKXAuth(*credentials)
    expected_signed_path = str(
        _okx_target(args.region, args.yes_asset_id).get("signed_path") or ""
    )

    def header_factory(target: Mapping[str, Any]) -> Mapping[str, str]:
        signed_path = str(target.get("signed_path") or "")
        if not signed_path or signed_path != expected_signed_path:
            raise RegistryError("market benchmark signed path does not match the selected asset")
        return auth.headers("GET", signed_path)

    return header_factory


def command_benchmark(args: argparse.Namespace) -> int:
    path = _absolute_path(args.file)
    _snapshot(path)  # Strictly reject corrupt registries before backend fallback.
    effective = _effective_nodes(path)
    missing = sorted(set(args.node) - set(effective))
    if missing:
        raise RegistryError(f"unknown node(s): {', '.join(missing)}")
    nodes = {
        node_id: node
        for node_id, node in effective.items()
        if not args.node or node_id in args.node
    }
    if not nodes:
        raise RegistryError("no proxy nodes selected")
    result = asyncio.run(_run_benchmark(args, nodes))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file", type=Path, default=DEFAULT_REGISTRY,
        help=f"registry path (default: {DEFAULT_REGISTRY})",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    add = commands.add_parser("add", help="add or explicitly replace a safe node override")
    add.add_argument("--id", required=True, type=_stable_id, help="stable database id")
    add.add_argument("--name", type=_canonical_name, help="canonical display name; defaults to id")
    add.add_argument("--location", required=True, help="human-readable location")
    add.add_argument("--route", choices=("direct", "tunnel"), default="tunnel")
    add.add_argument("--port", type=_port, help="127.0.0.1 SOCKS port for a tunnel")
    add.add_argument("--replace", action="store_true")
    add.set_defaults(func=command_add)

    remove = commands.add_parser(
        "remove",
        help="atomically remove a custom registry node (builtins are protected)",
    )
    remove.add_argument("--id", required=True, type=_stable_id)
    remove.set_defaults(func=command_remove)

    list_command = commands.add_parser("list", help="validate and list effective nodes")
    list_command.add_argument("--json", action="store_true")
    list_command.set_defaults(func=command_list)

    benchmark = commands.add_parser("benchmark", help="benchmark effective backend nodes")
    benchmark.add_argument("--region", choices=("AU", "CN"), required=True)
    benchmark.add_argument("--samples", type=_bounded_integer(1, 10), default=5)
    benchmark.add_argument("--concurrency", type=_bounded_integer(1, 8), default=4)
    benchmark.add_argument("--yes-asset-id", type=_asset_id, default="")
    benchmark.add_argument("--poly-token-id", type=_asset_id, default="")
    benchmark.add_argument(
        "--account-id",
        type=_account_id,
        default=None,
        help="local account used to sign --yes-asset-id book probes",
    )
    benchmark.add_argument("--node", action="append", type=_stable_id, default=[])
    benchmark.set_defaults(func=command_benchmark)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except RegistryError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
