"""Canonical proxy-node registry with persistent local overrides."""
from __future__ import annotations

import json
import logging
from pathlib import Path
import re
from typing import Mapping, Optional


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROXY_NODES_FILE = PROJECT_ROOT / "data" / "proxy_nodes.json"
REGISTRY_SCHEMA_VERSION = 1
NODE_ID_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,31}$")
CANONICAL_NAME_PATTERN = re.compile(r"^[A-Z]{2}-[A-Z]{3}-[0-9]{2}$")
HOST_PATTERN = re.compile(r"^[A-Za-z0-9.-]+$")
ROUTE_LABELS = {
    "direct": "Direct",
    "socks5": "SOCKS5",
    "tunnel": "SSH tunnel",
}

# IDs are persistent database values.  Do not rename them when changing labels.
BUILTIN_PROXY_NODES = {
    "JP1": {"name": "JP-TYO-01", "location": "Tokyo", "route": "socks5", "host": "168.138.196.214", "port": 9111},
    "JP2": {"name": "JP-TYO-02", "location": "Tokyo", "route": "direct", "host": None, "port": None},
    "SG1": {"name": "SG-SIN-01", "location": "Singapore", "route": "socks5", "host": "13.213.17.198", "port": 9111},
    "SYD1": {"name": "AU-SYD-01", "location": "Sydney", "route": "socks5", "host": "192.9.160.224", "port": 9111},
    "SYD2": {"name": "AU-SYD-02", "location": "Sydney", "route": "socks5", "host": "161.33.232.81", "port": 9111},
}


def canonical_proxy_label(node_id: str, node: Mapping) -> str:
    route = str(node.get("route") or "socks5").lower()
    location = str(node.get("location") or "Unknown").strip() or "Unknown"
    name = str(node.get("name") or node_id).strip().upper()
    return f"{name} | {location} | {ROUTE_LABELS.get(route, route.upper())}"


def normalise_proxy_node(node_id: str, raw: Mapping) -> dict:
    node_id = str(node_id or "").strip().upper()
    if not NODE_ID_PATTERN.fullmatch(node_id):
        raise ValueError("node id must use 1-32 uppercase letters, digits, '_' or '-'")
    if not isinstance(raw, Mapping):
        raise ValueError("node definition must be an object")
    unknown_fields = sorted(
        set(raw) - {"name", "location", "route", "host", "port"}
    )
    if unknown_fields:
        raise ValueError(
            f"unknown node fields: {', '.join(str(field) for field in unknown_fields)}"
        )

    name = str(raw.get("name") or node_id).strip().upper()
    if not CANONICAL_NAME_PATTERN.fullmatch(name):
        raise ValueError("name must use the canonical CC-CITY-NN format")

    location = str(raw.get("location") or "").strip()
    if not location or len(location) > 64:
        raise ValueError("location must be 1-64 characters")
    route = str(raw.get("route") or "").strip().lower()
    if route not in ROUTE_LABELS:
        raise ValueError("route must be direct, socks5 or tunnel")

    host: Optional[str]
    port: Optional[int]
    if route == "direct":
        if raw.get("host") is not None or raw.get("port") is not None:
            raise ValueError("direct nodes must use null host and port")
        host = None
        port = None
    else:
        host = str(raw.get("host") or "").strip()
        if (
            not host
            or len(host) > 253
            or not HOST_PATTERN.fullmatch(host)
        ):
            raise ValueError("host must be a hostname or IP address without a URL scheme")
        try:
            port = int(raw.get("port"))
        except (TypeError, ValueError) as exc:
            raise ValueError("port must be an integer") from exc
        if not 1024 <= port <= 65535:
            raise ValueError("port must be between 1024 and 65535")
        if route == "tunnel" and host != "127.0.0.1":
            raise ValueError("SSH tunnel nodes must listen on 127.0.0.1")

    node = {
        "name": name,
        "location": location,
        "route": route,
        "host": host,
        "port": port,
    }
    node["label"] = canonical_proxy_label(node_id, node)
    return node


def load_proxy_nodes(path: Path | str = PROXY_NODES_FILE) -> dict:
    builtin_nodes = {
        node_id: normalise_proxy_node(node_id, raw)
        for node_id, raw in BUILTIN_PROXY_NODES.items()
    }
    registry_path = Path(path)
    if not registry_path.exists():
        return builtin_nodes
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("registry root must be an object")
        unknown_root_fields = sorted(set(payload) - {"version", "nodes"})
        if unknown_root_fields:
            raise ValueError(
                f"unknown registry fields: {', '.join(unknown_root_fields)}"
            )
        if payload.get("version") != REGISTRY_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported registry version: {payload.get('version')!r}"
            )
        custom_nodes = payload.get("nodes")
        if not isinstance(custom_nodes, dict):
            raise ValueError("registry must contain a nodes object")
        overrides = {}
        for node_id, raw in custom_nodes.items():
            try:
                canonical_id = str(node_id).strip().upper()
                if node_id != canonical_id:
                    raise ValueError("node id must already be uppercase and trimmed")
                candidate = normalise_proxy_node(canonical_id, raw)
                if candidate["route"] == "socks5":
                    raise ValueError(
                        "custom public SOCKS5 nodes are not allowed; use a loopback SSH tunnel"
                    )
                overrides[canonical_id] = candidate
            except ValueError as exc:
                logger.error("Ignoring invalid proxy node %s: %s", node_id, exc)
        nodes = {**builtin_nodes, **overrides}
        _validate_effective_nodes(nodes)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logger.error("Ignoring invalid proxy registry %s: %s", registry_path, exc)
        return builtin_nodes
    return nodes


def _validate_effective_nodes(nodes: Mapping[str, Mapping]) -> None:
    canonical_names = {}
    tunnel_endpoints = {}
    direct_owner = None
    for node_id, node in nodes.items():
        name = node["name"]
        owner = canonical_names.get(name)
        if owner is not None:
            raise ValueError(f"canonical name {name} is shared by {owner} and {node_id}")
        canonical_names[name] = node_id

        route = node["route"]
        if route == "direct":
            if direct_owner is not None:
                raise ValueError(
                    f"direct route is shared by {direct_owner} and {node_id}"
                )
            direct_owner = node_id
        elif route == "tunnel":
            endpoint = (node["host"], node["port"])
            owner = tunnel_endpoints.get(endpoint)
            if owner is not None:
                raise ValueError(
                    f"tunnel endpoint {node['host']}:{node['port']} is shared by "
                    f"{owner} and {node_id}"
                )
            tunnel_endpoints[endpoint] = node_id


PROXY_NODES = load_proxy_nodes()
