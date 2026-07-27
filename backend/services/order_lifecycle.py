"""Shared, fail-closed helpers for OKX prediction-order lifecycles."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
import json
import os
from pathlib import Path
import secrets
import tempfile
from typing import Any, Mapping, Optional

from ..core.price_utils import as_decimal


OPEN_ORDER_STATUSES = frozenset({"PENDING_PLACE", "ACTIVE", "PENDING_CANCEL"})
TERMINAL_ORDER_STATUSES = frozenset(
    {"FILLED", "PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}
)
PLACE_CONFIRMATION_STATUSES = frozenset({"ACTIVE", *TERMINAL_ORDER_STATUSES})
UNACKNOWLEDGED_RESPONSE_CODES = frozenset(
    {"TIMEOUT", "ERROR", "INVALID_RESPONSE", "INVALID"}
)
RESERVING_ORDER_STATUSES = frozenset(
    {"PREPARED", "UNKNOWN", "PENDING_PLACE", "ACTIVE", "PENDING_CANCEL"}
)


def decimal_text(value: Any) -> str:
    """Return a finite Decimal as a non-exponent JSON string."""
    return format(as_decimal(value), "f")


def normalise_order_status(value: Any) -> str:
    status = str(value or "").strip().upper()
    aliases = {
        "PLACE_FAILED": "FAILED",
        "CANCELED": "CANCELLED",
    }
    return aliases.get(status, status)


def generate_client_order_id(client: Any) -> str:
    """Use the real client's regional generator, with a format-safe fallback."""
    factory = getattr(client, "generate_client_order_id", None)
    if callable(factory):
        value = str(factory() or "").strip()
        if value:
            return value
        raise ValueError("clientOrderId generator returned an empty value")
    # Tests and legacy client adapters may not expose the generator.  Real
    # OKXOutcomesClient always does.  0/1 are the HK/production prefix nibbles.
    return f"0x01{secrets.token_hex(15)}"


def atomic_write_json(path: Path | str, payload: Mapping[str, Any]) -> None:
    """Atomically replace a JSON state file after flushing its contents."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_name: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        temp_name = None
        # Best effort directory sync on POSIX.  Windows does not support
        # opening directories this way, while os.replace is already atomic.
        if os.name != "nt":
            directory_fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temp_name:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass


@dataclass(frozen=True)
class OrderObservation:
    client_order_id: str = ""
    order_id: str = ""
    market_id: str = ""
    asset_id: str = ""
    side: str = ""
    status: str = ""
    price: str = ""
    size: Decimal = Decimal("0")
    filled_size: Decimal = Decimal("0")
    filled_amount: Decimal = Decimal("0")
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlaceConfirmation:
    response: Mapping[str, Any]
    response_code: str
    observation: Optional[OrderObservation]
    confirmed: bool
    rejected: bool
    unresolved: bool


@dataclass(frozen=True)
class CancelConfirmation:
    response: Mapping[str, Any]
    observation: Optional[OrderObservation]
    confirmed_terminal: bool


def _safe_decimal(value: Any) -> Decimal:
    try:
        return as_decimal(value if value not in (None, "") else "0")
    except (TypeError, ValueError):
        return Decimal("0")


def observation_from_order(order: Mapping[str, Any]) -> OrderObservation:
    return OrderObservation(
        client_order_id=str(order.get("clientOrderId") or ""),
        order_id=str(order.get("oid") or order.get("id") or order.get("orderId") or ""),
        market_id=str(order.get("marketId") or ""),
        asset_id=str(order.get("assetId") or ""),
        side=str(order.get("side") or "").upper(),
        status=normalise_order_status(order.get("status")),
        price=str(order.get("price") or ""),
        size=_safe_decimal(order.get("size")),
        filled_size=_safe_decimal(order.get("filledSize")),
        filled_amount=_safe_decimal(order.get("filledAmount")),
        raw=dict(order),
    )


def response_order_id(response: Mapping[str, Any]) -> str:
    data = response.get("data") if isinstance(response.get("data"), Mapping) else {}
    return str(
        response.get("_oid")
        or response.get("oid")
        or data.get("oid")
        or data.get("orderId")
        or data.get("id")
        or ""
    )


def _matches_order(
    observation: OrderObservation,
    *,
    client_order_id: str,
    order_id: str,
) -> bool:
    if client_order_id and observation.client_order_id == client_order_id:
        return True
    if order_id and observation.order_id == order_id:
        # Once an exchange order ID is known it is an equally exact strategy
        # identifier.  A conflicting non-empty cloid is never accepted.
        return not (
            client_order_id
            and observation.client_order_id
            and observation.client_order_id != client_order_id
        )
    return False


async def _get_orders(client: Any, status: str, market_id: str) -> Mapping[str, Any]:
    getter = getattr(client, "get_orders", None)
    if not callable(getter):
        return {"code": "ERROR", "msg": "get_orders is unavailable"}
    try:
        return await getter(status=status, market_id=market_id or None)
    except TypeError:
        # Compatibility with minimal test/legacy adapters.
        return await getter(status=status)


async def observe_order(
    client: Any,
    *,
    client_order_id: str,
    order_id: str = "",
    market_id: str = "",
) -> Optional[OrderObservation]:
    """Query one exact strategy order without adopting same-asset orders."""
    if not client_order_id and not order_id:
        return None

    getter = getattr(client, "get_order", None)
    if order_id and callable(getter):
        try:
            response = await getter(order_id)
            data = response.get("data") if isinstance(response, Mapping) else None
            if str(response.get("code", "")) == "0" and isinstance(data, Mapping):
                observation = observation_from_order(data)
                if _matches_order(
                    observation,
                    client_order_id=client_order_id,
                    order_id=order_id,
                ):
                    return observation
        except Exception:
            pass

    for query_status in ("open", "closed"):
        try:
            response = await _get_orders(client, query_status, market_id)
        except Exception:
            continue
        if not isinstance(response, Mapping) or str(response.get("code", "")) != "0":
            continue
        data = response.get("data")
        orders = data.get("list") if isinstance(data, Mapping) else None
        if not isinstance(orders, list):
            continue
        for order in orders:
            if not isinstance(order, Mapping):
                continue
            observation = observation_from_order(order)
            if _matches_order(
                observation,
                client_order_id=client_order_id,
                order_id=order_id,
            ):
                return observation
    return None


async def wait_for_order(
    client: Any,
    *,
    client_order_id: str,
    order_id: str = "",
    market_id: str = "",
    accepted_statuses: frozenset[str] = PLACE_CONFIRMATION_STATUSES,
    attempts: int = 10,
    delay: float = 0.3,
) -> Optional[OrderObservation]:
    for attempt in range(max(1, int(attempts))):
        observation = await observe_order(
            client,
            client_order_id=client_order_id,
            order_id=order_id,
            market_id=market_id,
        )
        if observation and observation.status in accepted_statuses:
            return observation
        if attempt + 1 < max(1, int(attempts)):
            await asyncio.sleep(max(0.0, float(delay)))
    return None


async def place_and_confirm(
    client: Any,
    *,
    client_order_id: str,
    market_id: str,
    asset_id: str,
    side: str,
    price: str,
    size: str,
    tif: str = "gtc",
    reduce_only: bool = False,
    attempts: int = 10,
    delay: float = 0.3,
) -> PlaceConfirmation:
    """Place once and prove GTC became ACTIVE or reached a terminal state."""
    try:
        response = await client.place_order(
            asset_id=asset_id,
            side=side,
            price=price,
            size=size,
            tif=tif,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )
    except Exception as exc:
        response = {"code": "ERROR", "msg": str(exc)}
    if not isinstance(response, Mapping):
        response = {"code": "INVALID_RESPONSE", "msg": "Invalid order response"}

    response_code = str(response.get("code", "INVALID")).upper()
    should_reconcile = response_code == "0" or response_code in UNACKNOWLEDGED_RESPONSE_CODES
    if not should_reconcile:
        return PlaceConfirmation(
            response=response,
            response_code=response_code,
            observation=None,
            confirmed=False,
            rejected=True,
            unresolved=False,
        )

    observation = await wait_for_order(
        client,
        client_order_id=client_order_id,
        order_id=response_order_id(response),
        market_id=market_id,
        accepted_statuses=PLACE_CONFIRMATION_STATUSES,
        attempts=attempts,
        delay=delay,
    )
    return PlaceConfirmation(
        response=response,
        response_code=response_code,
        observation=observation,
        confirmed=observation is not None,
        rejected=False,
        unresolved=observation is None,
    )


async def cancel_and_confirm(
    client: Any,
    *,
    client_order_id: str,
    order_id: str,
    market_id: str,
    asset_id: str,
    attempts: int = 10,
    delay: float = 0.3,
) -> CancelConfirmation:
    """Cancel one exact order and prove a terminal REST status.

    OKX documents single-order cancellation for ACTIVE orders.  A freshly
    submitted order may still be PENDING_PLACE, so sending one cancel request
    and only polling afterwards can miss the first moment at which it becomes
    cancellable.  Observe on every attempt and submit (or retry) cancellation
    once the exact order is ACTIVE.
    """
    response: Mapping[str, Any] = {
        "code": "NOT_SENT",
        "msg": "Order has not reached a cancellable state",
    }
    cancel_acknowledged = False
    max_attempts = max(1, int(attempts))
    observation: Optional[OrderObservation] = None

    for attempt in range(max_attempts):
        observation = await observe_order(
            client,
            client_order_id=client_order_id,
            order_id=order_id,
            market_id=market_id,
        )
        if observation and observation.status in TERMINAL_ORDER_STATUSES:
            return CancelConfirmation(
                response=response,
                observation=observation,
                confirmed_terminal=True,
            )

        status = observation.status if observation else ""
        if status == "PENDING_CANCEL":
            cancel_acknowledged = True

        should_cancel = (
            (status == "ACTIVE" and not cancel_acknowledged)
            or (not observation and not cancel_acknowledged)
            or (
                bool(observation)
                and status not in {"PENDING_PLACE", "PENDING_CANCEL", "ACTIVE"}
                and not cancel_acknowledged
            )
        )
        if should_cancel:
            try:
                kwargs = {"order_id": order_id} if order_id else {
                    "client_order_id": client_order_id
                }
                response = await client.cancel_order(asset_id=asset_id, **kwargs)
            except Exception as exc:
                response = {"code": "ERROR", "msg": str(exc)}
            if not isinstance(response, Mapping):
                response = {
                    "code": "INVALID_RESPONSE",
                    "msg": "Invalid cancel response",
                }
            cancel_acknowledged = str(response.get("code", "")) == "0"

        if attempt + 1 < max_attempts:
            await asyncio.sleep(max(0.0, float(delay)))

    return CancelConfirmation(
        response=response,
        observation=None,
        confirmed_terminal=False,
    )


def update_owned_record(record: dict[str, Any], observation: OrderObservation) -> None:
    """Copy an exchange observation into a JSON-safe owned-order record."""
    if observation.client_order_id:
        record["client_order_id"] = observation.client_order_id
    if observation.order_id:
        record["order_id"] = observation.order_id
    if observation.market_id:
        record["market_id"] = observation.market_id
    if observation.asset_id:
        record["asset_id"] = observation.asset_id
    if observation.side:
        record["side"] = observation.side
    if observation.price:
        record["price"] = observation.price
    if observation.size > 0:
        record["size"] = decimal_text(observation.size)
    record["filled_size"] = decimal_text(observation.filled_size)
    record["filled_amount"] = decimal_text(observation.filled_amount)
    record["status"] = observation.status or record.get("status", "UNKNOWN")


def owned_order_remaining(record: Mapping[str, Any]) -> Decimal:
    return max(
        Decimal("0"),
        _safe_decimal(record.get("size")) - _safe_decimal(record.get("filled_size")),
    )


def archive_completed_order_groups(
    *,
    owned_orders: dict[str, dict[str, Any]],
    pending_sells: dict[str, Any],
    sell_retry_counts: dict[str, Any],
    sell_retry_after: dict[str, Any],
    archive: list[dict[str, Any]],
    archive_limit: int = 200,
) -> int:
    """Move fully settled BUY/protection groups out of the active ledger.

    A group is removable only after the entry is terminal, every protective
    order is terminal, every protective fill has been accounted, and total
    protective fills cover the entry fill.  Anything still capable of changing
    exchange exposure remains in ``owned_orders``.
    """
    completed: list[tuple[str, list[str], dict[str, Any]]] = []
    for buy_cloid, entry in list(owned_orders.items()):
        if entry.get("role") != "entry_buy":
            continue
        if normalise_order_status(entry.get("status")) not in TERMINAL_ORDER_STATUSES:
            continue

        entry_filled = _safe_decimal(entry.get("filled_size"))
        if _safe_decimal(entry.get("layer_recorded_size")) < entry_filled:
            continue

        child_ids: list[str] = []
        children: list[dict[str, Any]] = []
        exited = Decimal("0")
        settled = True
        for child_cloid, child in owned_orders.items():
            if (
                child.get("role") != "protective_sell"
                or str(child.get("parent_buy_cloid") or "") != str(buy_cloid)
            ):
                continue
            child_ids.append(child_cloid)
            child_status = normalise_order_status(child.get("status"))
            child_filled = _safe_decimal(child.get("filled_size"))
            if child_status not in TERMINAL_ORDER_STATUSES:
                settled = False
                break
            if _safe_decimal(child.get("accounted_fill_size")) < child_filled:
                settled = False
                break
            exited += child_filled
            children.append(dict(child))

        if not settled or exited < entry_filled:
            continue
        completed.append((
            buy_cloid,
            child_ids,
            {
                "entry": dict(entry),
                "protective_orders": children,
            },
        ))

    for buy_cloid, child_ids, snapshot in completed:
        archive.append(snapshot)
        owned_orders.pop(buy_cloid, None)
        for child_cloid in child_ids:
            owned_orders.pop(child_cloid, None)
        pending_sells.pop(buy_cloid, None)
        sell_retry_counts.pop(buy_cloid, None)
        sell_retry_after.pop(buy_cloid, None)

    limit = max(0, int(archive_limit))
    if limit == 0:
        archive.clear()
    elif len(archive) > limit:
        del archive[:-limit]
    return len(completed)
