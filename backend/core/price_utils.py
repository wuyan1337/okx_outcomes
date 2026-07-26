"""Exact price conversion and tick handling for prediction-market prices."""

from decimal import (
    Decimal,
    InvalidOperation,
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_UP,
)
from typing import Literal, Union


DecimalLike = Union[Decimal, str, int, float]
PriceRounding = Literal["buy", "sell", "nearest"]

ZERO = Decimal("0")
ONE = Decimal("1")
CENT_SCALE = Decimal("100")
POLY_CENT_QUANTUM = Decimal("0.01")
OKX_COARSE_TICK = Decimal("0.01")
OKX_FINE_TICK = Decimal("0.001")
OKX_COARSE_MIN = Decimal("0.04")
OKX_COARSE_MAX = Decimal("0.96")
OKX_MIN_PRICE = Decimal("0.001")
OKX_MAX_PRICE = Decimal("0.999")


def as_decimal(value: DecimalLike) -> Decimal:
    """Convert external numeric input without inheriting binary-float noise."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a price")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError, AttributeError) as exc:
        raise ValueError(f"invalid price: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"invalid price: {value!r}")
    return result


def cents_to_price(cents: DecimalLike) -> Decimal:
    return as_decimal(cents) / CENT_SCALE


def price_to_cents(price: DecimalLike) -> Decimal:
    return as_decimal(price) * CENT_SCALE


def okx_tick(price: DecimalLike) -> Decimal:
    price_decimal = as_decimal(price)
    if OKX_COARSE_MIN <= price_decimal <= OKX_COARSE_MAX:
        return OKX_COARSE_TICK
    return OKX_FINE_TICK


def _rounding_mode(rounding: PriceRounding) -> str:
    if rounding == "buy":
        return ROUND_FLOOR
    if rounding == "sell":
        return ROUND_CEILING
    if rounding == "nearest":
        return ROUND_HALF_UP
    raise ValueError(f"unsupported price rounding: {rounding}")


def align_okx_price(price: DecimalLike, rounding: PriceRounding) -> Decimal:
    """Align a raw 0..1 price to an executable OKX tick.

    Buy limits round down and sell limits round up so normalization never makes
    the order less conservative than the price requested by the caller.
    """
    price_decimal = as_decimal(price)
    if not ZERO < price_decimal < ONE:
        raise ValueError("price must be between 0 and 1")

    mode = _rounding_mode(rounding)
    tick = okx_tick(price_decimal)
    aligned = (price_decimal / tick).to_integral_value(rounding=mode) * tick

    # Crossing 0.04 or 0.96 can change the applicable tick. Re-align once using
    # the final region so the result remains aligned to that region's tick.
    final_tick = okx_tick(aligned)
    if final_tick != tick:
        aligned = (aligned / final_tick).to_integral_value(rounding=mode) * final_tick

    if not OKX_MIN_PRICE <= aligned <= OKX_MAX_PRICE:
        raise ValueError("price is outside OKX's executable range")
    return aligned


def is_okx_price_aligned(price: DecimalLike) -> bool:
    price_decimal = as_decimal(price)
    if not OKX_MIN_PRICE <= price_decimal <= OKX_MAX_PRICE:
        return False
    tick = okx_tick(price_decimal)
    return price_decimal % tick == ZERO


def format_okx_price(
    price: DecimalLike,
    rounding: PriceRounding | None = None,
) -> str:
    """Format a raw OKX price, optionally aligning it first."""
    price_decimal = (
        align_okx_price(price, rounding) if rounding else as_decimal(price)
    )
    if not is_okx_price_aligned(price_decimal):
        raise ValueError(f"price {price_decimal} is not on an OKX tick")
    precision = 2 if okx_tick(price_decimal) == OKX_COARSE_TICK else 3
    return f"{price_decimal:.{precision}f}"


def align_cents_for_okx(cents: DecimalLike, rounding: PriceRounding) -> Decimal:
    return price_to_cents(align_okx_price(cents_to_price(cents), rounding))


def format_okx_cents(cents: DecimalLike, rounding: PriceRounding | None = None) -> str:
    return format_okx_price(cents_to_price(cents), rounding)


def format_poly_cents(price: DecimalLike) -> str:
    """Format a raw Polymarket price as cents with 0.01-cent precision."""
    cents = price_to_cents(price).quantize(POLY_CENT_QUANTUM, rounding=ROUND_HALF_UP)
    return f"{cents:.2f}"


def invert_binary_price(price: DecimalLike) -> Decimal:
    price_decimal = as_decimal(price)
    if not ZERO <= price_decimal <= ONE:
        raise ValueError("price must be between 0 and 1")
    return ONE - price_decimal
