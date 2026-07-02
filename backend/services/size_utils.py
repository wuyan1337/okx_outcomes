"""Size string helpers for OKX prediction share quantities.

Keep share sizes exactly as decimal strings. Do not round, do not coerce to int,
and do not force fixed decimal places. The goal is to preserve user input and
OKX-returned sizes such as 100.59 so sell/close orders do not exceed balance.
"""
from decimal import Decimal, InvalidOperation


def normalize_size_str(value) -> str:
    """Return a safe decimal string without changing precision.

    Examples:
    - "100.59" -> "100.59"
    - "100.5900" -> "100.5900"
    - 100.59 -> "100.59"
    - None/invalid -> "0"
    """
    if value is None:
        return "0"
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return "0"
        try:
            Decimal(s)
        except InvalidOperation:
            return "0"
        return s
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return "0"
    return format(d, "f")


def size_to_float(value) -> float:
    try:
        return float(normalize_size_str(value))
    except Exception:
        return 0.0
