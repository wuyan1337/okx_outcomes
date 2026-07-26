"""OKX REST API authentication and EIP-712 signing for Outcomes.

Signing pipeline (matches OKX Rust SDK):
  Action -> msgpack(UnsignedTransaction) -> keccak256 = connectionId
  -> EIP-712 Agent{source, connectionId} struct hash
  -> keccak256(0x1901 || domainSeparator || structHash)
  -> ECDSA sign
"""
import hashlib
import hmac
import base64
import time
import secrets
import json
from datetime import datetime, timezone
from typing import Optional

import msgpack
from eth_account import Account
from eth_hash.auto import keccak


class OKXAuth:
    """Handles OKX REST API authentication (HMAC-SHA256)."""

    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        message = timestamp + method.upper() + path + body
        mac = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def headers(self, method: str, path: str, body: str = "") -> dict:
        _now = datetime.now(timezone.utc)
        ts = _now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{_now.microsecond // 1000:03d}Z"
        sign = self._sign(ts, method, path, body)
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }


# ─── EIP-712 constants (from OKX Rust SDK) ───

# Domain: name="Exchange", version="1", chainId=70000196, verifyingContract=0x0
_DOMAIN_NAME = "Exchange"
_DOMAIN_VERSION = "1"
_CHAIN_ID = 70_000_196  # ChainType::Mainnet
_VERIFYING_CONTRACT = b'\x00' * 20

# Agent source string
_AGENT_SOURCE = "Mainnet"

# EIP-712 type hashes (precomputed)
_EIP712_DOMAIN_TYPEHASH = keccak(
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
)
_AGENT_TYPEHASH = keccak(
    b"Agent(string source,bytes32 connectionId)"
)


def _domain_separator() -> bytes:
    """Compute EIP-712 domain separator."""
    from eth_abi import encode
    encoded = encode(
        ['bytes32', 'bytes32', 'bytes32', 'uint256', 'address'],
        [
            _EIP712_DOMAIN_TYPEHASH,
            keccak(_DOMAIN_NAME.encode()),
            keccak(_DOMAIN_VERSION.encode()),
            _CHAIN_ID,
            '0x' + '0' * 40,
        ]
    )
    return keccak(encoded)


def _agent_struct_hash(connection_id: bytes) -> bytes:
    """Compute EIP-712 struct hash for Agent{source, connectionId}."""
    from eth_abi import encode
    encoded = encode(
        ['bytes32', 'bytes32', 'bytes32'],
        [
            _AGENT_TYPEHASH,
            keccak(_AGENT_SOURCE.encode()),
            connection_id,
        ]
    )
    return keccak(encoded)


def _eip712_signing_hash(struct_hash: bytes) -> bytes:
    """Compute full EIP-712 signing hash: keccak(0x1901 || domainSep || structHash)."""
    return keccak(b'\x19\x01' + _domain_separator() + struct_hash)


class OKXSigner:
    """Handles EIP-712 signing for OKX Outcomes write operations.

    Pipeline: action -> msgpack -> keccak256(connectionId) -> EIP-712 Agent -> ECDSA
    """

    def __init__(self, private_key: str):
        self.private_key = private_key
        self.account = None
        if private_key and len(private_key) >= 64:
            try:
                key = private_key if private_key.startswith('0x') else '0x' + private_key
                self.account = Account.from_key(key)
            except Exception:
                pass
        self.address = self.account.address if self.account else None

    def generate_client_order_id(self, region: int = 0, env: int = 1) -> str:
        """Generate 34-char client order ID: 0x{region}{env}{30 random hex}."""
        rand = secrets.token_hex(15)  # 30 hex chars
        return f"0x{region:x}{env:x}{rand}"

    def now_millis(self) -> int:
        return int(time.time() * 1000)

    def sign_place_order(self, orders: list[dict], nonce: int) -> dict:
        """Sign a placeOrder action. Returns {r, s, v} signature."""
        action = self._build_place_order_action(orders)
        return self._sign_action(action, nonce)

    def sign_cancel(self, cancels: list[dict], nonce: int) -> dict:
        """Sign a cancel action. Returns {r, s, v} signature."""
        action = self._build_cancel_action(cancels)
        return self._sign_action(action, nonce)

    def sign_cancel_all(self, asset_ids: list, nonce: int, expires_after: int) -> dict:
        """Sign a cancelAll action. Returns {r, s, v} signature."""
        action = self._build_cancel_all_action(asset_ids)
        return self._sign_action(action, nonce, expires_after=expires_after)

    def sign_merge(self, market_id: str, size: str, nonce: int) -> dict:
        """Sign a predictionMerge action."""
        action = {"type": "predictionMerge", "marketId": market_id, "size": size}
        return self._sign_action(action, nonce)

    def sign_split(self, market_id: str, size: str, nonce: int) -> dict:
        """Sign a predictionSplit action."""
        action = {"type": "predictionSplit", "marketId": market_id, "size": size}
        return self._sign_action(action, nonce)

    def sign_redeem(self, market_id: str, nonce: int) -> dict:
        """Sign a predictionRedeem action."""
        action = {"type": "predictionRedeem", "marketId": market_id}
        return self._sign_action(action, nonce)

    def _build_place_order_action(self, orders: list[dict]) -> dict:
        """Build action dict matching Rust SDK field order for PlaceOrder.

        Wire field order: type, grouping, orders
        Order field order: assetId, side, marketType, clientOrderId, price,
                          reduceOnly, size, [sizeType], orderType
        """
        action_orders = []
        for o in orders:
            order = {
                "assetId": o["assetId"],
                "side": o["side"],
                "marketType": o["marketType"],
                "clientOrderId": o["clientOrderId"],
                "price": o["price"],
                "reduceOnly": o.get("reduceOnly", False),
                "size": o["size"],
            }
            # sizeType: only include if not "base" (default)
            size_type = o.get("sizeType", "base")
            if size_type and size_type != "base":
                order["sizeType"] = size_type
            order["orderType"] = o["orderType"]
            action_orders.append(order)

        return {
            "type": "placeOrder",
            "grouping": "na",
            "orders": action_orders,
        }

    def _build_cancel_action(self, cancels: list[dict]) -> dict:
        """Build cancel action matching Rust SDK field order.

        Wire field order: type, cancels
        Cancel item: assetId, marketType, (oid | clientOrderId)
        """
        items = []
        for c in cancels:
            item = {
                "assetId": c["assetId"],
                "marketType": c.get("marketType", "prediction"),
            }
            if "oid" in c:
                item["oid"] = c["oid"]
            elif "clientOrderId" in c:
                item["clientOrderId"] = c["clientOrderId"]
            items.append(item)
        return {"type": "cancel", "cancels": items}

    def _build_cancel_all_action(self, asset_ids: list) -> dict:
        """Build cancelAll action. Wire: type, assetIds, marketType."""
        return {
            "type": "cancelAll",
            "assetIds": [str(a) for a in asset_ids],
            "marketType": "prediction",
        }

    def _sign_action(self, action: dict, nonce: int, expires_after: Optional[int] = None) -> dict:
        """Sign an action using the OKX msgpack + EIP-712 pipeline.

        1. Build UnsignedTransaction {action, nonce, [expiresAfter]}
        2. msgpack serialize (named fields, camelCase)
        3. keccak256 -> connectionId
        4. EIP-712 Agent{source="Mainnet", connectionId} struct hash
        5. EIP-712 signing hash
        6. ECDSA sign
        """
        # Build the unsigned transaction dict (field order matters for msgpack)
        unsigned_tx = {"action": action, "nonce": nonce}
        if expires_after is not None:
            unsigned_tx["expiresAfter"] = expires_after

        # msgpack serialize with named fields
        packed = msgpack.packb(unsigned_tx, use_bin_type=True)

        # keccak256 -> connectionId
        connection_id = keccak(packed)

        # EIP-712 struct hash
        struct_hash = _agent_struct_hash(connection_id)

        # EIP-712 signing hash
        signing_hash = _eip712_signing_hash(struct_hash)

        # ECDSA sign
        signed = self.account.unsafe_sign_hash(signing_hash)
        r = hex(signed.r)
        s = hex(signed.s)
        v = signed.v  # already 0 or 1 from eth-account >= 0.12
        if v >= 27:
            v -= 27
        return {"r": r, "s": s, "v": v}
