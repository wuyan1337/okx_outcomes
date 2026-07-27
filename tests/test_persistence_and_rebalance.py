import asyncio
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

from backend.services.auto_rebalance import AutoRebalance
from backend.services.manual_strategy_d import SELLS_FILE_D
from backend.services.manual_strategy_s import SELLS_FILE_S


ROOT = Path(__file__).resolve().parents[1]


def _load_database_module():
    fernet_module = types.ModuleType("cryptography.fernet")

    class DummyFernet:
        @staticmethod
        def generate_key():
            return b"test-key"

        def __init__(self, _key):
            pass

    fernet_module.Fernet = DummyFernet
    cryptography_module = types.ModuleType("cryptography")
    module_name = "backend.core._database_persistence_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        ROOT / "backend" / "core" / "database.py",
    )
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "cryptography": cryptography_module,
            "cryptography.fernet": fernet_module,
            module_name: module,
        },
    ):
        spec.loader.exec_module(module)
    return module


def _load_client_module():
    socks_module = types.ModuleType("aiohttp_socks")
    socks_module.ProxyConnector = type(
        "ProxyConnector",
        (),
        {"from_url": classmethod(lambda cls, url: (cls, url))},
    )
    auth_module = types.ModuleType("backend.core.auth")
    auth_module.OKXAuth = type("OKXAuth", (), {})
    auth_module.OKXSigner = type("OKXSigner", (), {})
    database_module = types.ModuleType("backend.core.database")
    database_module.PROXY_NODES = {}
    module_name = "backend.core._client_pagination_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        ROOT / "backend" / "core" / "client.py",
    )
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "aiohttp_socks": socks_module,
            "backend.core.auth": auth_module,
            "backend.core.database": database_module,
            module_name: module,
        },
    ):
        spec.loader.exec_module(module)
    return module


class SniperDatabaseTests(unittest.TestCase):
    def test_sniper_snapshot_round_trip(self):
        database = _load_database_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            database.DB_PATH = Path(temp_dir) / "accounts.db"
            database.init_db()
            connection = database.get_db()
            connection.execute(
                """
                INSERT INTO accounts
                    (id, name, api_key, secret_key, passphrase,
                     agent_private_key, proxy_node)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("account-1", "test", "a", "b", "c", "d", "JP2"),
            )
            connection.commit()
            connection.close()

            config = {
                "config": {"threshold_cents": "40"},
                "targets": [{"id": "target-1", "poly_token_id": "poly"}],
            }
            runtime = {
                "status": "holding",
                "locked": True,
                "active_target_id": "target-1",
            }
            database.save_sniper_state("account-1", config, runtime, True)

            restored = database.get_sniper_state("account-1")
            self.assertEqual(restored["config"], config)
            self.assertEqual(restored["runtime"], runtime)
            self.assertTrue(restored["running"])

    def test_sniper_old_snapshot_round_trip(self):
        database = _load_database_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            database.DB_PATH = Path(temp_dir) / "accounts.db"
            database.init_db()
            connection = database.get_db()
            connection.execute(
                """
                INSERT INTO accounts
                    (id, name, api_key, secret_key, passphrase,
                     agent_private_key, proxy_node)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("account-old", "old", "a", "b", "c", "d", "JP2"),
            )
            connection.commit()
            connection.close()

            config = {"threshold_cents": 40, "timeout_secs": 120}
            runtime = {
                "status": "holding",
                "holding_side": "yes",
                "buy_client_order_id": "entry-1",
                "buy_size": 5,
            }
            database.save_sniper_old_state(
                "account-old", config, runtime, True
            )

            restored = database.get_sniper_old_state("account-old")
            self.assertEqual(restored["config"], config)
            self.assertEqual(restored["runtime"], runtime)
            self.assertTrue(restored["running"])


class ClientPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_list_uses_documented_limit_and_cursor(self):
        client_module = _load_client_module()
        client = client_module.OKXOutcomesClient.__new__(
            client_module.OKXOutcomesClient
        )
        calls = []

        async def fake_get(path, params):
            calls.append((path, dict(params)))
            if len(calls) == 1:
                return {
                    "code": "0",
                    "data": {
                        "list": [{"orderId": "1"}],
                        "hasNext": True,
                        "nextCursor": "next",
                    },
                }
            return {
                "code": "0",
                "data": {
                    "list": [{"orderId": "2"}],
                    "hasNext": False,
                },
            }

        client._get = fake_get
        response = await client.get_orders(
            status="closed",
            market_id="market-1",
            page_size=500,
        )

        self.assertEqual(
            response["data"]["list"],
            [{"orderId": "1"}, {"orderId": "2"}],
        )
        self.assertEqual(calls[0][1]["limit"], "50")
        self.assertNotIn("pageSize", calls[0][1])
        self.assertEqual(calls[1][1]["cursor"], "next")
        self.assertEqual(calls[1][1]["marketId"], "market-1")

    async def test_position_list_uses_limit_cursor_and_market_filter(self):
        client_module = _load_client_module()
        client = client_module.OKXOutcomesClient.__new__(
            client_module.OKXOutcomesClient
        )
        calls = []

        async def fake_get(path, params):
            calls.append((path, dict(params)))
            if len(calls) == 1:
                return {
                    "code": 0,
                    "data": {
                        "list": [{"id": "position-1"}],
                        "hasNext": True,
                        "nextCursor": "position-next",
                    },
                }
            return {
                "code": 0,
                "data": {"list": [{"id": "position-2"}], "hasNext": False},
            }

        client._get = fake_get
        response = await client.get_positions(
            status="open", market_id="market-1", page_size=500
        )

        self.assertEqual(
            response["data"]["list"],
            [{"id": "position-1"}, {"id": "position-2"}],
        )
        self.assertEqual(calls[0][1]["limit"], "100")
        self.assertEqual(calls[0][1]["marketId"], "market-1")
        self.assertEqual(calls[1][1]["cursor"], "position-next")

    async def test_position_pagination_failure_never_returns_partial_success(self):
        client_module = _load_client_module()
        client = client_module.OKXOutcomesClient.__new__(
            client_module.OKXOutcomesClient
        )
        calls = 0

        async def fake_get(_path, _params):
            nonlocal calls
            calls += 1
            if calls == 1:
                return {
                    "code": "0",
                    "data": {
                        "list": [{"id": "partial"}],
                        "hasNext": True,
                        "nextCursor": "next",
                    },
                }
            return {"code": "TIMEOUT", "msg": "timed out"}

        client._get = fake_get
        response = await client.get_positions(market_id="market-1")

        self.assertEqual(response["code"], "TIMEOUT")

    async def test_success_code_without_order_list_is_invalid(self):
        client_module = _load_client_module()
        client = client_module.OKXOutcomesClient.__new__(
            client_module.OKXOutcomesClient
        )
        client._get = mock.AsyncMock(return_value={"code": "0", "data": {}})

        response = await client.get_orders()

        self.assertEqual(response["code"], "INVALID_RESPONSE")

    async def test_success_code_without_position_list_is_invalid(self):
        client_module = _load_client_module()
        client = client_module.OKXOutcomesClient.__new__(
            client_module.OKXOutcomesClient
        )
        client._get = mock.AsyncMock(return_value={"code": "0", "data": {}})

        response = await client.get_positions()

        self.assertEqual(response["code"], "INVALID_RESPONSE")


class RebalancePersistenceTests(unittest.IsolatedAsyncioTestCase):
    def test_strategy_state_paths_do_not_depend_on_working_directory(self):
        self.assertTrue(SELLS_FILE_D.is_absolute())
        self.assertTrue(SELLS_FILE_S.is_absolute())
        self.assertEqual(SELLS_FILE_D.parents[1], ROOT)
        self.assertEqual(SELLS_FILE_S.parents[1], ROOT)

    def test_decimal_cost_layer_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            layers_path = Path(temp_dir) / "layers.json"
            rebalance = AutoRebalance(object(), "unit-test")
            rebalance._layers_file = layers_path
            rebalance.cost_layers = {}
            rebalance.add_to_layer("asset-1", "0.1", 2.5, market_id="market-1")
            self.assertTrue(rebalance.set_enabled(True))

            restored = AutoRebalance(object(), "unit-test-restored")
            restored._layers_file = layers_path
            restored.cost_layers = {}
            restored._load_layers()
            self.assertEqual(
                restored.cost_layers,
                {"market-1": {"asset-1": {Decimal("0.1"): 2.5}}},
            )
            self.assertTrue(restored.enabled)

            self.assertTrue(restored.set_enabled(False))
            disabled = AutoRebalance(object(), "unit-test-disabled")
            disabled._layers_file = layers_path
            disabled._load_layers()
            self.assertFalse(disabled.enabled)

    def test_old_or_invalid_v2_enabled_values_fail_closed(self):
        missing = object()
        with tempfile.TemporaryDirectory() as temp_dir:
            layers_path = Path(temp_dir) / "layers.json"
            for saved_enabled in (missing, "true", 1, None):
                with self.subTest(saved_enabled=saved_enabled):
                    payload = {
                        "version": 2,
                        "layers": {"market-1": {"asset-1": {"1.5": 2}}},
                        "legacy_unassigned": {},
                        "captured_buy_fills": {},
                    }
                    if saved_enabled is not missing:
                        payload["enabled"] = saved_enabled
                    layers_path.write_text(json.dumps(payload), encoding="utf-8")

                    rebalance = AutoRebalance(object(), "unit-test-fail-closed")
                    rebalance._layers_file = layers_path
                    rebalance.enabled = True
                    rebalance._load_layers()

                    self.assertFalse(rebalance.enabled)
                    self.assertEqual(
                        rebalance.cost_layers,
                        {"market-1": {"asset-1": {Decimal("1.5"): 2.0}}},
                    )

            layers_path.write_text("{not valid json", encoding="utf-8")
            corrupted = AutoRebalance(object(), "unit-test-corrupted")
            corrupted._layers_file = layers_path
            corrupted.enabled = True
            corrupted._load_layers()
            self.assertFalse(corrupted.enabled)

    def test_set_enabled_write_failure_rolls_back_and_preserves_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            layers_path = Path(temp_dir) / "layers.json"
            rebalance = AutoRebalance(object(), "unit-test-write-failure")
            rebalance._layers_file = layers_path
            self.assertTrue(rebalance.set_enabled(True))
            original = layers_path.read_bytes()

            with mock.patch.object(rebalance, "_save_layers", return_value=False):
                self.assertFalse(rebalance.set_enabled(False))

            self.assertTrue(rebalance.enabled)
            self.assertEqual(layers_path.read_bytes(), original)

    def test_clearing_layers_preserves_enabled_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            layers_path = Path(temp_dir) / "layers.json"
            rebalance = AutoRebalance(object(), "unit-test-clear-layers")
            rebalance._layers_file = layers_path
            rebalance.cost_layers = {
                "market-1": {"asset-1": {Decimal("55"): 3.0}}
            }
            self.assertTrue(rebalance.set_enabled(True))
            rebalance.cost_layers = {}
            self.assertTrue(rebalance._save_layers())

            restored = AutoRebalance(object(), "unit-test-clear-restored")
            restored._layers_file = layers_path
            restored._load_layers()
            self.assertTrue(restored.enabled)
            self.assertEqual(restored.cost_layers, {})

    async def test_partial_fill_capture_uses_status_time_and_exact_price(self):
        class FakeClient:
            async def get_orders(self, status="closed", market_id=None):
                self.status = status
                self.market_id = market_id
                return {
                    "code": "0",
                    "data": {
                        "list": [
                            {
                                "oid": "partial-1",
                                "assetId": "asset-1",
                                "side": "BUY",
                                "status": "PARTIALLY_FILLED",
                                "createdAt": "2000",
                                "filledSize": "3",
                                "price": "0.001",
                            },
                            {
                                "oid": "old-partial",
                                "assetId": "asset-1",
                                "side": "BUY",
                                "status": "PARTIALLY_FILLED",
                                "createdAt": "500",
                                "filledSize": "50",
                                "price": "0.55",
                            },
                        ]
                    },
                }

            async def get_positions(self, status="open", market_id=None):
                return {
                    "code": "0",
                    "data": {
                        "list": [{
                            "assetId": "asset-1",
                            "marketId": market_id,
                            "size": "3",
                        }]
                    },
                }

        rebalance = AutoRebalance(FakeClient(), "unit-test-capture")
        rebalance.cost_layers = {}
        rebalance._save_layers = lambda: None
        rebalance.get_strategy_params = lambda: {
            "strategy_type": "manual-s",
            "market_id": "market-1",
            "asset_id": "asset-1",
            "side": "yes",
            "asset_cost_cents": {"asset-1": Decimal("0.1")},
            "start_time": 1000,
        }

        await rebalance._capture_partial_fills()

        self.assertEqual(
            rebalance.cost_layers,
            {"market-1": {"asset-1": {Decimal("0.100"): 3.0}}},
        )


if __name__ == "__main__":
    unittest.main()
