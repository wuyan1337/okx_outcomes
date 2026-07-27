from pathlib import Path
import tempfile
import unittest
from unittest import mock

from backend.services import auto_rebalance as rebalance_module
from backend.services.auto_rebalance import AutoRebalance


class _Client:
    def __init__(self, *, orders=None, positions=None):
        self.orders = list(orders or [])
        self.positions = list(positions or [])

    async def get_orders(self, status="open"):
        return {"code": "0", "data": {"list": list(self.orders)}}

    async def get_positions(self, status="open"):
        return {"code": "0", "data": {"list": list(self.positions)}}


class RebalanceFlatRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def _rebalance(self, root: Path, client):
        template = root / "cost_layers_{account_id}.json"
        with mock.patch.object(rebalance_module, "COST_LAYERS_FILE", template):
            rebalance = AutoRebalance(client, account_id="account-a")
        rebalance._recovery = {
            "required": True,
            "rebalance_id": "",
            "stage": "state_load_failed",
            "phase": "load_failed",
            "context": {"error_type": "JSONDecodeError"},
            "strategy_params": {},
            "updated_at_unix_ns": 1,
        }
        return rebalance

    async def test_unusable_journal_can_only_clear_after_verified_flat(self):
        with tempfile.TemporaryDirectory() as directory:
            rebalance = self._rebalance(Path(directory), _Client())

            result = await rebalance.recover()

            self.assertTrue(result["ok"])
            self.assertFalse(result["recovery_required"])
            self.assertFalse(rebalance.recovery_required)
            self.assertTrue(rebalance._layers_file.exists())

    async def test_unusable_journal_stays_locked_when_position_remains(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2.5",
        }
        with tempfile.TemporaryDirectory() as directory:
            rebalance = self._rebalance(
                Path(directory), _Client(positions=[position])
            )

            result = await rebalance.recover()

            self.assertFalse(result["ok"])
            self.assertTrue(result["recovery_required"])
            self.assertTrue(rebalance.recovery_required)
            self.assertEqual(result["flat_status"]["nonzero_position_count"], 1)


if __name__ == "__main__":
    unittest.main()
