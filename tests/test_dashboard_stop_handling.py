from pathlib import Path
import unittest


DASHBOARD = Path(__file__).resolve().parents[1] / "frontend" / "dashboard.html"


class DashboardStopHandlingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DASHBOARD.read_text(encoding="utf-8")

    def test_stop_requests_check_http_and_json_result(self):
        self.assertIn("async function stopStrategyRequest(path,label)", self.source)
        self.assertIn("if(!r.ok||!d.ok)", self.source)

    def test_strategy_buttons_only_clear_running_state_after_confirmed_stop(self):
        for expected in (
            "stopStrategyRequest('/api/strategy/manual/stop','Manual-D')",
            "stopStrategyRequest('/api/strategy/manual-s/stop','Manual-S')",
            "stopStrategyRequest('/api/strategy/manual-s/stop','Bid-Sell')",
            "stopStrategyRequest('/api/sniper-old/stop','Sniper Old')",
        ):
            self.assertIn(expected, self.source)

        self.assertNotIn(
            "await api('/api/strategy/manual/stop',{method:'POST'});navId++",
            self.source,
        )
        self.assertNotIn(
            "await api('/api/strategy/manual-s/stop',{method:'POST'});navId++",
            self.source,
        )

    def test_delete_confirmation_describes_local_only_removal(self):
        self.assertIn("Delete this local account", self.source)
        self.assertIn("will not cancel OKX orders or close OKX positions", self.source)

    def test_dashboard_has_no_account_recovery_lock_ui(self):
        self.assertNotIn("RECOVERY LOCK", self.source)
        self.assertNotIn("recovery-banner", self.source)


if __name__ == "__main__":
    unittest.main()
