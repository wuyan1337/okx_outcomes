import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

try:
    from backend.core import database
except ModuleNotFoundError as exc:
    if exc.name != "cryptography":
        raise
    fernet_module = types.ModuleType("cryptography.fernet")

    class DummyFernet:
        @staticmethod
        def generate_key():
            return b"test-key"

        def __init__(self, _key):
            pass

    fernet_module.Fernet = DummyFernet
    cryptography_module = types.ModuleType("cryptography")
    module_name = "backend.core._database_sniper_state_test"
    database_path = Path(__file__).resolve().parents[1] / "backend" / "core" / "database.py"
    spec = importlib.util.spec_from_file_location(module_name, database_path)
    database = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "cryptography": cryptography_module,
            "cryptography.fernet": fernet_module,
            module_name: database,
        },
    ):
        spec.loader.exec_module(database)


class SniperPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = database.DB_PATH
        database.DB_PATH = Path(self.temp_dir.name) / "accounts.db"
        database.init_db()
        connection = database.get_db()
        connection.execute(
            """
            INSERT INTO accounts (
                id, name, api_key, secret_key, passphrase,
                agent_private_key, proxy_node
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("account-a", "Account A", "k", "s", "p", "pk", "JP2"),
        )
        connection.commit()
        connection.close()

    def tearDown(self):
        database.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_sniper_snapshot_round_trip_and_upsert(self):
        config = {
            "config": {"threshold_cents": "10.5", "buy_limit_cents": "50"},
            "targets": [{"id": "argentina", "side": "NO"}],
        }
        runtime = {
            "running": True,
            "locked": True,
            "active_target_id": "argentina",
            "holding": {
                "buy_size": "2.5",
                "entry_client_order_id": "entry-1",
                "exit_client_order_ids": ["exit-1"],
            },
        }

        database.save_sniper_state("account-a", config, runtime, True)
        first = database.get_sniper_state("account-a")

        self.assertEqual(first["config"], config)
        self.assertEqual(first["runtime"], runtime)
        self.assertTrue(first["running"])

        runtime["running"] = False
        runtime["locked"] = False
        database.save_sniper_state("account-a", config, runtime, False)
        second = database.get_sniper_state("account-a")
        connection = database.get_db()
        row_count = connection.execute(
            "SELECT COUNT(*) FROM sniper_state WHERE account_id = ?",
            ("account-a",),
        ).fetchone()[0]
        connection.close()

        self.assertEqual(row_count, 1)
        self.assertEqual(second["runtime"], runtime)
        self.assertFalse(second["running"])

    def test_deleting_account_cascades_sniper_snapshot(self):
        database.save_sniper_state("account-a", {"targets": []}, {"locked": False}, False)

        database.delete_account("account-a")

        self.assertIsNone(database.get_sniper_state("account-a"))

    def test_deleting_account_removes_only_its_exact_legacy_poly_state(self):
        database.set_global_config("poly_state_account-a", "owned")
        database.set_global_config("poly_state_account-a-copy", "keep")
        database.set_global_config("tg_chats", "keep-global")

        deleted = database.delete_account("account-a")

        self.assertTrue(deleted)
        self.assertEqual(database.get_global_config("poly_state_account-a", "missing"), "missing")
        self.assertEqual(database.get_global_config("poly_state_account-a-copy"), "keep")
        self.assertEqual(database.get_global_config("tg_chats"), "keep-global")

    def test_missing_account_does_not_delete_matching_legacy_poly_state(self):
        database.set_global_config("poly_state_missing-account", "keep")

        deleted = database.delete_account("missing-account")

        self.assertFalse(deleted)
        self.assertEqual(
            database.get_global_config("poly_state_missing-account"), "keep"
        )

    def test_account_state_file_cleanup_is_exact_and_idempotent(self):
        root = Path(self.temp_dir.name) / "state"
        root.mkdir()
        expected = {
            "pending_sells_d_account-a.json",
            "pending_sells_s_account-a.json",
            "cost_layers_account-a.json",
        }
        for name in expected:
            (root / name).write_text("{}", encoding="utf-8")
        preserved = {
            "pending_sells_d_account-a-copy.json",
            "cost_layers_account-a.json.bak",
            "unrelated.json",
        }
        for name in preserved:
            (root / name).write_text("{}", encoding="utf-8")

        first = database.cleanup_account_state_files("account-a", data_dir=root)
        second = database.cleanup_account_state_files("account-a", data_dir=root)

        self.assertTrue(first["complete"])
        self.assertEqual(set(first["deleted"]), expected)
        self.assertEqual(first["missing"], [])
        self.assertTrue(second["complete"])
        self.assertEqual(set(second["missing"]), expected)
        self.assertEqual(second["deleted"], [])
        self.assertEqual({path.name for path in root.iterdir()}, preserved)

    def test_account_state_file_cleanup_rejects_unsafe_account_id(self):
        root = Path(self.temp_dir.name) / "state"
        root.mkdir()
        sentinel = root / "sentinel.json"
        sentinel.write_text("keep", encoding="utf-8")

        for account_id in ("", "../account-a", "account/a", "account.a", " account-a"):
            with self.subTest(account_id=account_id):
                with self.assertRaisesRegex(ValueError, "Invalid account ID"):
                    database.cleanup_account_state_files(account_id, data_dir=root)

        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_account_state_file_cleanup_continues_after_one_file_error(self):
        root = Path(self.temp_dir.name) / "state"
        root.mkdir()
        (root / "pending_sells_d_account-a.json").write_text("{}", encoding="utf-8")
        (root / "pending_sells_s_account-a.json").write_text("{}", encoding="utf-8")
        blocked = root / "cost_layers_account-a.json"
        blocked.mkdir()

        result = database.cleanup_account_state_files("account-a", data_dir=root)

        self.assertFalse(result["complete"])
        self.assertEqual(
            set(result["deleted"]),
            {
                "pending_sells_d_account-a.json",
                "pending_sells_s_account-a.json",
            },
        )
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["errors"][0]["name"], "cost_layers_account-a.json")
        self.assertEqual(result["errors"][0]["status"], "error")
        self.assertIn("error", result["errors"][0])
        self.assertTrue(blocked.is_dir())

    def test_delete_account_rejects_unsafe_account_id_before_database_write(self):
        database.set_global_config("tg_chats", "keep-global")

        with self.assertRaisesRegex(ValueError, "Invalid account ID"):
            database.delete_account("../account-a")

        self.assertIsNotNone(database.get_account("account-a"))
        self.assertEqual(database.get_global_config("tg_chats"), "keep-global")

    def test_corrupt_snapshot_is_reported_without_rewriting_the_row(self):
        connection = database.get_db()
        connection.execute(
            """
            INSERT INTO sniper_state (account_id, config_json, runtime_json, running)
            VALUES (?, ?, ?, ?)
            """,
            ("account-a", "{broken", "[]", 1),
        )
        connection.commit()
        connection.close()

        snapshot = database.get_sniper_state("account-a")

        self.assertTrue(snapshot["corrupt"])
        self.assertEqual(
            snapshot["corrupt_fields"],
            ["config_json", "runtime_json"],
        )
        connection = database.get_db()
        raw = connection.execute(
            "SELECT config_json, runtime_json FROM sniper_state WHERE account_id = ?",
            ("account-a",),
        ).fetchone()
        connection.close()
        self.assertEqual(raw["config_json"], "{broken")
        self.assertEqual(raw["runtime_json"], "[]")


if __name__ == "__main__":
    unittest.main()
