import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from yarl import URL

from backend.core import proxy_nodes
from backend.services import latency_probe


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_SPEC = importlib.util.spec_from_file_location(
    "proxy_nodes_cli_for_tests",
    ROOT / "scripts" / "proxy_nodes.py",
)
if SCRIPT_SPEC is None or SCRIPT_SPEC.loader is None:
    raise RuntimeError("could not load scripts/proxy_nodes.py")
proxy_nodes_cli = importlib.util.module_from_spec(SCRIPT_SPEC)
SCRIPT_SPEC.loader.exec_module(proxy_nodes_cli)


class _ProbeResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def read(self):
        return b'{"code":"0","data":[{"bids":[],"asks":[]}]}'


class _RecordingSession:
    def __init__(self):
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return _ProbeResponse()


class ProxyNodeNormalisationTests(unittest.TestCase):
    def test_builtin_ids_stay_stable_while_labels_are_canonical(self):
        nodes = proxy_nodes.load_proxy_nodes(Path("missing-proxy-registry.json"))

        self.assertEqual(
            set(nodes),
            {"JP1", "JP2", "OSAKA", "SYD1", "SYD2"},
        )
        self.assertIn("JP1", nodes)
        self.assertEqual(nodes["JP1"]["name"], "JP-TYO-01")
        self.assertEqual(
            nodes["JP1"]["label"],
            "JP-TYO-01 | Tokyo | SOCKS5",
        )
        self.assertEqual(nodes["JP2"]["route"], "direct")
        self.assertIsNone(nodes["JP2"]["host"])
        self.assertTrue(
            {"JP3", "JP4", "JP5", "SG1", "K1"}.isdisjoint(nodes)
        )

    def test_retired_nodes_cannot_be_resolved_or_registered(self):
        retired_names = {
            "JP3": "JP-TYO-03",
            "JP4": "JP-TYO-04",
            "JP5": "JP-TYO-05",
            "SG1": "SG-SIN-01",
            "K1": "KR-SEL-01",
        }
        self.assertEqual(
            proxy_nodes.RETIRED_PROXY_NODE_IDS,
            frozenset(retired_names),
        )
        for node_id, name in retired_names.items():
            with self.subTest(node_id=node_id):
                with self.assertRaisesRegex(ValueError, "retired"):
                    proxy_nodes.require_proxy_node(node_id)
                with self.assertRaisesRegex(ValueError, "retired"):
                    proxy_nodes.normalise_proxy_node(
                        node_id,
                        {
                            "name": name,
                            "location": "Retired",
                            "route": "tunnel",
                            "host": "127.0.0.1",
                            "port": 9115,
                        },
                    )

    def test_direct_route_requires_null_proxy_coordinates(self):
        with self.assertRaisesRegex(ValueError, "null host and port"):
            proxy_nodes.normalise_proxy_node(
                "LOCAL",
                {
                    "name": "JP-TYO-99",
                    "location": "Tokyo",
                    "route": "direct",
                    "host": "should-not-be-used.example",
                    "port": 1234,
                },
            )

        node = proxy_nodes.normalise_proxy_node(
            "LOCAL",
            {
                "name": "JP-TYO-99",
                "location": "Tokyo",
                "route": "direct",
                "host": None,
                "port": None,
            },
        )
        self.assertEqual(node["label"], "JP-TYO-99 | Tokyo | Direct")

    def test_canonical_name_and_loopback_tunnel_are_enforced(self):
        with self.assertRaisesRegex(ValueError, "CC-CITY-NN"):
            proxy_nodes.normalise_proxy_node(
                "NODE1",
                {
                    "name": "Tokyo 1",
                    "location": "Tokyo",
                    "route": "socks5",
                    "host": "127.0.0.1",
                    "port": 9111,
                },
            )

        with self.assertRaisesRegex(ValueError, "127.0.0.1"):
            proxy_nodes.normalise_proxy_node(
                "NODE1",
                {
                    "name": "JP-TYO-98",
                    "location": "Tokyo",
                    "route": "tunnel",
                    "host": "203.0.113.10",
                    "port": 9111,
                },
            )


class ProxyNodeRegistryTests(unittest.TestCase):
    def _write_registry(self, directory: str, payload) -> Path:
        path = Path(directory) / "proxy_nodes.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_registry_can_override_stable_id_and_add_a_node(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP1": {
                        "name": "JP-TYO-01",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9211,
                    },
                    "JP-TYO-06": {
                        "name": "JP-TYO-06",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9216,
                    },
                },
            })

            nodes = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(nodes["JP1"]["port"], 9211)
        self.assertEqual(nodes["JP1"]["route"], "tunnel")
        self.assertEqual(nodes["JP-TYO-06"]["name"], "JP-TYO-06")

    def test_bad_custom_entries_do_not_replace_builtins_or_duplicate_names(self):
        original = proxy_nodes.load_proxy_nodes(Path("missing-proxy-registry.json"))
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP1": {
                        "name": "not-canonical",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9211,
                    },
                    "DUPLICATE": {
                        "name": "JP-TYO-02",
                        "location": "Tokyo",
                        "route": "direct",
                    },
                    "SG-SIN-02": {
                        "name": "SG-SIN-02",
                        "location": "Singapore",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9212,
                    },
                },
            })

            nodes = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(nodes["JP1"], original["JP1"])
        self.assertNotIn("DUPLICATE", nodes)
        # A globally ambiguous registry is rejected as a unit rather than
        # exposing a partially different node set to different processes.
        self.assertNotIn("SG-SIN-02", nodes)

    def test_corrupt_registry_falls_back_to_builtins(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proxy_nodes.json"
            path.write_text("{broken", encoding="utf-8")

            nodes = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(set(nodes), set(proxy_nodes.BUILTIN_PROXY_NODES))

    def test_local_registry_cannot_revive_retired_nodes(self):
        retired_names = {
            "JP3": "JP-TYO-03",
            "JP4": "JP-TYO-04",
            "JP5": "JP-TYO-05",
            "SG1": "SG-SIN-01",
            "K1": "KR-SEL-01",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    node_id: {
                        "name": name,
                        "location": "Retired",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9200 + index,
                    }
                    for index, (node_id, name) in enumerate(retired_names.items())
                },
            })

            nodes = proxy_nodes.load_proxy_nodes(path)

        self.assertTrue(set(retired_names).isdisjoint(nodes))

    def test_registration_cli_rejects_retired_id_without_writing_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP1": {
                        "name": "JP-TYO-01",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9211,
                    },
                    "JP-TYO-06": {
                        "name": "JP-TYO-06",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9216,
                    },
                },
            })
            before = path.read_bytes()
            command = [
                sys.executable,
                str(ROOT / "scripts" / "proxy_nodes.py"),
                "--file",
                str(path),
                "add",
                "--id",
                "SG1",
                "--name",
                "SG-SIN-01",
                "--location",
                "Singapore",
                "--port",
                "9216",
                "--replace",
            ]

            rejected = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("retired", rejected.stderr)
            self.assertEqual(path.read_bytes(), before)

    def test_cli_list_rejects_legacy_retired_override_without_rewriting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP3": {
                        "name": "JP-TYO-03",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9213,
                    },
                },
            })
            before = path.read_bytes()

            listed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "proxy_nodes.py"),
                    "--file",
                    str(path),
                    "list",
                ],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(listed.returncode, 0)
            self.assertIn("retired", listed.stderr)
            self.assertEqual(path.read_bytes(), before)

    def test_registration_cli_writes_the_backend_schema_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proxy_nodes.json"
            command = [
                sys.executable,
                str(ROOT / "scripts" / "proxy_nodes.py"),
                "--file",
                str(path),
                "add",
                "--id",
                "JP-TYO-06",
                "--location",
                "Tokyo",
                "--port",
                "9216",
            ]
            added = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )
            self.assertEqual(added.returncode, 0, added.stderr)
            before_duplicate = path.read_bytes()

            duplicate = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(duplicate.returncode, 0)
            self.assertEqual(path.read_bytes(), before_duplicate)
            payload = json.loads(path.read_text(encoding="utf-8"))
            loaded = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(payload["version"], proxy_nodes.REGISTRY_SCHEMA_VERSION)
        self.assertEqual(
            set(payload["nodes"]["JP-TYO-06"]),
            {"name", "location", "route", "host", "port"},
        )
        self.assertEqual(loaded["JP-TYO-06"]["route"], "tunnel")
        self.assertEqual(loaded["JP-TYO-06"]["host"], "127.0.0.1")

    def test_remove_cli_atomically_removes_only_selected_custom_node(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP-TYO-06": {
                        "name": "JP-TYO-06",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9216,
                    },
                    "SG-SIN-02": {
                        "name": "SG-SIN-02",
                        "location": "Singapore",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9217,
                    },
                },
            })

            removed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "proxy_nodes.py"),
                    "--file",
                    str(path),
                    "remove",
                    "--id",
                    "JP-TYO-06",
                ],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            effective = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertIn("removed custom node JP-TYO-06", removed.stdout)
        self.assertNotIn("JP-TYO-06", payload["nodes"])
        self.assertNotIn("JP-TYO-06", effective)
        self.assertIn("SG-SIN-02", payload["nodes"])
        self.assertIn("SG-SIN-02", effective)

    def test_remove_cli_refuses_builtin_and_unknown_without_registry_change(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP-TYO-06": {
                        "name": "JP-TYO-06",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9216,
                    },
                },
            })
            before = path.read_bytes()
            base = [
                sys.executable,
                str(ROOT / "scripts" / "proxy_nodes.py"),
                "--file",
                str(path),
                "remove",
                "--id",
            ]

            builtin = subprocess.run(
                [*base, "JP1"], cwd=ROOT, text=True, encoding="utf-8",
                capture_output=True, check=False,
            )
            unknown = subprocess.run(
                [*base, "UNKNOWN"], cwd=ROOT, text=True, encoding="utf-8",
                capture_output=True, check=False,
            )

            self.assertNotEqual(builtin.returncode, 0)
            self.assertIn("builtin", builtin.stderr)
            self.assertNotEqual(unknown.returncode, 0)
            self.assertIn("unknown", unknown.stderr)
            self.assertEqual(path.read_bytes(), before)

    def test_remove_cli_removes_only_builtin_override_and_restores_builtin(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP1": {
                        "name": "JP-TYO-01",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9211,
                    },
                    "JP-TYO-06": {
                        "name": "JP-TYO-06",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9216,
                    },
                },
            })

            removed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "proxy_nodes.py"),
                    "--file",
                    str(path),
                    "remove",
                    "--id",
                    "JP1",
                ],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            effective = proxy_nodes.load_proxy_nodes(path)

        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertIn("builtin node remains active", removed.stdout)
        self.assertNotIn("JP1", payload["nodes"])
        self.assertEqual(effective["JP1"]["route"], "socks5")
        self.assertEqual(effective["JP1"]["host"], "168.138.196.214")
        self.assertIn("JP-TYO-06", payload["nodes"])

    def test_remove_cli_can_safely_purge_one_legacy_retired_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_registry(directory, {
                "version": 1,
                "nodes": {
                    "JP3": {
                        "name": "JP-TYO-03",
                        "location": "Tokyo",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9213,
                    },
                    "SG1": {
                        "name": "SG-SIN-01",
                        "location": "Singapore",
                        "route": "tunnel",
                        "host": "127.0.0.1",
                        "port": 9214,
                    },
                },
            })

            removed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "proxy_nodes.py"),
                    "--file",
                    str(path),
                    "remove",
                    "--id",
                    "JP3",
                ],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=False,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertNotIn("JP3", payload["nodes"])
        self.assertIn("SG1", payload["nodes"])


class ProxyNodeBenchmarkCliTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _args(**overrides):
        values = {
            "region": "AU",
            "samples": 1,
            "yes_asset_id": "",
            "poly_token_id": "",
            "concurrency": 1,
            "account_id": "",
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    async def test_yes_asset_without_account_is_rejected_before_benchmark(self):
        benchmark = mock.AsyncMock()
        args = self._args(yes_asset_id="yes-asset-1")

        with (
            mock.patch.object(latency_probe, "benchmark_proxy_nodes", benchmark),
            self.assertRaisesRegex(
                proxy_nodes_cli.RegistryError,
                "--account-id is required",
            ),
        ):
            await proxy_nodes_cli._run_benchmark(args, {"JP2": {}})

        benchmark.assert_not_awaited()

    async def test_account_region_must_match_requested_region(self):
        benchmark = mock.AsyncMock()
        account = {
            "base_url": "https://www.okx.com",
            "api_key": "key",
            "secret_key": "secret",
            "passphrase": "passphrase",
        }
        args = self._args(
            region="AU",
            yes_asset_id="yes-asset-1",
            account_id="account-cn",
        )

        with (
            mock.patch(
                "backend.core.database.get_account_decrypted",
                return_value=account,
            ),
            mock.patch.object(latency_probe, "benchmark_proxy_nodes", benchmark),
            self.assertRaisesRegex(
                proxy_nodes_cli.RegistryError,
                "account region is CN, not requested AU",
            ),
        ):
            await proxy_nodes_cli._run_benchmark(args, {"JP2": {}})

        benchmark.assert_not_awaited()

    async def test_connectivity_benchmark_does_not_need_or_load_an_account(self):
        expected = {"schema_version": 2, "latencies": {}}
        benchmark = mock.AsyncMock(return_value=expected)
        args = self._args()

        with (
            mock.patch(
                "backend.core.database.get_account_decrypted",
                side_effect=AssertionError("connectivity probe loaded an account"),
            ),
            mock.patch.object(latency_probe, "benchmark_proxy_nodes", benchmark),
        ):
            result = await proxy_nodes_cli._run_benchmark(args, {"JP2": {}})

        self.assertIs(result, expected)
        benchmark.assert_awaited_once()
        self.assertIsNone(benchmark.await_args.kwargs["header_factory"])
        self.assertEqual(benchmark.await_args.kwargs["yes_asset_id"], "")

    def test_cli_connectivity_benchmark_accepts_missing_account_id(self):
        parsed = proxy_nodes_cli.build_parser().parse_args([
            "benchmark",
            "--region",
            "AU",
            "--samples",
            "1",
            "--node",
            "JP2",
        ])

        self.assertIsNone(parsed.account_id)

    async def test_signed_path_exactly_matches_outgoing_book_url(self):
        account = {
            "base_url": "https://us.okx.com",
            "api_key": "key",
            "secret_key": "secret",
            "passphrase": "passphrase",
        }
        auth = mock.Mock()
        auth.headers.return_value = {"OK-ACCESS-KEY": "key"}
        args = self._args(
            yes_asset_id="yes:asset-1",
            account_id="account-au",
        )
        target = latency_probe._okx_target(args.region, args.yes_asset_id)
        session = _RecordingSession()

        with (
            mock.patch(
                "backend.core.database.get_account_decrypted",
                return_value=account,
            ),
            mock.patch("backend.core.auth.OKXAuth", return_value=auth),
        ):
            header_factory = proxy_nodes_cli._benchmark_header_factory(args)
            await latency_probe._request_once(session, target, header_factory)

        auth.headers.assert_called_once_with("GET", target["signed_path"])
        self.assertEqual(len(session.requests), 1)
        outgoing_url, options = session.requests[0]
        outgoing_url = URL(outgoing_url)
        self.assertIsNone(options["params"])
        self.assertEqual(outgoing_url.raw_path_qs, target["signed_path"])
        self.assertEqual(
            str(outgoing_url),
            latency_probe.OKX_BASE_URLS[args.region] + target["signed_path"],
        )


if __name__ == "__main__":
    unittest.main()
