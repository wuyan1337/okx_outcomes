import ast
import asyncio
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import types
import unittest
from unittest import mock
import importlib.util


ROOT = Path(__file__).resolve().parents[1]


def _load_ws_book_module():
    """Load ws_book without requiring the optional web/runtime dependencies."""
    fastapi_stub = types.ModuleType("fastapi")
    fastapi_stub.WebSocket = type("WebSocket", (), {})
    fastapi_stub.WebSocketDisconnect = type("WebSocketDisconnect", (Exception,), {})

    socks_stub = types.ModuleType("aiohttp_socks")
    socks_stub.ProxyConnector = type(
        "ProxyConnector",
        (),
        {"from_url": classmethod(lambda cls, url: (cls, url))},
    )

    database_stub = types.ModuleType("backend.core.database")
    database_stub.PROXY_NODES = {}

    module_name = "backend._ws_book_test"
    spec = importlib.util.spec_from_file_location(module_name, ROOT / "backend" / "ws_book.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "fastapi": fastapi_stub,
            "aiohttp_socks": socks_stub,
            "backend.core.database": database_stub,
            module_name: module,
        },
    ):
        spec.loader.exec_module(module)
    return module


WS_BOOK = _load_ws_book_module()


def _load_base_path_symbols():
    """Compile only the dependency-free middleware definitions from app.py."""
    source_path = ROOT / "backend" / "app.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    wanted = {"_normalise_base_path", "BasePathMiddleware"}
    nodes = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in wanted
    ]
    if {node.name for node in nodes} != wanted:
        raise AssertionError("Base path middleware definitions were not found in backend/app.py")
    namespace = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source_path), "exec"), namespace)
    return namespace["_normalise_base_path"], namespace["BasePathMiddleware"]


NORMALISE_BASE_PATH, BASE_PATH_MIDDLEWARE = _load_base_path_symbols()


class _FakeResponse:
    status = 503

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return {}


class _FakeUpstreamWebSocket:
    def __init__(self, messages, stop_when_exhausted=None):
        self.messages = list(messages)
        self.stop_when_exhausted = stop_when_exhausted
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            return self.messages.pop(0)
        if self.stop_when_exhausted is not None:
            self.stop_when_exhausted.set()
        raise StopAsyncIteration

    async def send_str(self, payload):
        self.sent.append(json.loads(payload))


class _FakeDashboardSocket:
    def __init__(self):
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(payload)


def _book_message(action, bids, asks, seq, prev):
    payload = {
        "arg": {"channel": "pm-books", "instId": "asset-1"},
        "action": action,
        "data": [{"bids": bids, "asks": asks, "seqId": seq, "prevSeqId": prev}],
    }
    return types.SimpleNamespace(
        type=WS_BOOK.aiohttp.WSMsgType.TEXT,
        data=json.dumps(payload),
    )


class OkxBookTests(unittest.IsolatedAsyncioTestCase):
    def test_endpoint_matches_outcomes_hosts(self):
        self.assertEqual(
            WS_BOOK._okx_ws_endpoint("https://www.okx.com"),
            "wss://ws.okx.com:8443/ws/v5/business",
        )
        self.assertEqual(
            WS_BOOK._okx_ws_endpoint("https://us.okx.com"),
            "wss://wsus.okx.com/ws/v5/business",
        )

    def test_level_updates_delete_and_sort(self):
        bids, asks = WS_BOOK._okx_book_maps(
            [["0.40", "10", "1"], ["0.39", "5", "1"]],
            [["0.60", "8", "1"]],
        )
        WS_BOOK._apply_okx_levels(bids, [["0.40", "0", "0"], ["0.395", "7", "1"]])
        WS_BOOK._apply_okx_levels(asks, [["0.60", "9", "2"], ["0.61", "3", "1"]])
        out_bids, out_asks = WS_BOOK._okx_book_from_maps(bids, asks)
        self.assertEqual([row[0] for row in out_bids], ["0.395", "0.39"])
        self.assertEqual([row[0] for row in out_asks], ["0.60", "0.61"])
        self.assertEqual(out_asks[0][1:], ["9", "2"])

    async def test_snapshot_then_contiguous_update(self):
        stop_event = asyncio.Event()
        upstream = _FakeUpstreamWebSocket(
            [
                _book_message(
                    "snapshot",
                    [["0.40", "10", "1"]],
                    [["0.60", "8", "1"]],
                    10,
                    -1,
                ),
                _book_message(
                    "update",
                    [["0.40", "0", "0"], ["0.39", "5", "1"]],
                    [["0.60", "9", "2"]],
                    11,
                    10,
                ),
            ],
            stop_event,
        )
        connections = []

        class FakeSession:
            def __init__(self, **kwargs):
                connections.append(kwargs)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def get(self, *args, **kwargs):
                return _FakeResponse()

            def ws_connect(self, *args, **kwargs):
                return upstream

        dashboard = _FakeDashboardSocket()
        with mock.patch.object(WS_BOOK.aiohttp, "ClientSession", FakeSession):
            await WS_BOOK.okx_ws_relay(
                dashboard,
                "asset-1",
                "",
                "https://us.okx.com",
                stop_event,
            )

        self.assertEqual(len(connections), 1)
        self.assertEqual(upstream.sent, [{"op": "subscribe", "args": [{"channel": "pm-books", "instId": "asset-1"}]}])
        self.assertEqual(len(dashboard.messages), 2)
        self.assertEqual(dashboard.messages[-1]["bids"], [["0.39", "5", "1"]])
        self.assertEqual(dashboard.messages[-1]["asks"], [["0.60", "9", "2"]])

    async def test_sequence_gap_discards_delta_and_reconnects(self):
        stop_event = asyncio.Event()
        first = _FakeUpstreamWebSocket(
            [
                _book_message("snapshot", [["0.40", "10", "1"]], [], 10, -1),
                _book_message("update", [["0.41", "99", "1"]], [], 11, 8),
            ]
        )
        second = _FakeUpstreamWebSocket(
            [_book_message("snapshot", [["0.50", "4", "1"]], [], 20, -1)],
            stop_event,
        )
        upstreams = [first, second]
        connected_urls = []

        class FakeSession:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def get(self, *args, **kwargs):
                return _FakeResponse()

            def ws_connect(self, url, **kwargs):
                connected_urls.append(url)
                return upstreams.pop(0)

        async def no_delay(_seconds):
            return None

        dashboard = _FakeDashboardSocket()
        with (
            mock.patch.object(WS_BOOK.aiohttp, "ClientSession", FakeSession),
            mock.patch.object(WS_BOOK.asyncio, "sleep", no_delay),
        ):
            await WS_BOOK.okx_ws_relay(
                dashboard,
                "asset-1",
                "",
                "https://us.okx.com",
                stop_event,
            )

        self.assertEqual(
            connected_urls,
            [
                "wss://wsus.okx.com/ws/v5/business",
                "wss://wsus.okx.com/ws/v5/business",
            ],
        )
        self.assertEqual([msg["bids"] for msg in dashboard.messages], [
            [["0.40", "10", "1"]],
            [["0.50", "4", "1"]],
        ])

    async def test_update_before_ws_snapshot_forces_resync(self):
        stop_event = asyncio.Event()
        first = _FakeUpstreamWebSocket(
            [_book_message("update", [["0.41", "99", "1"]], [], 11, 10)]
        )
        second = _FakeUpstreamWebSocket(
            [_book_message("snapshot", [["0.50", "4", "1"]], [], 20, -1)],
            stop_event,
        )
        upstreams = [first, second]

        class FakeSession:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def get(self, *args, **kwargs):
                return _FakeResponse()

            def ws_connect(self, *args, **kwargs):
                return upstreams.pop(0)

        async def no_delay(_seconds):
            return None

        dashboard = _FakeDashboardSocket()
        with (
            mock.patch.object(WS_BOOK.aiohttp, "ClientSession", FakeSession),
            mock.patch.object(WS_BOOK.asyncio, "sleep", no_delay),
        ):
            await WS_BOOK.okx_ws_relay(
                dashboard,
                "asset-1",
                "",
                "https://us.okx.com",
                stop_event,
            )

        self.assertEqual([msg["bids"] for msg in dashboard.messages], [
            [["0.50", "4", "1"]],
        ])


class BasePathMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def test_normalisation(self):
        self.assertEqual(NORMALISE_BASE_PATH(""), "")
        self.assertEqual(NORMALISE_BASE_PATH("/"), "")
        self.assertEqual(NORMALISE_BASE_PATH(" okx/ "), "/okx")
        self.assertEqual(NORMALISE_BASE_PATH("/desk/okx/"), "/desk/okx")

    async def _scope_after_middleware(self, configured_prefix, scope):
        captured = []

        async def downstream(received_scope, receive, send):
            captured.append(received_scope)

        middleware = BASE_PATH_MIDDLEWARE(downstream, configured_prefix)

        async def receive():
            return {}

        async def send(_message):
            return None

        await middleware(scope, receive, send)
        self.assertEqual(len(captured), 1)
        return captured[0]

    async def test_root_path_is_unchanged_without_prefix(self):
        scope = {"type": "http", "path": "/", "raw_path": b"/", "headers": []}
        received = await self._scope_after_middleware("", scope)
        self.assertEqual(received["path"], "/")
        self.assertNotIn("root_path", received)

    async def test_prefix_preserving_http_request_is_stripped(self):
        scope = {
            "type": "http",
            "path": "/okx/api/status",
            "raw_path": b"/okx/api/status",
            "headers": [],
        }
        received = await self._scope_after_middleware("/okx", scope)
        self.assertEqual(received["root_path"], "/okx")
        self.assertEqual(received["path"], "/api/status")
        self.assertEqual(received["raw_path"], b"/api/status")

    async def test_prefix_root_maps_to_application_root(self):
        scope = {
            "type": "http",
            "path": "/okx",
            "raw_path": b"/okx",
            "headers": [],
        }
        received = await self._scope_after_middleware("/okx", scope)
        self.assertEqual(received["root_path"], "/okx")
        self.assertEqual(received["path"], "/")

    async def test_proxy_stripped_request_keeps_path_and_sets_root_path(self):
        scope = {
            "type": "http",
            "path": "/api/status",
            "raw_path": b"/api/status",
            "headers": [],
        }
        received = await self._scope_after_middleware("/okx", scope)
        self.assertEqual(received["root_path"], "/okx")
        self.assertEqual(received["path"], "/api/status")

    async def test_forwarded_prefix_and_websocket_are_supported(self):
        scope = {
            "type": "websocket",
            "path": "/okx/ws/book/account-1",
            "raw_path": b"/okx/ws/book/account-1",
            "headers": [(b"x-forwarded-prefix", b"/okx")],
        }
        received = await self._scope_after_middleware("", scope)
        self.assertEqual(received["root_path"], "/okx")
        self.assertEqual(received["path"], "/ws/book/account-1")


class FrontendPathAndSyntaxTests(unittest.TestCase):
    HTML_FILES = [
        ROOT / "frontend" / "dashboard.html",
        ROOT / "frontend" / "login.html",
        ROOT / "frontend" / "sniper-help.html",
    ]

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JavaScript syntax checks")
    def test_all_inline_javascript_parses(self):
        script_pattern = re.compile(r"<script(?:\s[^>]*)?>(.*?)</script>", re.IGNORECASE | re.DOTALL)
        for html_path in self.HTML_FILES:
            html = html_path.read_text(encoding="utf-8")
            for index, script in enumerate(script_pattern.findall(html), start=1):
                with self.subTest(file=html_path.name, script=index):
                    result = subprocess.run(
                        ["node", "--check", "-"],
                        input=script,
                        text=True,
                        encoding="utf-8",
                        capture_output=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for path-function checks")
    def test_frontend_base_path_detection(self):
        cases = {
            "dashboard.html": {
                "/dashboard": "",
                "/okx/dashboard": "/okx",
                "/desk/okx/dashboard/": "/desk/okx",
            },
            "login.html": {
                "/": "",
                "/login": "",
                "/okx": "/okx",
                "/okx/login/": "/okx",
            },
        }
        pattern = re.compile(r"function detectBasePath\(\)\{[^\r\n]+\}")
        for filename, path_cases in cases.items():
            html = (ROOT / "frontend" / filename).read_text(encoding="utf-8")
            match = pattern.search(html)
            self.assertIsNotNone(match, f"detectBasePath missing from {filename}")
            function_source = match.group(0)
            for pathname, expected in path_cases.items():
                with self.subTest(file=filename, pathname=pathname):
                    javascript = (
                        f"var location={{pathname:{json.dumps(pathname)}}};"
                        f"{function_source};process.stdout.write(detectBasePath());"
                    )
                    result = subprocess.run(
                        ["node", "-e", javascript],
                        text=True,
                        encoding="utf-8",
                        capture_output=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, expected)

    def test_dashboard_routes_http_and_websocket_through_base_path(self):
        html = (ROOT / "frontend" / "dashboard.html").read_text(encoding="utf-8")
        self.assertIn("fetch(apiUrl(p)", html)
        self.assertIn("fetch(apiUrl(np)", html)
        self.assertIn("location.host+BASE_PATH+'/ws/book/'", html)
        self.assertIn("document.getElementById('sn-help').href=apiUrl('/sniper-help')", html)
        self.assertIn("connectBook(_polyToken,a)", html)
        self.assertNotIn("function sob(a){\n  _okxAsset=a", html)
        self.assertIsNone(re.search(r"fetch\(\s*['\"]/(?:api|ws)/", html))

    def test_backend_reads_utf8_pages_explicitly(self):
        app_source = (ROOT / "backend" / "app.py").read_text(encoding="utf-8")
        for filename in ("login.html", "dashboard.html", "sniper-help.html"):
            self.assertIn(
                f'(frontend_dir / "{filename}").read_text(encoding="utf-8")',
                app_source,
            )

    def test_manual_sell_uses_available_size_without_rounding_shares(self):
        html = (ROOT / "frontend" / "dashboard.html").read_text(encoding="utf-8")
        app_source = (ROOT / "backend" / "app.py").read_text(encoding="utf-8")
        self.assertIn("pos.available_size", html)
        self.assertIn('"available_size": p.get("availableSize"', app_source)
        self.assertIsNone(
            re.search(r"mtSellInstant\([^\r\n;]*Math\.round", html)
        )


if __name__ == "__main__":
    unittest.main()
