import unittest
from types import SimpleNamespace
from unittest import mock

from backend import app as app_module
from backend import ws_book


class WebSocketTokenTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with app_module._ws_tokens_lock:
            app_module._ws_tokens.clear()

    def tearDown(self):
        with app_module._ws_tokens_lock:
            app_module._ws_tokens.clear()

    def test_token_is_account_bound_and_one_use(self):
        token, expires_in = app_module._issue_ws_token("account-a")

        self.assertEqual(expires_in, 30)
        self.assertFalse(app_module._consume_ws_token(token, "account-b"))
        self.assertFalse(app_module._consume_ws_token(token, "account-a"))

        second, _ = app_module._issue_ws_token("account-a")
        self.assertTrue(app_module._consume_ws_token(second, "account-a"))
        self.assertFalse(app_module._consume_ws_token(second, "account-a"))

    def test_expired_token_is_rejected(self):
        with mock.patch.object(app_module.time, "monotonic", return_value=100.0):
            token, _ = app_module._issue_ws_token("account-a")
        with mock.patch.object(app_module.time, "monotonic", return_value=131.0):
            self.assertFalse(app_module._consume_ws_token(token, "account-a"))

    async def test_token_endpoint_requires_a_loaded_account(self):
        manager = SimpleNamespace(get_instance=lambda account_id: object() if account_id == "account-a" else None)
        with mock.patch.object(app_module, "manager", manager):
            payload = await app_module.account_ws_token("account-a", user="test")

        self.assertTrue(payload["token"])
        self.assertEqual(payload["expires_in"], 30)


class WebSocketRouteAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_unauthenticated_socket_is_closed_before_accept(self):
        class FakeApp:
            handler = None

            def websocket(self, _path):
                def register(handler):
                    self.handler = handler
                    return handler

                return register

        class FakeSocket:
            query_params = {}

            def __init__(self):
                self.accepted = False
                self.closed = None

            async def accept(self):
                self.accepted = True

            async def close(self, code=1000, reason=None):
                self.closed = (code, reason)

        app = FakeApp()
        ws_book.register_ws_routes(
            app,
            SimpleNamespace(),
            token_validator=lambda _token, _account_id: False,
        )
        socket = FakeSocket()

        await app.handler(socket, "account-a")

        self.assertFalse(socket.accepted)
        self.assertEqual(socket.closed[0], 4401)


if __name__ == "__main__":
    unittest.main()
