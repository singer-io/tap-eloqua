import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

import requests

from tap_eloqua.client import EloquaClient, Server5xxError


class _TimerCtx:
    def __init__(self):
        self.tags = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestEloquaClientUnit(unittest.TestCase):
    def _client(self, config_path):
        return EloquaClient(
            config_path=config_path,
            client_id="client-id",
            client_secret="secret",
            refresh_token="refresh-token",
            redirect_uri="http://localhost/callback",
            user_agent="ua-test",
        )

    def test_context_manager_calls_auth_and_close(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.get_access_token = mock.MagicMock()
            session = client._EloquaClient__session
            session.close = mock.MagicMock()

            entered = client.__enter__()
            self.assertIs(entered, client)
            client.__exit__(None, None, None)

            client.get_access_token.assert_called_once()
            session.close.assert_called_once()
        finally:
            os.remove(cfg_path)

    def test_get_access_token_skips_when_unexpired(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client._EloquaClient__access_token = "existing"
            client._EloquaClient__expires = datetime.utcnow() + timedelta(minutes=2)
            client._EloquaClient__session.post = mock.MagicMock()

            EloquaClient.get_access_token.__wrapped__(client)

            client._EloquaClient__session.post.assert_not_called()
        finally:
            os.remove(cfg_path)

    def test_get_access_token_success_rotates_refresh_token(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            response = mock.MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 60,
            }
            client._EloquaClient__session.post = mock.MagicMock(return_value=response)

            EloquaClient.get_access_token.__wrapped__(client)

            self.assertEqual(client._EloquaClient__access_token, "new-access")
            self.assertEqual(client._EloquaClient__refresh_token, "new-refresh")
            self.assertIsNotNone(client._EloquaClient__expires)

            with open(cfg_path, "r", encoding="utf-8") as cfgf:
                written = json.load(cfgf)
            self.assertEqual(written["refresh_token"], "new-refresh")
        finally:
            os.remove(cfg_path)

    def test_get_access_token_raises_server_error(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            response = mock.MagicMock(status_code=503)
            client._EloquaClient__session.post = mock.MagicMock(return_value=response)

            with self.assertRaises(Server5xxError):
                EloquaClient.get_access_token.__wrapped__(client)
        finally:
            os.remove(cfg_path)

    def test_get_access_token_raises_on_non_200(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            response = mock.MagicMock(status_code=401)
            response.json.return_value = {"error": "unauthorized"}
            client._EloquaClient__session.post = mock.MagicMock(return_value=response)

            with self.assertRaises(Exception) as exc:
                EloquaClient.get_access_token.__wrapped__(client)
            self.assertIn("Unable to authenticate", str(exc.exception))
        finally:
            os.remove(cfg_path)

    @mock.patch("tap_eloqua.client.metrics.http_request_timer", return_value=_TimerCtx())
    def test_request_builds_url_and_headers(self, _mock_timer):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.get_access_token = mock.MagicMock()
            client._EloquaClient__base_url = "https://secure.eloqua.com"
            client._EloquaClient__access_token = "token-123"

            response = mock.MagicMock()
            response.status_code = 200
            response.raise_for_status.return_value = None
            response.json.return_value = {"ok": True}
            client._EloquaClient__session.request = mock.MagicMock(return_value=response)

            out = EloquaClient.request.__wrapped__(
                client,
                "POST",
                path="/api/test",
                endpoint="unit",
                headers={"X-Test": "1"},
            )

            self.assertEqual(out, {"ok": True})
            req_kwargs = client._EloquaClient__session.request.call_args.kwargs
            self.assertEqual(req_kwargs["headers"]["Authorization"], "Bearer token-123")
            self.assertEqual(req_kwargs["headers"]["User-Agent"], "ua-test")
            self.assertEqual(req_kwargs["headers"]["Content-Type"], "application/json")
            self.assertEqual(client._EloquaClient__session.request.call_args.args[1], "https://secure.eloqua.com/api/test")
        finally:
            os.remove(cfg_path)

    @mock.patch("tap_eloqua.client.metrics.http_request_timer", return_value=_TimerCtx())
    def test_request_fetches_base_url_when_missing(self, _mock_timer):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.get_access_token = mock.MagicMock()
            client.get_base_urls = mock.MagicMock(side_effect=lambda: setattr(client, "_EloquaClient__base_url", "https://base"))
            client._EloquaClient__access_token = "token-123"

            response = mock.MagicMock(status_code=200)
            response.raise_for_status.return_value = None
            response.json.return_value = {"ok": True}
            client._EloquaClient__session.request = mock.MagicMock(return_value=response)

            EloquaClient.request.__wrapped__(client, "GET", path="/path")
            client.get_base_urls.assert_called_once()
        finally:
            os.remove(cfg_path)

    @mock.patch("tap_eloqua.client.metrics.http_request_timer", return_value=_TimerCtx())
    def test_request_raises_server5xx(self, _mock_timer):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.get_access_token = mock.MagicMock()
            client._EloquaClient__base_url = "https://base"
            client._EloquaClient__access_token = "token"

            response = mock.MagicMock(status_code=500)
            client._EloquaClient__session.request = mock.MagicMock(return_value=response)

            with self.assertRaises(Server5xxError):
                EloquaClient.request.__wrapped__(client, "GET", path="/path")
        finally:
            os.remove(cfg_path)

    @mock.patch("tap_eloqua.client.metrics.http_request_timer", return_value=_TimerCtx())
    def test_request_raises_http_error(self, _mock_timer):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.get_access_token = mock.MagicMock()
            client._EloquaClient__base_url = "https://base"
            client._EloquaClient__access_token = "token"

            response = mock.MagicMock(status_code=404)
            response.raise_for_status.side_effect = requests.HTTPError("boom")
            client._EloquaClient__session.request = mock.MagicMock(return_value=response)

            with self.assertRaises(requests.HTTPError):
                EloquaClient.request.__wrapped__(client, "GET", path="/path")
        finally:
            os.remove(cfg_path)

    def test_get_and_post_wrappers(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump({"refresh_token": "initial"}, cfg)
            cfg_path = cfg.name
        try:
            client = self._client(cfg_path)
            client.request = mock.MagicMock(return_value={"ok": True})

            out_get = client.get("/g", params={"x": 1})
            out_post = client.post("/p", json={"a": 1})

            self.assertEqual(out_get, {"ok": True})
            self.assertEqual(out_post, {"ok": True})
            client.request.assert_any_call("GET", path="/g", params={"x": 1})
            client.request.assert_any_call("POST", path="/p", json={"a": 1})
        finally:
            os.remove(cfg_path)
