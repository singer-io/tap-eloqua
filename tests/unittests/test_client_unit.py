import json
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from tap_eloqua.client import EloquaClient, Server5xxError


class TestClientUnit(unittest.TestCase):
    def _build_client(self, config_path):
        return EloquaClient(
            config_path=config_path,
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            redirect_uri="https://localhost/callback",
            user_agent="tap-eloqua-tests",
        )

    def test_get_access_token_reuses_valid_cached_token(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        session = MagicMock()

        with patch.object(client, "_EloquaClient__session", session):
            client._EloquaClient__access_token = "cached-token"
            client._EloquaClient__expires = datetime.utcnow() + timedelta(minutes=5)
            client.get_access_token()

        session.post.assert_not_called()

    def test_get_access_token_fetches_and_persists_rotated_refresh_token(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "access_token": "new-access-token",
            "refresh_token": "rotated-refresh-token",
            "expires_in": 3600,
        }

        session = MagicMock()
        session.post.return_value = response

        with patch.object(client, "_EloquaClient__session", session):
            client.get_access_token()

        self.assertEqual(client._EloquaClient__access_token, "new-access-token")
        self.assertEqual(client._EloquaClient__refresh_token, "rotated-refresh-token")
        self.assertIsNotNone(client._EloquaClient__expires)

        with open(config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        self.assertEqual(config["refresh_token"], "rotated-refresh-token")

    def test_get_access_token_raises_server_error_for_5xx(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        response = MagicMock()
        response.status_code = 500
        session = MagicMock()
        session.post.return_value = response

        with patch.object(client, "_EloquaClient__session", session):
            with self.assertRaises(Server5xxError):
                client.get_access_token()

    def test_get_access_token_raises_for_non_200(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        response = MagicMock()
        response.status_code = 401
        response.json.return_value = {"error": "invalid_client"}
        session = MagicMock()
        session.post.return_value = response

        with patch.object(client, "_EloquaClient__session", session):
            with self.assertRaises(Exception) as error_context:
                client.get_access_token()
        self.assertIn("Unable to authenticate", str(error_context.exception))

    def test_enter_calls_get_access_token_and_returns_self(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        with patch.object(client, "get_access_token") as mock_access_token:
            returned_client = client.__enter__()
        self.assertIs(returned_client, client)
        mock_access_token.assert_called_once_with()

    def test_exit_closes_session(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        session = MagicMock()
        with patch.object(client, "_EloquaClient__session", session):
            client.__exit__(None, None, None)
        session.close.assert_called_once_with()

    def test_get_base_urls_sets_base_url(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        with patch.object(client, "request", return_value={"urls": {"base": "https://api.eloqua.test"}}) as mock_request:
            client.get_base_urls()

        self.assertEqual(client._EloquaClient__base_url, "https://api.eloqua.test")
        mock_request.assert_called_once_with("GET", url="https://login.eloqua.com/id", endpoint="base_url")

    def test_request_sets_headers_and_content_type_for_post(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        client._EloquaClient__base_url = "https://api.eloqua.test"
        client._EloquaClient__access_token = "token-123"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        class DummyTimer:
            def __init__(self):
                self.tags = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

        session = MagicMock()
        session.request.return_value = response

        with patch.object(client, "_EloquaClient__session", session), \
             patch.object(client, "get_access_token"), \
             patch("tap_eloqua.client.metrics.http_request_timer", return_value=DummyTimer()):
            payload = client.request("POST", path="/v1/resource", endpoint="test_endpoint")

        self.assertEqual(payload, {"ok": True})
        called_kwargs = session.request.call_args.kwargs
        self.assertEqual(called_kwargs["headers"]["Authorization"], "Bearer token-123")
        self.assertEqual(called_kwargs["headers"]["User-Agent"], "tap-eloqua-tests")
        self.assertEqual(called_kwargs["headers"]["Content-Type"], "application/json")

    def test_request_uses_existing_headers_and_no_endpoint(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        client._EloquaClient__base_url = "https://api.eloqua.test"
        client._EloquaClient__access_token = "token-123"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        class DummyTimer:
            def __init__(self):
                self.tags = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

        session = MagicMock()
        session.request.return_value = response

        with patch.object(client, "_EloquaClient__session", session), \
             patch.object(client, "get_access_token"), \
             patch("tap_eloqua.client.metrics.http_request_timer", return_value=DummyTimer()):
            payload = client.request("GET", path="/v1/resource", headers={"X-Test": "1"})

        self.assertEqual(payload, {"ok": True})
        called_kwargs = session.request.call_args.kwargs
        self.assertEqual(called_kwargs["headers"]["X-Test"], "1")
        self.assertEqual(called_kwargs["headers"]["Authorization"], "Bearer token-123")

    def test_request_fetches_base_url_when_missing(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        client._EloquaClient__access_token = "token-123"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        class DummyTimer:
            def __init__(self):
                self.tags = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

        session = MagicMock()
        session.request.return_value = response

        with patch.object(client, "_EloquaClient__session", session), \
             patch.object(client, "get_access_token"), \
             patch.object(client, "get_base_urls", side_effect=lambda: setattr(client, "_EloquaClient__base_url", "https://api.eloqua.test")) as mock_get_base_urls, \
             patch("tap_eloqua.client.metrics.http_request_timer", return_value=DummyTimer()):
            payload = client.request("GET", path="/v1/resource")

        self.assertEqual(payload, {"ok": True})
        mock_get_base_urls.assert_called_once_with()

    def test_request_raises_server_error_for_5xx(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        client._EloquaClient__base_url = "https://api.eloqua.test"
        client._EloquaClient__access_token = "token-123"

        response = MagicMock()
        response.status_code = 503

        class DummyTimer:
            def __init__(self):
                self.tags = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

        session = MagicMock()
        session.request.return_value = response

        with patch.object(client, "_EloquaClient__session", session), \
             patch.object(client, "get_access_token"), \
             patch("tap_eloqua.client.metrics.http_request_timer", return_value=DummyTimer()):
            with self.assertRaises(Server5xxError):
                client.request("GET", path="/v1/resource", endpoint="test_endpoint")

    def test_get_and_post_delegate_to_request(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"refresh_token": "original"}, config_file)
            config_path = config_file.name

        client = self._build_client(config_path)
        with patch.object(client, "request", side_effect=[{"x": 1}, {"y": 2}]) as mock_request:
            get_result = client.get("/foo", endpoint="e1")
            post_result = client.post("/bar", endpoint="e2")

        self.assertEqual(get_result, {"x": 1})
        self.assertEqual(post_result, {"y": 2})
        self.assertEqual(mock_request.call_count, 2)


if __name__ == "__main__":
    unittest.main()
