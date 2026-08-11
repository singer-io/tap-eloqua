import json
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from tap_eloqua.client import EloquaClient


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


if __name__ == "__main__":
    unittest.main()
