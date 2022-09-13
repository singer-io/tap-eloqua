import json
import unittest
from tap_eloqua.client import EloquaClient, strftime
from unittest.mock import patch
import datetime
import pytz


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def mocked_auth_post(*args, **kwargs):
    return MockResponse({"access_token": "hellothere", "refresh_token": "hello", "expires_in": 5000}, status_code=200)


class Test_ClientDevMode(unittest.TestCase):
    """Test the dev mode functionality"""

    base_config = {"client_id": "",
                   "client_secret": "",
                   "refresh_token": "",
                   "redirect_uri": "",
                   "user_agent": "", }
    tmp_config_path = "/tmp/eloqua_config.json"
    

    @patch("requests.Session.post", side_effect=mocked_auth_post)
    def test_client_without_dev_mode(self, mocked_auth_post):
        """checks if the client can write refresh token and expiry to config"""

        config_sample = self.base_config

        with open(self.tmp_config_path, "w") as ff:
            json.dump(config_sample, ff)

        params = {"config_path": self.tmp_config_path, "dev_mode": False}
        params.update(self.base_config)

        client = EloquaClient(**params)
        client.get_access_token()
        with open(self.tmp_config_path, "r") as config_file:
            config = json.load(config_file)
            self.assertEqual(True, "expires_in" in config)
            self.assertEqual(True, "access_token" in config)
            self.assertEqual(True, "refresh_token" in config)

    def test_devmode_accesstoken_absent(self, *args, **kwargs):
        """
        checks exception if access token is not present in config
        """
        config_sample = self.base_config

        with open(self.tmp_config_path, "w") as config_file:
            json.dump(config_sample, config_file)

        params = {"config_path": self.tmp_config_path, "dev_mode": True}
        params.update(self.base_config)
        try:
            client = EloquaClient(**params)
            client.get_access_token()
        except Exception as _:
            self.assertEqual(str(_), "Unable to locate key in config")

    @patch("requests.Session.post", side_effect=mocked_auth_post)
    def test_client_valid_token(self, mocked_auth_post):
        """checks if the client can fetch and validate refresh token and expiry from config"""

        self.tmp_config_path = "/tmp/eloqua_config.json"
        config_sample = {
            "access_token": "hellothere",
            "expires_in": strftime(datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) + datetime.timedelta(minutes=5))}
        config_sample.update(self.base_config)
        with open(self.tmp_config_path, "w") as config_file:
            json.dump(config_sample, config_file)
        params = {"config_path": self.tmp_config_path, "dev_mode": True}
        params.update(self.base_config)
        EloquaClient(**params).get_access_token()


    @patch("requests.Session.post", side_effect=mocked_auth_post)
    def test_client_invalid_token(self, mocked_auth_post):
        """checks if the client can fetch and validate refresh token and expiry from config"""

        config_sample = {
            "access_token": "hellothere",
            "expires_in": strftime(datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) - datetime.timedelta(minutes=5))}
        config_sample.update(self.base_config)
        with open(self.tmp_config_path, "w") as config_file:
            json.dump(config_sample, config_file)
        params = {"config_path": self.tmp_config_path, "dev_mode": True}
        params.update(self.base_config)
        try:
            EloquaClient(**params).get_access_token()
        except Exception as _:
            self.assertEqual(
                str(_), "Access Token in config is expired, unable to authenticate in dev mode")