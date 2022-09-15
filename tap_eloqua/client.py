from datetime import timedelta

import backoff
import requests
from requests.exceptions import ConnectionError
from singer import get_logger, metrics
from singer.utils import now, strftime, strptime_to_utc

from .utils import read_config, write_config

LOGGER = get_logger()


class Server5xxError(Exception):
    pass


class EloquaClient(object):
    def __init__(self, config_path, client_id, client_secret, refresh_token, redirect_uri, user_agent, dev_mode=False):
        self.__config_path = config_path
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__redirect_uri = redirect_uri
        self.__user_agent = user_agent
        self.dev_mode = dev_mode
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.__base_url = None

    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo, Server5xxError, max_tries=5, factor=2)
    def get_access_token(self):
        if self.dev_mode:
            config = read_config(self.__config_path)
            try:
                self.__access_token = config["access_token"]
                self.__refresh_token = config["refresh_token"]
                self.__expires = strptime_to_utc(config["expires_in"])
            except KeyError as _:
                LOGGER.fatal("Unable to locate key %s in config", _)
                raise Exception("Unable to locate key in config")
            if self.__access_token is not None and self.__expires > now():
                return
            LOGGER.fatal("Access Token in config is expired, unable to authenticate in dev mode")
            raise Exception("Access Token in config is expired, unable to authenticate in dev mode")

        if self.__access_token is not None and self.__expires > now():
            return

        headers = {}
        if self.__user_agent:
            headers["User-Agent"] = self.__user_agent

        response = self.__session.post(
            "https://login.eloqua.com/auth/oauth2/token",
            auth=(self.__client_id, self.__client_secret),
            headers=headers,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.__refresh_token,
                "redirect_uri": self.__redirect_uri,
                "scope": "full",
            },
        )

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            eloqua_response = response.json()
            eloqua_response.update({"status": response.status_code})
            raise Exception("Unable to authenticate (Eloqua response: `{}`)".format(eloqua_response))

        data = response.json()
        self.__access_token = data["access_token"]
        self.__refresh_token = data["refresh_token"]
        expires_seconds = data["expires_in"] - 10  # pad by 10 seconds
        self.__expires = now() + timedelta(seconds=expires_seconds)

        if not self.dev_mode:
            update_config_keys = {
                "refresh_token": self.__refresh_token,
                "access_token": self.__access_token,
                "expires_in": strftime(self.__expires),
            }
            config = write_config(self.__config_path, update_config_keys)

    def get_base_urls(self):
        data = self.request("GET", url="https://login.eloqua.com/id", endpoint="base_url")
        self.__base_url = data["urls"]["base"]

    @backoff.on_exception(backoff.expo, (Server5xxError, ConnectionError), max_tries=5, factor=2)
    def request(self, method, path=None, url=None, **kwargs):
        self.get_access_token()

        if not url and self.__base_url is None:
            self.get_base_urls()

        if not url and path:
            url = self.__base_url + path

        if "endpoint" in kwargs:
            endpoint = kwargs["endpoint"]
            del kwargs["endpoint"]
        else:
            endpoint = None

        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = "Bearer {}".format(self.__access_token)

        if self.__user_agent:
            kwargs["headers"]["User-Agent"] = self.__user_agent

        if method == "POST":
            kwargs["headers"]["Content-Type"] = "application/json"

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request("GET", path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path=path, **kwargs)
