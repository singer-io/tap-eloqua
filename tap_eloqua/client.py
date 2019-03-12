import json
from datetime import datetime, timedelta

import backoff
import requests
from requests.exceptions import ConnectionError
from singer import metrics

class Server5xxError(Exception):
    pass

class EloquaClient(object):
    def __init__(self,
                 config_path,
                 client_id,
                 client_secret,
                 refresh_token,
                 redirect_uri,
                 user_agent):
        self.__config_path = config_path
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__redirect_uri = redirect_uri
        self.__user_agent = user_agent
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.__base_url = None

    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_access_token(self):
        if self.__access_token is not None and self.__expires > datetime.utcnow():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            'https://login.eloqua.com/auth/oauth2/token',
            auth=(self.__client_id, self.__client_secret),
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'refresh_token': self.__refresh_token,
                'redirect_uri': self.__redirect_uri,
                'scope': 'full'
            })

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise Exception('Unable to authenticate')

        data = response.json()

        self.__access_token = data['access_token']
        self.__refresh_token = data['refresh_token']

        ## refresh_token rotates on every reauth
        with open(self.__config_path) as file:
            config = json.load(file)
        config['refresh_token'] = data['refresh_token']
        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

        expires_seconds = data['expires_in'] - 10 # pad by 10 seconds
        self.__expires = datetime.utcnow() + timedelta(seconds=expires_seconds)

    def get_base_urls(self):
        data = self.request('GET',
                            url='https://login.eloqua.com/id',
                            endpoint='base_url')
        self.__base_url = data['urls']['base']

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError),
                          max_tries=5,
                          factor=2)
    def request(self, method, path=None, url=None, **kwargs):
        self.get_access_token()

        if not url and self.__base_url is None:
            self.get_base_urls()

        if not url and path:
            url = self.__base_url + path

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
