# -*- coding: utf-8 -*-
"""
Utils file
"""

# from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta
from typing import Callable, Dict, Tuple  # , List

import requests
import simplejson
from prefeitura_rio.pipelines_utils.logging import log


# pylint: disable=too-many-arguments, too-many-instance-attributes
class Api:
    """
    Api
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        base_url: str = None,
        header_type: str = None,
        token_callback: Callable[[str, datetime], None] = lambda *_: None,
        request_timeout: int = 180,
    ) -> None:
        if username is None or password is None:
            raise ValueError("Must be set refresh token or username with password")

        self._base_url = base_url
        self._username = username
        self._password = password
        self._header_type = header_type
        self._token_callback = token_callback
        self._request_timeout = request_timeout
        self._headers, self._token, self._expires_at = self._get_headers()

    def _get_headers(self) -> Tuple[Dict[str, str], str, datetime]:

        response = requests.post(
            f"{self._base_url}login",
            headers={"accept": "application/json", "Content-Type": "application/json"},
            json={
                # 'grant_type': 'password',
                # 'scope': 'openid profile',
                "username": self._username,
                "password": self._password,
            },
            timeout=self._request_timeout,
        )
        log(f"Status code: {response.status_code}\nResponse: {response.content}")
        if response.status_code == 200:
            response_json = response.json()
            token_word = [i for i in response_json.keys() if "token" in i.lower()][0]
            token = response_json[token_word]
            # now + expires_in_seconds - 10 minutes
            expires_word = [i for i in response_json.keys() if "expires" in i.lower()]
            expires_at = (
                datetime.now() + timedelta(seconds=30 * 60)
                if len(expires_word) == 0
                else datetime.now() + timedelta(seconds=int(response_json[expires_word[0]]))
            )
            log(f"Token {token[:10]} expires at {expires_at}")
        else:
            log(f"Status code: {response.status_code}\nResponse:{response.content}")
            raise Exception()

        if self._header_type == "token":
            return {"token": f"{token}"}, token, expires_at
        return {"Authorization": f"Bearer {token}"}, token, expires_at

    def _refresh_token_if_needed(self) -> None:
        if self._expires_at <= datetime.now():
            self._headers, self._token, self._expires_at = self._get_headers()
            self._token_callback(self.get_token(), self.expires_at())

    def refresh_token(self):
        """
        refresh
        """
        self._expires_at = datetime.now()
        self._refresh_token_if_needed()

    def get_token(self):
        """
        get token
        """
        self._refresh_token_if_needed()
        if "Authorization" in self._headers.keys():
            return self._headers["Authorization"].split(" ")[1]
        return self._headers["token"]

    def expires_at(self):
        """
        expire
        """
        return self._expires_at

    def _request(self, method: str, *args, **kwargs) -> requests.Response:
        """
        request
        """
        self._refresh_token_if_needed()
        try:
            fn = getattr(requests, method)
        except AttributeError:
            raise ValueError(f"Method {method} not found in requests module")  # noqa
        response = fn(*args, headers=self._headers, timeout=self._request_timeout, **kwargs)
        return response

    def get(self, path: str, timeout: int = None) -> Dict:
        """
        get
        """
        timeout = timeout or self._request_timeout
        response = self._request("get", f"{self._base_url}{path}", timeout=timeout)
        response.raise_for_status()
        try:
            return response.json()
        except simplejson.JSONDecodeError:
            return response

    def put(self, path, json=None):
        """
        put
        """
        response = self._request("put", f"{self._base_url}{path}", json=json)
        return response

    def post(self, path, data: dict = None, json: dict = None, files: dict = None):
        """
        post
        """
        response = self._request(
            "post",
            url=f"{self._base_url}{path}",
            data=data,
            json=json,
            files=files,
        )
        return response
