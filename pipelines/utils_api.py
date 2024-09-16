# -*- coding: utf-8 -*-
"""
Utils file
"""

# from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta
from typing import Callable, Dict, Tuple  # , List
import requests
from prefeitura_rio.pipelines_utils.logging import log


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
    ) -> None:
        if username is None or password is None:
            raise ValueError("Must be set refresh token or username with password")

        self._base_url = base_url
        self._username = username
        self._password = password
        self._header_type = header_type
        self._token_callback = token_callback
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
        )
        log(f"Status code: {response.status_code}\nResponse:{response.content}")
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

        return self._headers["Authorization"].split(" ")[1]

    def expires_at(self):
        """
        expire
        """
        return self._expires_at

    def get(self, path: str, timeout: int = 120) -> Dict:
        """
        get
        """
        self._refresh_token_if_needed()
        response = requests.get(f"{self._base_url}{path}", headers=self._headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def put(self, path, json_data=None):
        """
        put
        """
        self._refresh_token_if_needed()
        response = requests.put(f"{self._base_url}{path}", headers=self._headers, json=json_data)
        return response

    def post(self, path, data: dict = None, json_data: dict = None, files: dict = None):
        """
        post
        """
        self._refresh_token_if_needed()
        response = requests.post(
            url=f"{self._base_url}{path}",
            headers=self._headers,
            data=data,
            json=json_data,
            files=files,
        )
        # response = requests.post(f"{self._base_url}{path}", headers=self._headers, json=json_data)
        return response
