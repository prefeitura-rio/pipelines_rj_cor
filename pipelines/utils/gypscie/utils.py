# -*- coding: utf-8 -*-
"""
Utils file
"""

# from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta
from time import sleep
from typing import Callable, Dict, Tuple  # , List

import basedosdados as bd
import requests
from prefeitura_rio.pipelines_utils.logging import log


class GypscieApi:
    """
    GypscieApi
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        base_url: str = None,
        token_callback: Callable[[str, datetime], None] = lambda *_: None,
    ) -> None:
        if username is None or password is None:
            raise ValueError("Must be set refresh token or username with password")

        self._base_url = base_url or "https://gypscie.dados.rio/api/"
        self._username = username
        self._password = password
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
        if response.status_code == 200:
            token = response.json()["token"]
            # now + expires_in_seconds - 10 minutes
            expires_at = datetime.now() + timedelta(seconds=30 * 60)
        else:
            log(f"Status code: {response.status_code}\nResponse:{response.content}")
            raise Exception()

        return {"Authorization": f"Bearer {token}"}, token, expires_at

    def _refresh_token_if_needed(self) -> None:
        if self._expires_at <= datetime.now():
            self._headers, self._token, self._expires_at = self._get_headers()
            self._token_callback(self.get_token(), self.expires_at())

    def refresh_token(self):
        """
        refresh
        """
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

    def put(self, path, json=None):
        """
        put
        """
        self._refresh_token_if_needed()
        response = requests.put(f"{self._base_url}{path}", headers=self._headers, json=json)
        return response

    def post(self, path, data: dict = None, json: dict = None, files: dict = None):
        """
        post
        """
        self._refresh_token_if_needed()
        response = requests.post(
            url=f"{self._base_url}{path}",
            headers=self._headers,
            data=data,
            json=json,
            files=files,
        )
        # response = requests.post(f"{self._base_url}{path}", headers=self._headers, json=json)
        return response


def bq_project(kind: str = "bigquery_prod"):
    """Get the set BigQuery project_id

    Args:
        kind (str, optional): Which client to get the project name from.
        Options are 'bigquery_staging', 'bigquery_prod' and 'storage_staging'
        Defaults to 'bigquery_prod'.

    Returns:
        str: the requested project_id
    """
    return bd.upload.base.Base().client[kind].project


def wait_run(api, task_response, flow_type: str = "dataflow") -> Dict:
    """
    Force flow wait for the end of data processing
    flow_type: dataflow or processor
    Return:
    {
        "result": {},
        "state": "string",
        "status": "string"
    }
    """
    if "task_id" in task_response.keys():
        _id = task_response.get("task_id")
    else:
        log(f"Error processing: task_id not found on response:{task_response}")
        # TODO: stop flow here

    # Request to get the execution status
    path_flow_type = "status_workflow_run" if flow_type == "dataflow" else "status_processor_run"
    response = api.get(
        path=f"{path_flow_type}/" + _id,
    )

    log(f"Execution status: {response}.")
    while response["state"] == "STARTED":
        sleep(5)
        response = wait_run(api, task_response)

    if response["state"] != "SUCCESS":
        log("Error processing this dataset. Stop flow or restart this task")

    return response
