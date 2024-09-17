# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,R0913
"""
Common  Tasks for rj-cor
"""

import json

import pandas as pd
from prefect import task
from prefect.triggers import all_successful
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.utils.utils import log
from pipelines.utils_rj_cor import treat_redis_output

# from pipelines.utils.utils import get_redis_client
# from redis_pal import RedisPal


@task(checkpoint=False)
def task_get_redis_client(
    infisical_host_env: str = "REDIS_HOST",
    infisical_port_env: str = "REDIS_PORT",
    infisical_db_env: str = "REDIS_DB",
    infisical_password_env: str = "REDIS_PASSWORD",
    infisical_secrets_path: str = "/",
):
    """
    Gets a Redis client.

    Args:
        infisical_host_env: The environment variable for the Redis host.
        infisical_port_env: The environment variable for the Redis port.
        infisical_db_env: The environment variable for the Redis database.
        infisical_password_env: The environment variable for the Redis password.

    Returns:
        The Redis client.
    """
    redis_host = get_secret(infisical_host_env, path=infisical_secrets_path)[infisical_host_env]
    redis_port = int(
        get_secret(infisical_port_env, path=infisical_secrets_path)[infisical_port_env]
    )
    redis_db = int(get_secret(infisical_db_env, path=infisical_secrets_path)[infisical_db_env])
    redis_password = get_secret(infisical_password_env, path=infisical_secrets_path)[
        infisical_password_env
    ]
    return get_redis_client(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
    )


@task
def task_build_redis_hash(dataset_id: str, table_id: str, name: str = None, mode: str = "prod"):
    """
    Helper function for building a key to redis
    """
    redis_hash = dataset_id + "." + table_id
    if name:
        redis_hash = redis_hash + "." + name
    if mode == "dev":
        redis_hash = f"{mode}.{redis_hash}"
    return redis_hash


# @task
# def get_on_redis(
#     dataset_id: str,
#     table_id: str,
#     mode: str = "prod",
#     wait=None,
# ) -> list:
#     """
#     Get filenames saved on Redis.
#     converti em task_get_redis_output
#     """
#     redis_client = get_redis_client()
#     key = build_redis_key(dataset_id, table_id, "files", mode)
#     files_on_redis = redis_client.get(key)
#     files_on_redis = [] if files_on_redis is None else files_on_redis
#     files_on_redis = list(set(files_on_redis))
#     files_on_redis.sort()
#     return files_on_redis


@task
def task_get_redis_output(
    redis_client,
    redis_hash: str = None,
    key: str = None,
    treat_output: bool = True,
    is_df: bool = False,
):
    """
    Get Redis output. Use get to obtain a df from redis or hgetall if is a key value pair.
    Redis output example: {b'date': b'2023-02-27 07:29:04'}
    """

    if is_df:
        json_data = redis_client.get(redis_hash)
        log(f"[DEGUB] json_data {json_data}")
        if json_data:
            # If data is found, parse the JSON string back to a Python object (dictionary)
            data_dict = json.loads(json_data)
            # Convert the dictionary back to a DataFrame
            return pd.DataFrame(data_dict)

        return pd.DataFrame()

    if redis_hash and key:
        output = redis_client.hget(redis_hash, key)
    elif key:
        output = redis_client.get(key)
        output = [] if output is None else output
        output = list(set(output))
        output.sort()
    else:
        output = redis_client.hgetall(redis_hash)
    if len(output) > 0 and treat_output:
        output = treat_redis_output(output)
    log(f"Output from redis {type(output)}\n{output}")
    return output


@task(trigger=all_successful)
def task_save_list_on_redis(
    redis_client,
    redis_hash: str = None,
    redis_key: str = None,
    files: list = [],
    keep_last: int = 50,
    wait=None,
) -> None:
    """
    Set the last updated time on Redis.
    """
    if isinstance(files, list):
        files = list(set(files))
        files.sort()
        files = files[-keep_last:]
    log(f"Saving files {files} on redis {hash} {redis_key}")
    # TODO: adicinar quando tiver hash tb
    redis_client.set(redis_key, files)
