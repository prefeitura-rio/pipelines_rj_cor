# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,R0913
"""
Common  Tasks for rj-cor
"""

from prefect import task
from prefect.triggers import all_successful
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.utils_rj_cor import build_redis_key

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
def get_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    wait=None,
) -> list:
    """
    Get filenames saved on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files_on_redis = redis_client.get(key)
    files_on_redis = [] if files_on_redis is None else files_on_redis
    files_on_redis = list(set(files_on_redis))
    files_on_redis.sort()
    return files_on_redis


@task(trigger=all_successful)
def save_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    files: list = [],
    keep_last: int = 50,
    wait=None,
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files = list(set(files))
    print(">>>> save on redis files ", files)
    files.sort()
    files = files[-keep_last:]
    redis_client.set(key, files)
