# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,R0913
"""
Common  Tasks for rj-cor
"""

from pathlib import Path
from typing import List, Union, Tuple
from google.cloud import storage
import pandas as pd
import pendulum
from prefect import task
from prefect.triggers import all_successful

from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.pandas import (  # pylint: disable=E0611, E0401
    parse_date_columns,
    to_partitions,
)
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client  # pylint: disable=E0611, E0401

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
) -> List:
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


@task(nout=2)
def get_storage_destination(filename: str, path: str) -> Tuple[str, str]:
    """
    Get storage blob destinationa and the name of the source file
    """
    destination_blob_name = f"cor-clima-imagens/radar/mendanha/{filename}.png"
    source_file_name = f"{path}/{filename}.png"
    log(f"File destination_blob_name {destination_blob_name}")
    log(f"File source_file_name {source_file_name}")
    return destination_blob_name, source_file_name


@task
def upload_files_to_storage(
    project: str, bucket_name: str, destination_blob_name: str, source_file_name: List[str]
) -> None:
    """
    Upload files to GCS

    project="datario"
    bucket_name="datario-public"
    destination_blob_name=f"cor-clima-imagens/radar/mendanha/{filename}.png"
    source_file_name=f"{path}/{filename}.png"
    """
    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)
    # Cria um blob (o arquivo dentro do bucket)
    blob = bucket.blob(destination_blob_name)
    for i in source_file_name:
        blob.upload_from_filename(i)
        log(f"File {i} sent to {destination_blob_name} on bucket {bucket_name}.")


@task
def save_dataframe(
    dfr: pd.DataFrame,
    partition_column: str,
    suffix: str = "current_timestamp",
    path: str = "temp",
    wait=None,  # pylint: disable=unused-argument
) -> Union[str, Path]:
    """
    Salvar dfr tratados em csv para conseguir subir pro GCP
    """

    prepath = Path(path)
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dfr, partition_column)
    if suffix == "current_timestamp":
        suffix = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=suffix,
    )
    log(f"Data saved on {prepath}")
    return prepath
