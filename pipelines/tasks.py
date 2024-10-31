# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,R0913
"""
Common  Tasks for rj-cor
"""

import json
from pathlib import Path
from typing import List, Union

import pandas as pd
import pendulum  # pylint: disable=E0611, E0401
from google.cloud import storage  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from prefect.triggers import all_successful  # pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.infisical import (
    get_secret,  # pylint: disable=E0611, E0401
)
from prefeitura_rio.pipelines_utils.pandas import (  # pylint: disable=E0611, E0401
    parse_date_columns,
    to_partitions,
)
from prefeitura_rio.pipelines_utils.redis_pal import (  # pylint: disable=E0611, E0401
    get_redis_client,
)

from pipelines.utils.utils import build_redis_key, log
from pipelines.utils_rj_cor import treat_redis_output

# from redis_pal import RedisPal


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


@task
def get_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    wait=None,
) -> list:
    """
    Get filenames saved on Redis.
    converti em task_get_redis_output
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files_on_redis = redis_client.get(key)
    files_on_redis = [] if files_on_redis is None else files_on_redis
    files_on_redis = list(set(files_on_redis))
    files_on_redis.sort()
    return files_on_redis


@task
def task_get_redis_output(
    redis_client,
    redis_hash: str = None,
    redis_key: str = None,
    treat_output: bool = True,
    expected_output_type: str = "list",
    is_df: bool = False,
):
    """
    Get Redis output. Use get to obtain a df from redis or hgetall if is a key value pair.
    Redis output example: {b'date': b'2023-02-27 07:29:04'}
    expected_output_type "list"m "dict", "df"
    """

    if is_df or expected_output_type == "df":
        json_data = redis_client.get(redis_hash)
        log(f"[DEGUB] json_data {json_data}")
        if json_data:
            # If data is found, parse the JSON string back to a Python object (dictionary)
            data_dict = json.loads(json_data)
            # Convert the dictionary back to a DataFrame
            return pd.DataFrame(data_dict)

        return pd.DataFrame()

    if redis_hash and redis_key:
        output = redis_client.hget(redis_hash, redis_key)
    elif redis_key:
        output = redis_client.get(redis_key)
    else:
        output = redis_client.hgetall(redis_hash)

    if not output:
        output = [] if expected_output_type == "list" else {}

    log(f"Output from redis before treatment{type(output)}\n{output}")
    if len(output) > 0 and treat_output and not isinstance(output, list):
        output = treat_redis_output(output)
    log(f"Output from redis {type(output)}\n{output}")
    return output


# @task(trigger=all_successful)
# def save_on_redis(
#     dataset_id: str,
#     table_id: str,
#     mode: str = "prod",
#     files: list = [],
#     keep_last: int = 50,
#     wait=None,
# ) -> None:
#     """
#     Set the last updated time on Redis.
#     """
#     redis_client = get_redis_client_from_infisical()
#     key = build_redis_key(dataset_id, table_id, "files", mode)
#     files = list(set(files))
#     print(">>>> save on redis files ", files)
#     files.sort()
#     files = files[-keep_last:]
#     redis_client.set(key, files)


@task(trigger=all_successful)
def task_save_on_redis(
    redis_client,
    values,
    redis_hash: str = None,
    redis_key: str = None,
    keep_last: int = 50,
    wait=None,
) -> None:
    """
    Save values on redis. If values are a list, order ir and keep only last names.
    """

    if isinstance(values, list):
        values = list(set(values))
        values.sort()
        values = values[-keep_last:]

    if isinstance(values, dict):
        values = json.dumps(values)

    log(f"Saving files {values} on redis {redis_hash} {redis_key}")

    if redis_hash and redis_key:
        redis_client.hset(redis_hash, redis_key, values)
    elif redis_key:
        redis_client.set(redis_key, values)
    log(f"Saved to Redis hash: {redis_hash}, key: {redis_key}, value: {values}")


@task
def get_storage_destination(path: str, filename: str = None) -> str:
    """
    Get storage blob destinationa and the name of the source file
    """
    destination_blob_name = f"{path}/{filename}" if filename else path
    log(f"File destination_blob_name {destination_blob_name}")
    return destination_blob_name


# @task
# def upload_files_to_storage(
#     project: str, bucket_name: str, destination_blob_name: str, source_file_name: List[str]
# ) -> None:
#     """
#     Upload files to GCS

#     project="datario"
#     bucket_name="datario-public"
#     destination_blob_name=f"cor-clima-imagens/radar/mendanha/{filename}.png"
#     source_file_name=f"{path}/{filename}.png"
#     """
#     storage_client = storage.Client(project=project)
#     bucket = storage_client.bucket(bucket_name)
#     # Cria um blob (o arquivo dentro do bucket)
#     blob = bucket.blob(destination_blob_name)
#     for i in source_file_name:
#         blob.upload_from_filename(i)
#         log(f"File {i} sent to {destination_blob_name} on bucket {bucket_name}.")


@task
def upload_files_to_storage(
    project: str, bucket_name: str, destination_folder: str, source_file_names: List[str]
) -> None:
    """
    Upload multiple files to GCS, where the destination folder is the directory in the bucket
    and source_file_names is a list of file paths to upload.

    project="datario"
    bucket_name="datario-public"
    destination_folder="cor-clima-imagens/radar/mendanha/"
    source_file_names=["/local/path/image1.png", "/local/path/image2.png"]
    """
    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)

    log(f"Uploading {len(source_file_names)} files to {destination_folder}.")
    for file_path in source_file_names:
        file_name = file_path.split("/")[-1]
        blob = bucket.blob(f"{destination_folder}/{file_name}")
        blob.upload_from_filename(file_path)

        log(f"File {file_name} sent to {destination_folder} on bucket {bucket_name}.")


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


@task
def task_create_partitions(
    data: pd.DataFrame,
    partition_date_column: str,
    # partition_columns: List[str],
    savepath: str = "temp",
    data_type: str = "csv",
    suffix: str = None,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
    wait=None,  # pylint: disable=unused-argument
) -> Path:  # sourcery skip: raise-specific-error
    """
    Create task for to_partitions
    """
    log(f"Data before partition columns creation {data.iloc[0]}")
    data, partition_columns = parse_date_columns(data, partition_date_column)
    log(f"Created partition columns {partition_columns} and data first row now is {data.iloc[0]}")
    saved_files = to_partitions(
        data=data,
        partition_columns=partition_columns,
        savepath=savepath,
        data_type=data_type,
        suffix=suffix,
        build_json_dataframe=build_json_dataframe,
        dataframe_key_column=dataframe_key_column,
    )
    log(f"Partition saved files {saved_files}")
    log(f"Returned path {savepath}, {type(savepath)}")
    return Path(savepath)
