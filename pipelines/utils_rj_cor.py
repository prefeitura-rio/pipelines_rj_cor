# -*- coding: utf-8 -*-
"""
Utils for rj-cor
"""
import json
from os import getenv
from time import sleep
from typing import Callable, Dict

import basedosdados as bd
import pandas as pd
import pendulum
from google.cloud import storage
from loguru import logger

# from redis_pal import RedisPal
# import pipelines.constants
from prefeitura_rio.pipelines_utils.infisical import (
    get_secret,  # pylint: disable=E0611, E0401
)
from prefeitura_rio.pipelines_utils.redis_pal import (  # pylint: disable=E0611, E0401
    get_redis_client,
)

from pipelines.utils.utils import log

###############
#
# Redis
#
###############


def getenv_or_action(key: str, action: Callable[[str], None], default: str = None) -> str:
    """Get env or action"""
    value = getenv(key)
    if value is None:
        value = action(key)
    if value is None:
        value = default
    return value


def ignore(key: str) -> None:
    """Ignore"""
    log(f"Ignore key {key}")


def warn(key: str) -> None:
    """Log a warn"""
    logger.warning(f"WARNING: Environment variable {key} is not set.")


def raise_error(key: str) -> None:
    """Raise error"""
    raise ValueError(f"Environment variable {key} is not set.")


# def get_redis_client(
#     host: str = "redis.redis.svc.cluster.local",
#     port: int = 6379,
#     db: int = 0,  # pylint: disable=C0103
#     password: str = None,
# ) -> RedisPal:
#     """
#     Returns a Redis client.
#     """
#     return RedisPal(
#         host=host,
#         port=port,
#         db=db,
#         password=password,
#     )


def get_redis_client_from_infisical(
    infisical_host_env: str = "REDIS_HOST",
    infisical_port_env: str = "REDIS_PORT",
    infisical_db_env: str = "REDIS_DB",
    infisical_password_env: str = "REDIS_PASSWORD",
    infisical_secrets_path: str = "/redis",
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
    log(f"Acessing host: {redis_host}")
    return get_redis_client(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
    )


def build_redis_key(dataset_id: str, table_id: str, name: str = None, mode: str = "prod"):
    """
    Helper function for building a key to redis
    """
    key = dataset_id + "." + table_id
    if name:
        key = key + "." + name
    if mode == "dev":
        key = f"{mode}.{key}"
    return key


def save_str_on_redis(
    redis_hash: str,
    key: str,
    value: str,
):
    """
    Function to save a string on redis
    """

    redis_client = get_redis_client_from_infisical()
    redis_client.hset(redis_hash, key, value)


def treat_redis_output(text):
    """
    Redis returns a dict where both key and value are byte string
    Example: {b'date': b'2023-02-27 07:29:04'}
    """
    if isinstance(list(text.keys())[0], bytes):
        return {k.decode("utf-8"): v for k, v in text.items()}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in text.items()}


def compare_dates_between_tables_redis(
    key_table_1: str,
    format_date_table_1: str,
    key_table_2: str,
    format_date_table_2: str,
):
    """
    Function that checks if the date saved on the second
    table is bigger then the first one
    """

    # get saved date on redis
    date_1 = get_redis_output(key_table_1)
    date_2 = get_redis_output(key_table_2)

    # Return true if there is no date_1 or date_2 saved on redis
    if (len(date_1) == 0) | (len(date_2) == 0):
        return True

    # Convert date to pendulum
    date_1 = pendulum.from_format(date_1["date"], format_date_table_1)
    date_2 = pendulum.from_format(date_2["date"], format_date_table_2)
    comparison = date_1 < date_2
    log(f"Is {date_2} bigger than {date_1}? {comparison}")
    return comparison


# pylint: disable=W0106
def save_updated_rows_on_redis(  # pylint: disable=R0914, R0913
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    unique_id: str = "id_estacao",
    date_column: str = "data_medicao",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    mode: str = "prod",
) -> pd.DataFrame:
    """
    Acess redis to get the last time each unique_id was updated, return
    updated unique_id as a DataFrame and save new dates on redis
    """

    redis_client = get_redis_client_from_infisical()

    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"

    # Access all data saved on redis with this key
    last_updates = redis_client.hgetall(key)

    if len(last_updates) == 0:
        last_updates = pd.DataFrame(dataframe[unique_id].unique(), columns=[unique_id])
        last_updates["last_update"] = "1900-01-01 00:00:00"
        log(f"Redis key: {key}\nCreating Redis fake values:\n {last_updates}")
    else:
        # Convert data in dictionary in format with unique_id in key and last updated time as value
        # Example > {"12": "2022-06-06 14:45:00"}
        last_updates = {k.decode("utf-8"): v.decode("utf-8") for k, v in last_updates.items()}

        # Convert dictionary to dataframe
        last_updates = pd.DataFrame(last_updates.items(), columns=[unique_id, "last_update"])

        log(f"Redis key: {key}\nRedis actual values:\n {last_updates}")

    # Garante that both are string
    dataframe[unique_id] = dataframe[unique_id].astype(str)
    last_updates[unique_id] = last_updates[unique_id].astype(str)

    # dataframe and last_updates need to have the same index, in our case unique_id
    missing_in_dfr = [
        i for i in last_updates[unique_id].unique() if i not in dataframe[unique_id].unique()
    ]
    missing_in_updates = [
        i for i in dataframe[unique_id].unique() if i not in last_updates[unique_id].unique()
    ]

    # If unique_id doesn't exists on updates we create a fake date for this station on updates
    if len(missing_in_updates) > 0:
        for i, _id in enumerate(missing_in_updates):
            last_updates.loc[-i] = [_id, "1900-01-01 00:00:00"]

    # If unique_id doesn't exists on dataframe we remove this stations from last_updates
    if len(missing_in_dfr) > 0:
        last_updates = last_updates[~last_updates[unique_id].isin(missing_in_dfr)]

    # Merge dfs using unique_id
    dataframe = dataframe.merge(last_updates, how="left", on=unique_id)
    log(f"Comparing times: {dataframe.sort_values(unique_id)}")

    # Keep on dataframe only the stations that has a time after the one that is saved on redis
    dataframe[date_column] = dataframe[date_column].apply(
        pd.to_datetime, format=date_format
    ) + pd.DateOffset(hours=0)

    dataframe["last_update"] = dataframe["last_update"].apply(
        pd.to_datetime, format="%Y-%m-%d %H:%M:%S"
    ) + pd.DateOffset(hours=0)

    dataframe = dataframe[dataframe[date_column] > dataframe["last_update"]].dropna(
        subset=[unique_id]
    )
    log(f"Dataframe after comparison: {dataframe.sort_values(unique_id)}")
    # Keep only the last date for each unique_id
    keep_cols = [unique_id, date_column]
    new_updates = dataframe[keep_cols].sort_values(keep_cols)
    new_updates = new_updates.groupby(unique_id, as_index=False).tail(1)
    new_updates[date_column] = new_updates[date_column].dt.strftime("%Y-%m-%d %H:%M:%S")
    log(f">>> Updated df: {new_updates.head(10)}")

    # Convert stations with the new updates dates in a dictionary
    new_updates = dict(zip(new_updates[unique_id], new_updates[date_column]))
    log(f">>> data to save in redis as a dict: {new_updates}")

    # Save this new information on redis
    [redis_client.hset(key, k, v) for k, v in new_updates.items()]

    return dataframe.reset_index()


def get_redis_output(
    redis_hash: str = None, key: str = None, treat_output: bool = True, is_df: bool = False
):
    """
    Get Redis output. Use get to obtain a df from redis or hgetall if is a key value pair.
    Redis output example: {b'date': b'2023-02-27 07:29:04'}
    """
    # TODO: validar mudanças em outras pipes
    redis_client = get_redis_client_from_infisical()  # (host="127.0.0.1")

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
    return output


def compare_actual_df_with_redis_df(
    dfr: pd.DataFrame,
    dfr_redis: pd.DataFrame,
    columns: list,
) -> pd.DataFrame:
    """
    Compare df from redis to actual df and return only the rows from actual df
    that are not already saved on redis.
    """
    for col in columns:
        if col not in dfr_redis.columns:
            dfr_redis[col] = None
        dfr_redis[col] = dfr_redis[col].astype(dfr[col].dtypes)
    log(f"\nEnded conversion types from dfr to dfr_redis: \n{dfr_redis.dtypes}")

    dfr_diff = (
        pd.merge(dfr, dfr_redis, how="left", on=columns, indicator=True)
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )
    log(f"\nDf resulted from the difference between dft_redis and dfr: \n{dfr_diff.head()}")

    updated_dfr_redis = pd.concat([dfr_redis, dfr_diff[columns]])

    return dfr_diff, updated_dfr_redis


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


def wait_task_run(api, task_id) -> Dict:
    """
    Force flow wait for the end of data processing
    """
    if "task_id" in task_id.keys():
        _id = task_id.get("task_id")

        # Requisição do resultado da task_id
        response = api.get(
            path="status_processor_run/" + _id,
        )

    print(f"Response state: {response['state']}")
    while response["state"] == "STARTED":
        sleep(5)
        response = wait_task_run(api, task_id)

    if response["state"] != "SUCCESS":
        print("Error processing this dataset. Stop flow or restart this task")

    return response


###############
#
# Storage
#
###############


def list_files_storage(bucket, prefix: str, sort_key: str = None) -> list:
    """List files from bucket"""
    blobs = list(bucket.list_blobs(prefix=prefix))
    files = [blob.name for blob in blobs if blob.name.endswith(".h5")]
    sorted_files = sorted(files, key=sort_key)
    return sorted_files


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    return blobs


def list_all_directories(bucket, bucket_name, prefix=""):
    """List all directories in a Google Cloud Storage bucket recursively."""
    # client = storage.Client()
    # bucket = client.get_bucket(bucket_name)

    directories = set()
    blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

    for blob in blobs:
        log(f"Blobs inside directorie: {blob}")  # Os blobs são arquivos.

    directories.update(blobs.prefixes)

    for sub_prefix in blobs.prefixes:
        directories.update(list_all_directories(bucket, bucket_name, sub_prefix))
    return sorted(directories)


def download_blob(bucket_name, source_blob_name, destination_file_name) -> None:
    """
    Downloads a blob mencioned on source_blob_name from bucket_name
    and save it on destination_file_name.
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to file path {destination_file_name}. successfully ")
