# -*- coding: utf-8 -*-
"""
Tasks for setting rain data in Redis.
"""
from time import sleep
from typing import Dict, List, Union

import pandas as pd
from basedosdados import Base # pylint: disable=E0611, E0401
from google.cloud import bigquery  # pylint: disable=E0611, E0401
from prefect import task # pylint: disable=E0611, E0401

from pipelines.utils.utils import get_redis_client, log


@task(checkpoint=False)
def get_data(query: str) -> pd.DataFrame:
    """
    Load rain data from BigQuery
    """

    # Get billing project ID
    log("Inferring billing project ID from environment.")
    billing_project_id = "rj-cor"
    log("Querying data from BigQuery")
    # job = client["bigquery"].query(query, job_config=job_config)
    # https://github.com/prefeitura-rio/pipelines_rj_iplanrio/blob/ecd21c727b6f99346ef84575608e560e5825dd38/pipelines/painel_obras/dump_data/tasks.py#L39

    bq_client = bigquery.Client(
        credentials=Base(bucket_name="rj-cor")._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dataframe = results.to_dataframe()
    log("End download data from bigquery")

    # Type assertions
    if "chuva_15min" in dataframe.columns:
        dataframe["chuva_15min"] = dataframe["chuva_15min"].astype("float64")
    else:
        log(
            'Column "chuva_15min" not found in dataframe, skipping type assertion.',
            "warning",
        )

    log("Data loaded successfully.")

    return dataframe


@task(checkpoint=False)
def dataframe_to_dict(dataframe: pd.DataFrame) -> List[Dict[str, Union[str, float]]]:
    """
    Convert dataframe to dictionary
    """
    log("Converting dataframe to dictionary...")
    return dataframe.to_dict(orient="records")


@task(checkpoint=False)
def set_redis_key(
    key: str,
    value: List[Dict[str, Union[str, float]]],
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
) -> None:
    """
    Set Redis key
    """
    log("Setting Redis key...")
    redis_client = get_redis_client(host=host, port=port, db=db)
    redis_client.set(key, value)
    log("Redis key set successfully.")
    log(f"key: {key} and value: {value}")
