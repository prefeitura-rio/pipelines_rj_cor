# -*- coding: utf-8 -*-
# flake8: noqa E501

"""
Tasks for setting rain data in Redis.
"""
# from time import sleep
from typing import Dict, List, Union

import basedosdados as bd  # pylint: disable=E0611, E0401
import pandas as pd
from basedosdados import Base  # pylint: disable=E0611, E0401

# from google.cloud import bigquery  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

from pipelines.utils_rj_cor import get_redis_client_from_infisical


@task(checkpoint=False)
def get_data(query: str) -> pd.DataFrame:
    """
    Load rain data from BigQuery
    """

    log(f"Querying data from BigQuery: {query}")

    mode = "prod"
    billing_project_id: str = None
    try:
        bd_base = Base()
        billing_project_id = bd_base.config["gcloud-projects"][mode]["name"]
    except KeyError:
        pass
    if not billing_project_id:
        raise ValueError(
            "billing_project_id must be either provided or inferred from environment variables"
        )
    log(f"Billing project ID: {billing_project_id}")

    # Load data
    log("Loading data from BigQuery...")
    dataframe = bd.read_sql(query=query, billing_project_id=billing_project_id, from_file=True)

    # # Get billing project ID
    # log("Inferring billing project ID from environment.")
    # billing_project_id = "rj-cor"
    # # job = client["bigquery"].query(query, job_config=job_config)
    # # https://github.com/prefeitura-rio/pipelines_rj_iplanrio/blob/ecd21c727b6f99346ef84575608e560e5825dd38/pipelines/painel_obras/dump_data/tasks.py#L39

    # bq_client = bigquery.Client(
    #     credentials=Base(bucket_name="rj-cor")._load_credentials(mode="prod"),
    #     project=billing_project_id,
    # )
    # job = bq_client.query(query)
    # while not job.done():
    #     sleep(1)
    # log("Getting result from query")
    # results = job.result()
    # log("Converting result to pandas dataframe")
    # dataframe = results.to_dataframe()
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
) -> None:
    """
    Set Redis key
    """
    log("Setting Redis key...")
    redis_client = get_redis_client_from_infisical(
        infisical_password_env=None, infisical_secrets_path="/redis_api_dados_rio"
    )
    redis_client.set(key, value)
    log("Redis key set successfully.")
    log(f"key: {key} and value: {value}")
