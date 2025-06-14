# -*- coding: utf-8 -*-
# pylint: disable=C0103,R0914,R0913
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
from pathlib import Path
from typing import Tuple, Union

import numpy as np
import pandas as pd
import pendulum  # pylint: disable=E0401
import requests
from bs4 import BeautifulSoup  # pylint: disable=E0401
from prefect import task  # pylint: disable=E0401
from prefeitura_rio.pipelines_utils.infisical import get_secret  # pylint: disable=E0401
from prefeitura_rio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

from pipelines.constants import constants
from pipelines.meteorologia.precipitacao_alertario.utils import (
    fill_col_if_dropped_on_source,
    parse_date_columns_old_api,
    replace_future_dates,
    treat_date_col,
)
from pipelines.utils.utils import (
    build_redis_key,
    compare_dates_between_tables_redis,
    get_redis_output,
    parse_date_columns,
    save_str_on_redis,
    save_updated_rows_on_redis,
    to_partitions,
)


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data() -> pd.DataFrame:
    """
    Request data from API and return each data in a different dataframe.
    """

    url = get_secret("ALERTARIO_API")["ALERTARIO_API"]

    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Get all tables from HTML structure
            tables = soup.find_all("table")

            # Data cames in Brazillian format and has some extra newline
            tables = [str(table).replace(",", ".").replace("\n", "") for table in tables]

            # Convert HTML table to pandas dataframe
            dfr = pd.read_html(str(tables), decimal=",")
        else:
            log(f"Erro ao fazer a solicitação. Código de status: {response.status_code}")

    except requests.RequestException as e:
        log(f"Erro durante a solicitação: {e}")

    dfr_pluviometric = dfr[0]
    dfr_meteorological = dfr[1]
    # dfr_rain_conditions = dfr[2]
    # dfr_landslide_probability = dfr[3]

    log(f"\nPluviometric df {dfr_pluviometric.iloc[0]}")
    log(f"\nMeteorological df {dfr_meteorological.iloc[0]}")

    return (
        dfr_pluviometric,
        dfr_meteorological,
    )  # , dfr_rain_conditions, dfr_landslide_probability


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_pluviometer_and_meteorological_data(
    dfr: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    """

    # Treat dfr if is from pluviometers
    if isinstance(dfr.columns, pd.MultiIndex):
        # Keeping only the rightmost level of the MultiIndex columns
        dfr.columns = dfr.columns.droplevel(level=0)

        rename_cols = {
            "N°": "id_estacao",
            "Hora Leitura": "data_medicao",
            "05 min": "acumulado_chuva_5min",
            "10 min": "acumulado_chuva_10min",
            "15 min": "acumulado_chuva_15min",
            "30 min": "acumulado_chuva_30min",
            "1h": "acumulado_chuva_1h",
            "2h": "acumulado_chuva_2h",
            "3h": "acumulado_chuva_3h",
            "4h": "acumulado_chuva_4h",
            "6h": "acumulado_chuva_6h",
            "12h": "acumulado_chuva_12h",
            "24h": "acumulado_chuva_24h",
            "96h": "acumulado_chuva_96h",
            "No Mês": "acumulado_chuva_mes",
        }

    else:
        rename_cols = {
            "N°": "id_estacao",
            "Hora Leitura": "data_medicao",
            "Temp. (°C)": "temperatura",
            "Umi. do Ar (%)": "umidade_ar",
            "Índice de Calor (°C)": "sensacao_termica",
            "P. Atm. (hPa)": "pressao_atmosferica",
            "P. de Orvalho (°C)": "temperatura_orvalho",
            "Vel. do Vento (Km/h)": "velocidade_vento",
            "Dir. do Vento (°)": "direcao_vento",
        }  # confirmar nome das colunas com inmet

    dfr.rename(columns=rename_cols, inplace=True)

    keep_cols = list(rename_cols.values())

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    # dfr.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%d/%m/%Y - %H:%M:%S")
    dfr = replace_future_dates(dfr)

    log(f"Dataframe before comparing with last data saved on redis for {table_id} {dfr.iloc[0]}")

    dfr = save_updated_rows_on_redis(
        dfr,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format="%Y-%m-%d %H:%M:%S",
        mode=mode,
    )

    empty_data = dfr.shape[0] == 0

    if not empty_data:
        see_cols = ["id_estacao", "data_medicao", "last_update"]
        log(f"df after comparing with last data on redis for {table_id} {dfr[see_cols].head()}")
        log(f"Dataframe first row after comparing for {table_id} {dfr.iloc[0]}")
        dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")
        log(f"Dataframe after converting to string for {table_id} {dfr[see_cols].head()}")

        # Save max date on redis to compare this with last dbt run
        max_date = str(dfr["data_medicao"].max())
        redis_key = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)
        log(f"Dataframe is not empty. Redis key: {redis_key} and new date: {max_date}")
        save_str_on_redis(redis_key, "date", max_date)

        if not empty_data:
            # Changin values "ND" and "-" to "None"
            dfr.replace(["ND", "-"], [None, None], inplace=True)

        dfr = fill_col_if_dropped_on_source(dfr, keep_cols)
        # TODO: seria interessante ter uma função para analisar se há colunas novas
        # e gerar um alerta pois teve caso de alterarem o nome da coluna sensação térmica
        # Fix columns order
        dfr = dfr[keep_cols]
    else:
        # If df is empty stop flow on flows.py
        log(f"Dataframe for {table_id} is empty. Skipping update flow.")

    return dfr, empty_data


@task(nout=2)
def save_data(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    columns: str = None,
    treatment_version: int = None,
    data_type: str = "csv",
    suffix: bool = True,
    wait=None,  # pylint: disable=unused-argument
) -> Tuple[Union[str, Path], Union[str, Path]]:
    """
    Salvar dfr tratados em csv para conseguir subir pro GCP
    """

    prepath = Path(f"/tmp/precipitacao_alertario/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    treatment_version = str(treatment_version) + "_" if treatment_version else ""
    suffix = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M") if suffix else None
    if columns:
        dfr = dfr[columns]

    # Remove partition columns if they already exist
    new_partition_columns = ["ano_particao", "mes_particao", "data_particao"]
    dfr = dfr.drop(columns=[col for col in new_partition_columns if col in dfr.columns])

    log(f"Dataframe for {data_name} before partitions {dfr.iloc[0]}")
    dataframe, partitions = parse_date_columns(dfr, partition_column)
    log(f"Dataframe for {data_name} after partitions {dataframe.iloc[0]}")

    full_paths = to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type=data_type,
        suffix=suffix,
    )
    # if preffix or rename:
    #     log(f"Adding preffix {preffix} on {full_paths}")
    #     new_paths = []
    #     for full_path in full_paths:
    #         change_filename = f"{preffix}_data" if preffix else rename
    #         new_filename = full_path.name.replace("data", change_filename)
    #         savepath = full_path.with_name(new_filename)

    #     # Renomear o arquivo
    #     full_path.rename(savepath)
    #     new_paths.append(savepath)
    # full_paths = new_paths
    log(f"Files saved on {prepath}, full paths are {full_paths}")
    # TODO alterar funções seguintes para receberem uma lista em vez de ter o full_paths[0]
    return prepath, full_paths


@task
def save_last_dbt_update(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Save on dbt last timestamp where it was updated
    """
    last_update_key = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)
    last_update = get_redis_output(last_update_key)
    redis_key = build_redis_key(dataset_id, table_id, name="dbt_last_update", mode=mode)
    log(f"Saving {last_update} as last time dbt was updated")
    save_str_on_redis(redis_key, "date", last_update["date"])


@task(skip_on_upstream_skip=False)
def check_to_run_dbt(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
) -> bool:
    """
    It will run even if its upstream tasks skip.
    """

    key_table_1 = build_redis_key(dataset_id, table_id, name="dbt_last_update", mode=mode)
    key_table_2 = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)

    format_date_table_1 = "YYYY-MM-DD HH:mm:SS"
    format_date_table_2 = "YYYY-MM-DD HH:mm:SS"

    # Returns true if date saved on table_2 (alertario) is bigger than
    # the date saved on table_1 (dbt).
    run_dbt = compare_dates_between_tables_redis(
        key_table_1, format_date_table_1, key_table_2, format_date_table_2
    )
    log(f">>>> debug data alertario > data dbt: {run_dbt}")
    return run_dbt


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_old_pluviometer(
    dfr: pd.DataFrame,
    wait=None,  # pylint: disable=unused-argument
) -> pd.DataFrame:
    """
    Renomeia colunas no estilo do antigo flow.
    """

    log(
        f"Starting treating pluviometer data to match old API.\
            Pluviometer table enter as\n{dfr.iloc[0]}"
    )
    rename_cols = {
        "acumulado_chuva_15min": "acumulado_chuva_15_min",
        "acumulado_chuva_1h": "acumulado_chuva_1_h",
        "acumulado_chuva_4h": "acumulado_chuva_4_h",
        "acumulado_chuva_24h": "acumulado_chuva_24_h",
        "acumulado_chuva_96h": "acumulado_chuva_96_h",
    }

    dfr.rename(columns=rename_cols, inplace=True)

    # date_format = "%Y-%m-%d %H:%M:%S"
    # dfr["data_medicao"] = dfr["data_medicao"].dt.strftime(date_format)
    dfr.data_medicao = dfr.data_medicao.apply(treat_date_col)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dfr[float_cols] = dfr[float_cols].apply(pd.to_numeric, errors="coerce")

    # Altera valores negativos para None
    dfr[float_cols] = np.where(dfr[float_cols] < 0, None, dfr[float_cols])

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    dfr.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    # Fix columns order
    dfr = dfr[
        [
            "data_medicao",
            "id_estacao",
            "acumulado_chuva_15_min",
            "acumulado_chuva_1_h",
            "acumulado_chuva_4_h",
            "acumulado_chuva_24_h",
            "acumulado_chuva_96_h",
        ]
    ]
    log(
        f"Ending treating pluviometer data to match old API.\
            Pluviometer table finished as\n{dfr.iloc[0]}"
    )
    return dfr


@task
def save_data_old(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    wait=None,  # pylint: disable=unused-argument
) -> Union[str, Path]:
    """
    Salvar dfr tratados em csv para conseguir subir pro GCP
    """

    prepath = Path(f"/tmp/precipitacao_alertario/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    log(f"Dataframe before partitions old api {dfr.iloc[0]}")
    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns_old_api(dfr, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")
    log(f"Dataframe after partitions old api {dataframe.iloc[0]}")

    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"{data_name} files saved on {prepath}")
    return prepath


@task
def convert_sp_timezone_to_utc(dfr, data_column: str = "data_medicao") -> pd.DataFrame:
    """
    Convert a dataframe data_column from São Paulo (UTC-3) to UTC.

    Parameters:
    dfr (pd.DataFrame): DataFrame with data_column.

    Returns:
    pd.DataFrame: DataFrame with data_column converted to UTC.
    """

    if data_column not in dfr.columns:
        raise ValueError(f"DataFrame must contain a column named {data_column}.")

    dfr[data_column] = pd.to_datetime(dfr[data_column])
    dfr[data_column] = dfr[data_column].dt.tz_localize("America/Sao_Paulo")
    dfr[data_column] = dfr[data_column].dt.tz_convert("UTC")
    dfr[data_column] = dfr[data_column].dt.strftime("%Y-%m-%d %H:%M:%S")

    return dfr
