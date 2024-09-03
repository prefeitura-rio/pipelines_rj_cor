# -*- coding: utf-8 -*-
# pylint: disable=W0612, W0613, W0102, W1514
# flake8: noqa: F841
"""
Tasks for setting rain dashboard using radar data.
"""
# from datetime import timedelta
# import json
import os
from pathlib import Path

# from typing import Union, Tuple

from google.cloud import storage

# import pandas as pd
# import pendulum
from prefect import task

# from prefect.engine.signals import ENDRUN
# from prefect.engine.state import Skipped

# from pipelines.constants import constants
from pipelines.meteorologia.radar.mendanha.utils import (
    download_blob,
    extract_timestamp,
    # list_blobs_with_prefix,
    list_files_storage,
)

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.gcs import get_gcs_client


@task()
def get_filenames_storage(
    bucket_name: str = "rj-escritorio-scp",
    files_saved_redis: list = [],
) -> list:
    """Esc"""
    log("Starting geting filenames from storage")
    volumes = [
        "mendanha/odimhdf5/vol_a/",
        "mendanha/odimhdf5/vol_b/",
        "mendanha/odimhdf5/vol_c/",
        "mendanha/odimhdf5/vol_d/",
    ]

    vol_a = volumes[0]

    # client = storage.Client()
    # client = storage.Client(project="rj-escritorio")

    client: storage.Client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(delimiter='/')
    directories = set()

    for page in blobs.pages:
        directories.update(page.prefixes)
    sorted(directories)
    log(f"Directories inside bucket {directories}")

    # Listar e ordenar arquivos de cada volume
    volume_files = {}

    sorted_files = list_files_storage(bucket, prefix=vol_a)
    log(f"{len(sorted_files)} files found in vol_a")
    volume_files[vol_a] = sorted_files

    # Identificar o último arquivo em vol_a
    last_file_vol_a = volume_files[vol_a][-1]
    last_timestamp_vol_a = extract_timestamp(last_file_vol_a)
    log(f"Last file on vol_a {last_file_vol_a}")

    # TODO: check if this file is already on redis ou mover os arquivos tratados para uma
    # data_partição e assim ter que ler menos nomes de arquivos

    for vol in volumes[1:]:
        sorted_files = list_files_storage(bucket, prefix=vol)
        volume_files[vol] = sorted_files

    # Encontrar os arquivos subsequentes em vol_b, vol_c e vol_d
    selected_files = [last_file_vol_a]
    for vol in volumes[1:]:
        next_files = [
            file for file in volume_files[vol] if extract_timestamp(file) > last_timestamp_vol_a
        ]
        if next_files:
            selected_files.append(next_files[0])

    log(f"Selected files on scp: {selected_files}")
    return selected_files


@task()
def download_files_storage(
    bucket_name: str, files_to_download: list, destination_path: str
) -> None:
    """
    Realiza o download dos arquivos listados em files_to_download no bucket especificado
    """

    os.makedirs(destination_path, exist_ok=True)

    for file in files_to_download:
        log(f"Downloading file {file}")
        source_blob_name, destination_file_name = file, file.split("/")[-1]
        destination_file_name = Path(destination_path, destination_file_name)
        download_blob(bucket_name, source_blob_name, destination_file_name)


# @task
# def save_data(dfr: pd.DataFrame) -> Union[str, Path]:
#     """
#     Save treated data in csv partitioned by date
#     """

#     prepath = Path("/tmp/precipitacao_radar/")
#     prepath.mkdir(parents=True, exist_ok=True)

#     partition_column = "data_medicao"
#     dataframe, partitions = parse_date_columns(dfr, partition_column)
#     suffix = pd.to_datetime(dataframe[partition_column]).max().strftime("%Y%m%d%H%M%S")

#     # Cria partições a partir da data
#     to_partitions(
#         data=dataframe,
#         partition_columns=partitions,
#         savepath=prepath,
#         data_type="csv",
#         suffix=suffix,
#     )
#     log(f"[DEBUG] Files saved on {prepath}")
#     return prepath
