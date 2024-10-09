# -*- coding: utf-8 -*-
"""
Tasks
"""
import datetime
import os
from pathlib import Path
from time import sleep
from typing import Dict, List

import numpy as np
import pandas as pd
from basedosdados.upload.base import Base
from google.cloud import bigquery
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log
from requests.exceptions import HTTPError

from pipelines.constants import constants  # pylint: disable=E0611, E0401
from pipelines.precipitation_model.rionowcast.utils import (  # pylint: disable=E0611, E0401
    GypscieApi,
    wait_run,
)


# noqa E302, E303
@task()
def access_api():
    """# noqa E303
    Acess api and return it to be used in other requests
    """
    infisical_username = constants.INFISICAL_USERNAME.value
    infisical_password = constants.INFISICAL_PASSWORD.value

    # username = get_secret(secret_name="USERNAME", path="/gypscie", environment="prod")
    # password = get_secret(secret_name="PASSWORD", path="/gypscie", environment="prod")

    username = get_secret(infisical_username, path="/gypscie")[infisical_username]
    password = get_secret(infisical_password, path="/gypscie")[infisical_password]
    api = GypscieApi(username=username, password=password)

    return api


@task()
def get_billing_project_id(
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
) -> str:
    """
    Get billing project id
    """
    if not billing_project_id:
        log("Billing project ID was not provided, trying to get it from environment variable")
    try:
        bd_base = Base()
        billing_project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
        log(f"Billing project ID was inferred from environment variables: {billing_project_id}")
    except KeyError:
        pass
    if not billing_project_id:
        raise ValueError(
            "billing_project_id must be either provided or inferred from environment variables"
        )
    log(f"Billing project ID: {billing_project_id}")
    return billing_project_id


def download_data_from_bigquery(query: str, billing_project_id: str) -> pd.DataFrame:
    """ADD"""
    # pylint: disable=E1124, protected-access
    # client = google_client(billing_project_id, from_file=True, reauth=False)
    # job_config = bigquery.QueryJobConfig()
    # # job_config.dry_run = True

    # # Get data
    # log("Querying data from BigQuery")
    # job = client["bigquery"].query(query, job_config=job_config)
    # https://github.com/prefeitura-rio/pipelines_rj_iplanrio/blob/ecd21c727b6f99346ef84575608e560e5825dd38/pipelines/painel_obras/dump_data/tasks.py#L39
    bq_client = bigquery.Client(
        credentials=Base(bucket_name="rj-cor")._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)

    # Get data
    # log("Querying data from BigQuery")
    # job = client["bigquery"].query(query)
    # while not job.done():
    #     sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    log("End download data from bigquery")
    return dfr


@task()
def register_dataset_on_gypscie(api, filepath: Path, domain_id: int = 1) -> Dict:
    """
    Register dataset on gypscie and return its informations like id.
    Obs: dataset name must be unique.
    Return:
    {
        'domain':
        {
            'description': 'This project has the objective to create nowcasting models.',
            'id': 1,
            'name': 'rionowcast_precipitation'
        },
        'file_type': 'csv',
        'id': 18,
        'name': 'rain_gauge_to_model',
        'register': '2024-07-02T19:20:32.507744',
        'uri': 'http://gypscie.dados.rio/api/download/datasets/rain_gauge_to_model.zip'
    }
    """
    log(f"\nStart registring dataset by sending {filepath} Data to Gypscie")

    data = {
        "domain_id": domain_id,
        "name": str(filepath).split("/")[-1].split(".")[0]
        + "_"
        + datetime.datetime.now().strftime("%Y%m%d%H%M%S"),  # pylint: disable=use-maxsplit-arg
    }
    log(type(data), data)
    files = {
        "files": open(file=filepath, mode="rb"),  # pylint: disable=consider-using-with
    }

    response = api.post(path="datasets", data=data, files=files)

    log(f"register_dataset_on_gypscie response: {response} and response.json(): {response.json()}")
    return response.json()


@task(nout=2)
def get_dataset_processor_info(api, processor_name: str):
    """
    Geting dataset processor information
    """
    log(f"Getting dataset processor info for {processor_name}")
    dataset_processors_response = api.get(
        path="dataset_processors",
    )

    # log(dataset_processors_response)
    dataset_processor_id = None
    for response in dataset_processors_response:
        if response.get("name") == processor_name:
            dataset_processor_id = response["id"]
            # log(response)
            # log(response["id"])
    return dataset_processors_response, dataset_processor_id

    # if not dataset_processor_id:
    #     log(f"{processor_name} not found. Try adding it.")


@task()
# pylint: disable=too-many-arguments
def execute_dataset_processor(
    api,
    processor_id: int,
    dataset_id: list,  # como pegar os vários datasets
    environment_id: int,
    project_id: int,
    parameters: dict
    # adicionar campos do dataset_processor
) -> List:
    """
    Requisição de execução de um DatasetProcessor
    """
    log("\nStarting executing dataset processing")

    task_response = api.post(
        path="processor_run",
        json={
            "dataset_id": dataset_id,
            "environment_id": environment_id,
            "parameters": parameters,
            "processor_id": processor_id,
            "project_id": project_id,
        },
    )
    # task_response = {'task_id': '227e74bc-0057-4e63-a30f-8374604e442b'}

    # response = wait_run(api, task_response.json())

    # if response["state"] != "SUCCESS":
    #     failed_message = "Error processing this dataset. Stop flow or restart this task"
    #     log(failed_message)
    #     task_state = Failed(failed_message)
    #     raise ENDRUN(state=task_state)

    # output_datasets = response["result"]["output_datasets"]  # returns a list with datasets
    # log(f"\nFinish executing dataset processing, we have {len(output_datasets)} datasets")
    # return output_datasets
    return task_response.json(["task_id"])


@task()
def predict(api, model_id: int, dataset_id: int, project_id: int) -> dict:
    """
    Requisição de execução de um processo de Predição
    """
    print("Starting prediction")
    response = api.post(
        path="predict",
        data={
            "model_id": model_id,
            "dataset_id": dataset_id,
            "project_id": project_id,
        },
    )
    print(f"Prediction ended. Response: {response}, {response.json()}")
    return response.json()


def calculate_start_and_end_date(
    hours_from_past: int,
) -> tuple[datetime.datetime, datetime.datetime]:
    """
    Calculates the start and end date based on the hours from past
    """
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(hours=hours_from_past)
    return start_date, end_date


@task()
def query_data_from_gcp(  # pylint: disable=too-many-arguments
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    start_date: str = None,
    end_date: str = None,
    save_format: str = "csv",
) -> Path:
    """
    Download historical data from source.
    format: csv or parquet
    """
    log(f"Start downloading {dataset_id}.{table_id} data")

    directory_path = Path("data/input/")
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    savepath = directory_path / f"{dataset_id}_{table_id}"  # TODO:

    # pylint: disable=consider-using-f-string
    # noqa E262
    query = """
        SELECT
            *
        FROM rj-cor.{}.{}
        """.format(
        dataset_id,
        table_id,
    )

    # pylint: disable=consider-using-f-string
    if start_date:
        filter_query = """
            WHERE data_particao BETWEEN '{}' AND '{}'
        """.format(
            start_date, end_date
        )
        query += filter_query

    log(f"Query used to download data:\n{query}")

    dfr = download_data_from_bigquery(query=query, billing_project_id=billing_project_id)
    if save_format == "csv":
        dfr.to_csv(f"{savepath}.csv", index=False)
    elif save_format == "parquet":
        dfr.to_parquet(f"{savepath}.parquet", index=False)
    # bd.download(savepath=savepath, query=query, billing_project_id=billing_project_id)

    log(f"{table_id} data saved on {savepath}")
    return savepath


@task()
def execute_prediction_on_gypscie(
    api,
    model_params: dict,
    # hours_to_predict,
) -> str:
    """
    Requisição de execução de um processo de Predição
    Return task_id
    """
    log("Starting prediction")
    task_response = api.post(
        path="workflow_run",
        json=model_params,
    )
    # data={
    #             "model_id": model_id,
    #             "dataset_id": dataset_id,
    #             "project_id": project_id,
    #         },
    response = wait_run(api, task_response.json())

    if response["state"] != "SUCCESS":
        failed_message = "Error processing this dataset. Stop flow or restart this task"
        log(failed_message)
        task_state = Failed(failed_message)
        raise ENDRUN(state=task_state)

    print(f"Prediction ended. Response: {response}, {response.json()}")
    # TODO: retorna a predição? o id da do dataset?

    return response.json().get("task_id")  # response.json().get('task_id')


@task
def task_wait_run(api, task_response, flow_type: str = "dataflow") -> Dict:
    """
    Force flow wait for the end of data processing
    flow_type: dataflow or processor
    """
    return wait_run(api, task_response, flow_type)


@task
def get_dataflow_params(  # pylint: disable=too-many-arguments
    workflow_id,
    environment_id,
    project_id,
    load_data_funtion_id,
    pre_processing_function_id,
    model_function_id,
    radar_data_id,
    rain_gauge_data_id,
    grid_data_id,
    model_data_id,
) -> List:
    """
    Return parameters for the model

    {
        "workflow_id": 36,
        "environment_id": 1,
        "parameters": [
            {
                "function_id":42,
                "params": {"radar_data_path":178, "rain_gauge_data_path":179, "grid_data_path":177}
            },
            {
                "function_id":43
            },
            {
                "function_id":45,
                "params": {"model_path":191}  # model was registered on Gypscie as a dataset
            }
        ],
        "project_id": 1
    }
    """
    return {
        "workflow_id": workflow_id,
        "environment_id": environment_id,
        "parameters": [
            {
                "function_id": load_data_funtion_id,
                "params": {
                    "radar_data_path": radar_data_id,
                    "rain_gauge_data_path": rain_gauge_data_id,
                    "grid_data_path": grid_data_id,
                },
            },
            {
                "function_id": pre_processing_function_id,
            },
            {"function_id": model_function_id, "params": {"model_path": model_data_id}},
        ],
        "project_id": project_id,
    }


@task()
def get_output_dataset_ids_on_gypscie(
    api,
    task_id,
) -> List:
    """
    Get output files id with predictions
    """
    try:
        response = api.get(path="status_workflow_run/" + task_id)
        response = response.json()
    except HTTPError as err:
        if err.response.status_code == 404:
            print(f"Task {task_id} not found")
            return []

    return response.get("output_datasets")


@task()
def download_datasets_from_gypscie(
    api,
    dataset_names: List,
    wait=None,
) -> List:
    """
    Get output files with predictions
    """
    for file_name in dataset_names:
        response = api.get(path=f"download/datasets/{file_name}.zip")
        if response.status_code == 200:
            log(f"Dataset {file_name} downloaded")
        else:
            log(f"Dataset {file_name} not found on Gypscie")
    # TODO: verificar se o arquivo é .zip mesmo
    return [dataset_name + ".zip" for dataset_name in dataset_names]


@task
def desnormalize_data(array: np.ndarray):
    """
    Desnormalize data

    Inputs:
        array: numpy array
    Returns:
        a numpy array with the values desnormalized
    """
    return array


@task
def geolocalize_data(prediction_datasets: np.ndarray, now_datetime: str) -> pd.DataFrame:
    """
    Geolocalize data using grid and add timestamp

    Inputs:
        prediction_datasets: numpy array
        now_datetime: string in format YYYY_MM_DD__H_M_S
    Returns:
        a pandas dataframe to be saved on GCP
    Expected columns: latitude, longitude, janela_predicao,
    valor_predicao, data_predicao (timestamp em que foi realizada a previsão)
    """
    return prediction_datasets


@task
def create_image(data) -> List:
    """
    Create image using Geolocalized data or the numpy array from desnormalized_data function
    Exemplo de código que usei pra gerar uma imagem vindo de um xarray:

    def create_and_save_image(data: xr.xarray, variable) -> Path:
        plt.figure(figsize=(10, 10))

        # Use the Geostationary projection in cartopy
        axis = plt.axes(projection=ccrs.PlateCarree())

        lat_max, lon_max = (
            -21.708288842894145,
            -42.36573106186053,
        )  # canto superior direito
        lat_min, lon_min = (
            -23.793855217170343,
            -45.04488171189226,
        )  # canto inferior esquerdo

        extent = [lon_min, lat_min, lon_max, lat_max]
        img_extent = [extent[0], extent[2], extent[1], extent[3]]

        # Define the color scale based on the channel
        colormap = "jet"  # White to black for IR channels

        # Plot the image
        img = axis.imshow(data, origin="upper", extent=img_extent, cmap=colormap, alpha=0.8)

        # Add coastlines, borders and gridlines
        axis.coastlines(resolution='10m', color='black', linewidth=0.8)
        axis.add_feature(cartopy.feature.BORDERS, edgecolor='white', linewidth=0.5)


        grdln = axis.gridlines(
            crs=ccrs.PlateCarree(),
            color="gray",
            alpha=0.7,
            linestyle="--",
            linewidth=0.7,
            xlocs=np.arange(-180, 180, 1),
            ylocs=np.arange(-90, 90, 1),
            draw_labels=True,
        )
        grdln.top_labels = False
        grdln.right_labels = False

        plt.colorbar(
            img,
            label=variable.upper(),
            extend="both",
            orientation="horizontal",
            pad=0.05,
            fraction=0.05,
        )

        output_image_path = Path(os.getcwd()) / "output" / "images"

        save_image_path = output_image_path / (f"{variable}.png")

        if not output_image_path.exists():
            output_image_path.mkdir(parents=True, exist_ok=True)

        plt.savefig(save_image_path, bbox_inches="tight", pad_inches=0, dpi=300)
        plt.show()
        return save_image_path
    """
    save_image_path = "image.png"

    return save_image_path


@task
def get_dataset_info(station_type: str, source: str) -> Dict:
    """
    Inputs:
        station_type: str ["rain_gauge", "weather_station", "radar"]
        source: str ["alertario", "inmet", "mendanha"]
    """

    if station_type == "rain_gauge":
        dataset_info = {
            "dataset_id": "clima_pluviometro",
            "filename": "gauge_station_bq",
            "partition_date_column": "datetime",
        }
        if source == "alertario":
            dataset_info["table_id"] = "taxa_precipitacao_alertario"
            dataset_info["destination_table_id"] = "preprocessamento_pluviometro_alertario"
    elif station_type == "weather_station":
        dataset_info = {
            "dataset_id": "clima_pluviometro",
            "filename": "weather_station_bq",
            "partition_date_column": "datetime",
        }
        if source == "alertario":
            dataset_info["table_id"] = "meteorologia_alertario"
            dataset_info[
                "destination_table_id"
            ] = "preprocessamento_estacao_meteorologica_alertario"
        elif source == "inmet":
            dataset_info["table_id"] = "meteorologia_inmet"
            dataset_info["destination_table_id"] = "preprocessamento_estacao_meteorologica_inmet"
    else:
        dataset_info = {
            "dataset_id": "clima_radar",
            "partition_date_column": "datetime",
        }
        if source == "mendanha":
            dataset_info["storage_path"] = ""
            dataset_info["destination_table_id"] = "preprocessamento_radar_mendanha"
        elif source == "guaratiba":
            dataset_info["storage_path"] = ""
            dataset_info["destination_table_id"] = "preprocessamento_radar_guaratiba"
        elif source == "macae":
            dataset_info["storage_path"] = ""
            dataset_info["destination_table_id"] = "preprocessamento_radar_macae"

    return dataset_info


def path_to_dfr(path: str) -> pd.DataFrame:

    """
    Reads a csv or parquet file from the given path and returns a dataframe
    """
    if path.endswith(".csv"):
        dfr = pd.read_csv(path)
    elif path.endswith(".parquet"):
        dfr = pd.read_parquet(path)
    else:
        raise ValueError("File extension not supported")
    return dfr


def add_columns_on_dfr(
    dfr: pd.DataFrame, model_version: int, update_time: bool = False
) -> pd.DataFrame:
    """
    Reads a csv or parquet file from the given path and adds a column
    with the update time based on Brazil timezone
    """
    if update_time:
        dfr["update_time"] = pd.Timestamp.now(tz="America/Sao_Paulo")
    if model_version is not None:
        dfr["model_version"] = model_version
    return dfr
