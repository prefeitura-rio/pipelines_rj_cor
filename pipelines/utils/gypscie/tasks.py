# -*- coding: utf-8 -*-
# pylint: disable= C0207
"""
Tasks
"""
import datetime
import gzip
import os
import shutil
import zipfile
from pathlib import Path
from time import sleep
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from basedosdados import Base  # pylint: disable=E0611, E0401
from google.cloud import bigquery  # pylint: disable=E0611, E0401
from prefect import task, context  # pylint: disable=E0611, E0401
from prefect.engine.signals import ENDRUN  # pylint: disable=E0611, E0401
from prefect.engine.state import Failed, Skipped  # pylint: disable=E0611, E0401

# pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from requests.exceptions import HTTPError

from pipelines.constants import constants  # pylint: disable=E0611, E0401
from pipelines.utils.gypscie.utils import (  # pylint: disable=E0611, E0401
    GypscieApi,
    wait_run,
)


# noqa E302, E303
# @task(timeout=300)
@task()
def access_api():
    """# noqa E303
    Acess api and return it to be used in other requests
    """
    infisical_path = constants.INFISICAL_PATH.value
    infisical_url = constants.INFISICAL_URL.value
    infisical_username = constants.INFISICAL_USERNAME.value
    infisical_password = constants.INFISICAL_PASSWORD.value

    # username = get_secret(secret_name="USERNAME", path="/gypscie", environment="prod")
    # password = get_secret(secret_name="PASSWORD", path="/gypscie", environment="prod")

    url = get_secret(infisical_url, path=infisical_path)[infisical_url]
    username = get_secret(infisical_username, path=infisical_path)[infisical_username]
    password = get_secret(infisical_password, path=infisical_path)[infisical_password]
    api = GypscieApi(base_url=url, username=username, password=password)

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
    parameters: dict,
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
def execute_dataflow_on_gypscie(
    api,
    model_params: dict,
    # hours_to_predict,
) -> List:
    """
    Requisição de execução de um processo de Predição
    Return
    {'state': 'STARTED'}
    {'result': {'output_datasets': [236]}, 'state': 'SUCCESS'}
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

    log(f"Prediction ended. Response: {response}")
    return response["result"].get("output_datasets")


@task
def task_wait_run(api, task_response, flow_type: str = "dataflow") -> Dict:
    """
    Force flow wait for the end of data processing
    flow_type: dataflow or processor
    """
    return wait_run(api, task_response, flow_type)


@task
def get_dataflow_alertario_params(  # pylint: disable=too-many-arguments
    workflow_id,
    environment_id,
    project_id,
    rain_gauge_data_id,
    rain_gauge_metadata_path,
    load_data_function_id,
    parse_date_time_function_id,
    drop_duplicates_function_id,
    replace_inconsistent_values_function_id,
    add_lat_lon_function_id,
    save_data_function_id,
) -> List:
    """
    Return parameters for the alertario ETL

    {
        "workflow_id": 41,
        "environment_id": 1,
        "parameters": [
            {
                "function_id":53,  # load_data
                "params": {
                    "rain_gauge_data_path":226,
                    "rain_gauge_metadata_path":227
                }
            },
            {
                "function_id":54  # parse_date_time
            },
            {
                "function_id":55  # drop_duplicates
            },
            {
                "function_id":56  # replace_inconsistent_values
            },
            {
                "function_id":57  # add_lat_lon
            },
            {
                "function_id":58,  # save_data
                "params": {"output_path":"dados_alertario_20230112_190000.parquet"}
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
                "function_id": load_data_function_id,
                "params": {
                    "rain_gauge_data_path": rain_gauge_data_id,
                    "rain_gauge_metadata_path": rain_gauge_metadata_path,
                },
            },
            {
                "function_id": parse_date_time_function_id,
            },
            {
                "function_id": drop_duplicates_function_id,
            },
            {
                "function_id": replace_inconsistent_values_function_id,
            },
            {
                "function_id": add_lat_lon_function_id,
            },
            {
                "function_id": save_data_function_id,
                "params": {"output_path": "preprocessed_data_alertario.parquet"},
            },
        ],
        "project_id": project_id,
    }


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
    output_function_id,
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
            {"function_id": output_function_id, "params": {"output_path": "prediction.npy"}},
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
    log(f"status_workflow_run response {response}")

    return response.get("output_datasets")


@task()
def get_dataset_name_on_gypscie(
    api,
    dataset_ids: list,
) -> List:
    """
    Get datasets name using their dataset ids
    """
    dataset_names = []
    log(f"All dataset_ids to get names: {dataset_ids}")
    for dataset_id in dataset_ids:
        log(f"Getting name for dataset id: {dataset_id}")
        try:
            response = api.get(path="datasets/" + str(dataset_id))
        except HTTPError as err:
            if err.response.status_code == 404:
                print(f"Dataset_id {dataset_id} not found")
                return []
        log(f"Get dataset name response {response}")
        dataset_names.append(response.get("name"))
    log(f"All dataset names {dataset_names}")
    return dataset_names


@task()
def download_datasets_from_gypscie(
    api,
    dataset_names: List,
    wait=None,  # pylint: disable=unused-argument
) -> List:
    """
    Get output files with predictions
    """
    log(f"\n\nDataset names to be downloaded from Gypscie: {dataset_names}")
    for dataset_name in dataset_names:
        log(f"Downloading dataset {dataset_name} from Gypscie")
        response = api.get(f"download/datasets/{dataset_name}.zip")
        log(f"Download {dataset_name}'s response: {response}")
        if response.status_code == 200:
            dataset = response.content
            with open(f"{dataset_name}.zip", "wb") as file:
                file.write(dataset)
            log(f"Dataset {dataset_name} downloaded")
        else:
            log(f"Dataset {dataset_name} not found on Gypscie")
    return dataset_names


@task
def unzip_files(compressed_files: List[str], destination_folder: str = "./") -> List[str]:
    """
    Unzip .zip and .gz files to destination folder.
    """
    log(f"Compressed files: {compressed_files} will be sent to {destination_folder}.")
    compressed_files = [
        zip_file if zip_file.endswith((".zip", ".gz")) else zip_file + ".zip"
        for zip_file in compressed_files
    ]
    os.makedirs(destination_folder, exist_ok=True)

    extracted_files = []
    for file in compressed_files:
        if file.endswith(".zip"):
            log("zip file found")
            with zipfile.ZipFile(file, "r") as zip_ref:
                zip_ref.extractall(destination_folder)
                extracted_files.extend(
                    [os.path.join(destination_folder, f) for f in zip_ref.namelist()]
                )
        elif file.endswith(".gz"):
            output_file = os.path.join(destination_folder, os.path.basename(file)[:-3])
            with gzip.open(file, "rb") as gz_file:
                with open(output_file, "wb") as out_file:
                    shutil.copyfileobj(gz_file, out_file)
            extracted_files.append(output_file)
    log(f"Extracted files: {extracted_files}")
    return extracted_files


@task
def read_numpy_files(file_paths: List[str]) -> List[np.ndarray]:
    """
    Read numpy arrays and return a list with of them
    """
    arrays = []
    for file_path in file_paths:
        array = np.load(file_path)
        arrays.append(array)
    return arrays


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
    now_datetime = now_datetime + 1
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
    data = data + 1
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
            dataset_info["destination_table_id"] = (
                "preprocessamento_estacao_meteorologica_alertario"
            )
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
    log(f"Dataset info: {dataset_info}")
    return dataset_info


@task
def path_to_dfr(paths: List[str]) -> pd.DataFrame:
    """
    Reads csvs or parquets filess from the given paths and returns a concatenated dataframe.
    """
    log(f"Start converting files from {paths} to a df.")
    dataframes = []

    for path in paths:
        try:
            if path.endswith(".csv"):
                dfr_ = pd.read_csv(path)
            elif path.endswith(".parquet"):
                dfr_ = pd.read_parquet(path)
            else:
                raise ValueError(f"File extension not supported for file: {path}")
            dataframes.append(dfr_)

        except AttributeError as error:
            log(f"type(path) {type(path)} error {error}")

    if dataframes:
        dfr = pd.concat(dataframes, ignore_index=True)
    else:
        dfr = pd.DataFrame()
    log(f"Dataframe : {dfr.iloc[0]}")
    return dfr


@task
def add_caracterization_columns_on_dfr(
    dfr: pd.DataFrame, model_version: None, update_time: bool = False
) -> pd.DataFrame:
    """
    Add a column with the update time based on Brazil timezone and model version
    """

    if update_time:
        dfr["update_time"] = pd.Timestamp.now(tz="America/Sao_Paulo")
    if model_version is not None:
        model_version_ = str(model_version)
        dfr["model_version"] = model_version_
    log(f"Dataframe with new columns {dfr.iloc[0]}")
    return dfr


@task
def convert_columns_type(
    dfr: pd.DataFrame, columns: list = None, new_types: list = None
) -> pd.DataFrame:
    """
    Converts specified columns in a DataFrame to the provided data types.

    Parameters:
        dfr (pd.DataFrame): The input DataFrame to modify.
        columns (list): List of column names to be converted.
        new_types (list): List of target data types for each column, in the same order as `columns`.

    Returns:
        pd.DataFrame: The modified DataFrame with columns converted to specified types.
    """
    if len(columns) != len(new_types):
        raise ValueError("The lists `columns` and `new_types` must be of the same length.")

    for col, new_type in zip(columns, new_types):
        if col in dfr.columns:
            dfr[col] = dfr[col].astype(new_type)

    return dfr


@task
def rename_files(
    files: List[Union[Path, str]],
    original_name: str = "data",
    preffix: str = None,
    rename: str = None,
) -> List[Path]:
    """
    Renomeia os arquivos com base em um prefixo ou novo nome.
    """
    new_paths = []
    for file_path in files:
        file_path = Path(file_path)
        print(f"Original file path: {file_path}")

        change_filename = f"{preffix}_{original_name}" if preffix else rename
        print(f"Name to replace '{original_name}' with: {change_filename}")
        new_filename = file_path.name.replace(original_name, change_filename)
        savepath = file_path.with_name(new_filename)

        # Rename file
        file_path.rename(savepath)
        new_paths.append(savepath)
        print(f"Renamed file paths: {new_paths}")
    return new_paths


@task
def timeout_flow(
    timeout_seconds: int = 600,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Stop flow if it exceeds timeout_seconds
    """
    start_time = datetime.datetime.now(datetime.timezone.utc)
    while True:
        elapsed_time = datetime.datetime.now(datetime.timezone.utc) - start_time
        if elapsed_time > datetime.timedelta(seconds=timeout_seconds):
            stop_message = f"Time exceeded. Stop flow after {timeout_seconds} seconds"
            log(stop_message)
            task_state = Skipped(stop_message)
            raise ENDRUN(state=task_state)
        sleep(30)


def monitor_flow(timeout_seconds, flow_):
    """
    Tarefa de monitoramento paralela para interromper o fluxo
    se o tempo total ultrapassar o limite.
    """
    start_time = datetime.utcnow()
    while True:
        elapsed_time = datetime.utcnow() - start_time
        if elapsed_time > datetime.timedelta(seconds=timeout_seconds):
            log(f"elapsed_time {elapsed_time}")
            logger = context.get("logger")
            stop_message = (
                f"Tempo limite de {timeout_seconds} segundos excedido. Encerrando o fluxo."
            )
            logger.warning(stop_message)
            flow_.set_reference_tasks([Failed(stop_message)])  # Define o estado de falha do fluxo
            return
        sleep(10)  # Verifica o tempo a cada 10 segundos
