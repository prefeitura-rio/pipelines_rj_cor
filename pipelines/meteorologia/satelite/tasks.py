# -*- coding: utf-8 -*-
# pylint: disable=W0102, W0613, R0913, R0914, R0915
# flake8: noqa: E501
"""
Tasks for emd
"""

import datetime as dt
import os
import re
from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
import pendulum  # pylint: disable=E0401
from prefect import task  # pylint: disable=E0401
from prefect.engine.signals import ENDRUN  # pylint: disable=E0401
from prefect.engine.state import Skipped  # pylint: disable=E0401
from prefeitura_rio.pipelines_utils.logging import log  # pylint: disable=E0401

from pipelines.meteorologia.satelite.satellite_utils import (  # get_point_value,
    choose_file_to_download,
    create_and_save_image,
    download_blob,
    extract_julian_day_and_hour_from_filename,
    get_area_mean_value,
    get_files_from_aws,
    get_files_from_gcp,
    get_info,
    get_variable_values,
    remap_g16,
    save_data_in_file,
)

# import requests


@task()
def get_dates(current_time, product) -> str:
    """
    Task para obter o dia atual caso nenhuma data tenha sido passada
    Subtraimos 5 minutos da hora atual pois o último arquivo que sobre na aws
    sempre cai na hora seguinte (Exemplo: o arquivo
    OR_ABI-L2-RRQPEF-M6_G16_s20230010850208_e20230010859516_c20230010900065.nc
    cujo início da medição foi às 08:50 foi salvo na AWS às 09:00:33).
    """
    if current_time is None:
        current_time = pendulum.now("UTC").subtract(minutes=5).to_datetime_string()
    # Product sst is updating one hour later
    if product == "SSTF":
        current_time = pendulum.now("UTC").subtract(minutes=55).to_datetime_string()
    return current_time


@task(nout=1)
def slice_data(current_time: str, ref_filename: str = None) -> dict:
    """
    slice data to separate in year, julian_day, month, day and hour in UTC
    """
    if ref_filename is not None:
        year, julian_day, hour_utc = extract_julian_day_and_hour_from_filename(ref_filename)
        month = None
        day = None
    else:
        year = current_time[:4]
        month = current_time[5:7]
        day = current_time[8:10]
        hour_utc = current_time[11:13]
        julian_day = dt.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S").strftime("%j")

    date_hour_info = {
        "year": str(year),
        "julian_day": str(julian_day),
        "month": str(month),
        "day": str(day),
        "hour_utc": str(hour_utc),
    }

    return date_hour_info


@task(nout=2, max_retries=10, retry_delay=dt.timedelta(seconds=60))
def download(
    product: str,
    date_hour_info: dict,
    band: str = None,
    ref_filename: str = None,
    redis_files: list = [],
    wait=None,
    mode_redis: str = "prod",
) -> Union[str, Path]:
    """
    Access S3 or GCP and download the first file on this specified date hour
    that is not already saved on redis
    """

    year = date_hour_info["year"]
    julian_day = date_hour_info["julian_day"]
    hour_utc = date_hour_info["hour_utc"][:2]
    partition_path = f"ABI-L2-{product}/{year}/{julian_day}/{hour_utc}/"
    log(f"Getting files from {partition_path}")

    storage_files_path, storage_origin, storage_conection = get_files_from_aws(partition_path)
    log(storage_files_path)
    if len(storage_files_path) == 0:
        storage_files_path, storage_origin, storage_conection = get_files_from_gcp(partition_path)

    # Keep only files from specified band
    if product == "CMIPF":
        # para capturar banda 13
        storage_files_path = [f for f in storage_files_path if bool(re.search("C" + band, f))]

    # Skip task if there is no file on API
    if len(storage_files_path) == 0:
        log("No available files on API")
        skip = Skipped("No available files on API")
        raise ENDRUN(state=skip)

    base_path = os.path.join(os.getcwd(), "temp", "input", mode_redis, product[:-1])

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    # Seleciona primeiro arquivo que não tem o nome salvo no redis
    log(f"\n\n[DEBUG]: available files on API: {storage_files_path}")
    log(f"\n\n[DEBUG]: filenames that are already saved on redis_files: {redis_files}")

    redis_files_updated, destination_file_path, download_file = choose_file_to_download(
        storage_files_path, base_path, redis_files, ref_filename
    )
    log(f"/n redis_files_updated after append function: {redis_files_updated}")
    # Skip task if there is no new file
    if download_file is None:
        log("No new available files")
        skip = Skipped("No new available files")
        raise ENDRUN(state=skip)

    # Download file from aws or gcp
    if storage_origin == "aws":
        storage_conection.get(download_file, destination_file_path)
    else:
        download_blob(
            bucket_name=storage_conection,
            source_blob_name=download_file,
            destination_file_name=destination_file_path,
            mode="prod",
        )

    return destination_file_path, redis_files_updated


@task
def tratar_dados(filename: str) -> dict:
    """
    Convert X, Y coordinates from netcdf file to a latlon coordinates
    and select only the specified region on extent variable.
    """
    log(f"\n Started treating file: {filename}")
    # Create the basemap reference for the Rectangular Projection.
    # You may choose the region you want.

    # Full Disk Extent
    # extent = [-156.00, -81.30, 6.30, 81.30]

    # Brazil region
    # extent = [-90.0, -40.0, -20.0, 10.0]

    # Estado do RJ
    # lat_max, lon_max = (-20.69080839963545, -40.28483671464648)
    # lat_min, lon_min = (-23.801876626302175, -45.05290312102409)

    # Região da cidade do Rio de Janeiro
    # lat_max, lon_min = (-22.802842397418548, -43.81200531887697)
    # lat_min, lon_max = (-23.073487725280266, -43.11300020870994)

    # Recorte da região da cidade do Rio de Janeiro segundo meteorologista
    lat_max, lon_max = (
        -21.699774257353113,
        -42.35676996062447,
    )  # canto superior direito
    lat_min, lon_min = (
        -23.801876626302175,
        -45.05290312102409,
    )  # canto inferior esquerdo

    extent = [lon_min, lat_min, lon_max, lat_max]

    # Get informations from the nc file
    product_caracteristics = get_info(filename)
    product_caracteristics["extent"] = extent

    # Call the remap function to convert x, y to lon, lat and save converted file
    remap_g16(
        filename,
        extent,
        product=product_caracteristics["product"],
        variable=product_caracteristics["variable"],
    )

    return product_caracteristics


@task(nout=2)
def save_data(info: dict, mode_redis: str = "prod") -> Union[str, Path]:
    """
    Concat all netcdf data and save partitioned by date on a csv
    """

    log("Start saving product on a csv")
    output_path, output_filepath = save_data_in_file(
        product=info["product"],
        variable=info["variable"],
        datetime_save=info["datetime_save"],
        mode_redis=mode_redis,
    )
    return output_path, output_filepath


@task
def rearange_dataframe(output_filepath: Path) -> pd.DataFrame:
    """
    Rearrange dataframe to convert it to a data array without creating a mess on lat an lon order.
    The first column is the latitude in descending order and the second column is the longitude
    in ascending order.
    """
    dfr = pd.read_csv(output_filepath)
    dfr = dfr.sort_values(by=["latitude", "longitude"], ascending=[False, True])
    return dfr


@task()
def generate_point_value(info: dict, dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Get the value of a point on the image.
    """

    br_time = pendulum.from_format(info["datetime_save"], "YYYYMMDD HHmmss", tz="America/Sao_Paulo")
    formatted_time = br_time.format("YYYY-MM-DD HH:mm:ss")
    log(f"DEBUG info: {info}")

    df_point_values = pd.DataFrame(
        columns=["produto_satelite", "data_medicao", "metrica", "valor", "latitude", "longitude"]
    )

    for i, var in enumerate(info["variable"]):
        log(f"\nStart getting point value for variable {var}\n")

        var = var.lower()
        selected_point = [-23.06879, -43.35591] if var == "sst" else [-22.89980, -43.35546]

        data_array = get_variable_values(dfr, var)

        # point_value, lat_lon = get_point_value(data_array)
        # df_point_values.loc[i] = [var, formatted_time,"Ponto",point_value,lat_lon[0],lat_lon[1]]

        log(f"\n[DEBUG] max value: {np.nanmax(data_array)} min value: {np.nanmin(data_array)}")
        area_range = list(range(5, 55, 5)) + list(range(60, 150, 10))
        for distance_km in area_range:
            point_value, lat_lon = get_area_mean_value(
                data_array, selected_point=selected_point, distance_km=distance_km
            )
            log(f"DEBUG: Mean value calculated for a distance of {distance_km}: {point_value}")
            if not np.isnan(point_value):
                if var == "sst":
                    point_value -= 273.15
                break

        df_point_values.loc[i] = [
            var,
            formatted_time,
            f"Area {distance_km}km",
            point_value,
            lat_lon[0],
            lat_lon[1],
        ]
        log(f"DEBUG df_point_values: {df_point_values.iloc[i]}")

    return df_point_values.drop_duplicates()


@task(nout=2)
def define_background(type_image_background: str) -> Tuple[str, str]:
    """
    Define if should create image with or without background.

    Args:
    - type_image_background (str): Type of image background.
        Options: "with", "without", "both".

    Returns:
    - create_img_background (bool): If should create image with background.
    - create_img_without_background (bool): If should create image without background.
    """
    create_img_background, create_img_without_background = False, False
    if type_image_background in ["with", "both"]:
        create_img_background = True
    if type_image_background in ["without", "both"]:
        create_img_without_background = True
    log(f"Create img with back {create_img_background} and without {create_img_without_background}")
    return create_img_background, create_img_without_background


@task
def create_image(info: dict, dfr: pd.DataFrame, background: str = "without") -> List:
    """
    Create image from dataframe, get the value of a point on the image and send these to API.

    Input:
    - info: dict
    - dfr: pd.DataFrame
    - background: str ["with", "without", "both"]

    Return:
    - save_image_paths: List
    """
    save_image_paths = []
    for var in info["variable"]:
        log(f"\nStart creating image for variable {var}\n")
        var = var.lower()
        data_array = get_variable_values(dfr, var)

        # Get the pixel values
        data = data_array.data[:]
        if var == "sst":
            data = data - 273.15
            log("Values converted from Kelvin to Celsius")
        log(f"\n[DEBUG] {var} data \n{data}")
        log(f"\nmax value: {data.max()} min value: {data.min()}")

        if background not in ["without"]:
            save_image_paths.append(
                create_and_save_image(data, info, var, with_background=True, with_colorbar=True)
            )
        if background not in ["with"]:
            save_image_paths.append(create_and_save_image(data, info, var, with_background=False))

    log(f"\nImages were saved on {save_image_paths}\n")
    return save_image_paths


# def create_image_and_upload_to_api(info: dict, output_filepath: Path):
#     """
#     Create image from dataframe, get the value of a point on the image and send these to API.
#     """

#     dfr = pd.read_csv(output_filepath)
#     dfr = dfr.sort_values(by=["latitude", "longitude"], ascending=[False, True])
#     for var in info["variable"]:
#         log(f"\nStart creating image for variable {var}\n")
#         var = var.lower()
#         data_array = get_variable_values(dfr, var)
#         point_value = get_point_value(data_array)

#         # Get the pixel values
#         data = data_array.data[:]
#         log(f"\n[DEBUG] {var} data \n{data}")
#         log(f"\nmax value: {data.max()} min value: {data.min()}")
#         save_image_path = create_and_save_image(data, info, var)

#         log(f"\nStart uploading image for variable {var} on API\n")
#         # upload_image_to_api(var, save_image_path, point_value)
#         var = "cp" if var == "cape" else var

#         log("Getting API url")
#         url_secret = get_vault_secret("rionowcast")["data"]
#         log(f"urlsecret1 {url_secret}")
#         url_secret = url_secret["url_api_satellite_products"]
#         log(f"urlsecret2 {url_secret}")
#         api_url = f"{url_secret}/{var.lower()}"
#         log(
#             f"\n Sending image {save_image_path} to API: {api_url} with value {point_value}\n"
#         )

#         payload = {"value": point_value}

#         # Convert from Path to string
#         save_image_path = str(save_image_path)

#         with open(save_image_path, "rb") as image_file:
#             files = {"image": (save_image_path, image_file, "image/jpeg")}
#             response = requests.post(api_url, data=payload, files=files)

#         if response.status_code == 200:
#             log("Finished the request successful!")
#             log(response.json())
#         else:
#             log(f"Error: {response.status_code}, {response.text}")
#         log(save_image_path)
#         log(f"\nEnd uploading image for variable {var} on API\n")


@task(nout=2)
def prepare_data_for_redis(
    dataframe: pd.DataFrame, satellite_variables_list: list, point_values: List[List[dict]]
) -> Union[List, List[dict]]:
    """
    Prepares data for Redis by updating point values based on the given dataframe and
    satellite_variables list.

    Args:
        dataframe (pd.DataFrame): The dataframe containing the data to update point values.
        satellite_variables_list (list): A list of product names to match against the dataframe.
        point_values (List[List[dict]]): A list of lists containing dictionaries of point values.

    Returns:
        Union[List, List[dict]]: A tuple containing the updated satellite_variables list and the
        updated point values.
    """

    log(f"satellite_variables list: {satellite_variables_list}, point_values:\n{point_values}")

    actual_satellite_variables = dataframe["produto_satelite"].unique()
    actual_satellite_variables = [i.lower() for i in actual_satellite_variables]

    if not point_values:
        point_values = [[] for i in actual_satellite_variables]

    updated_point_values = []

    for product, values in zip(satellite_variables_list, point_values):
        if product not in actual_satellite_variables:
            continue
        updated_value = {
            "timestamp": dataframe.loc[
                dataframe["produto_satelite"] == product, "data_medicao"
            ].iloc[0],
            "valor": dataframe.loc[dataframe["produto_satelite"] == product, "valor"].iloc[0],
        }

        check_duplicates = [item["timestamp"] == updated_value["timestamp"] for item in values]
        if sum(check_duplicates) > 0:
            continue

        values.append(updated_value)

        # Drop any value that is not on last 12h
        twelve_hours_ago = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(hours=12)
        values = [
            item
            for item in values
            if (
                pd.to_datetime(item["timestamp"]).tz_convert("America/Sao_Paulo")
                if pd.to_datetime(item["timestamp"]).tzinfo
                else pd.to_datetime(item["timestamp"])
                .tz_localize("UTC")
                .tz_convert("America/Sao_Paulo")
            )
            >= twelve_hours_ago
        ]

        updated_point_values.append(values)
    log(f"Updated point_values:\n{updated_point_values}")
    return satellite_variables_list, updated_point_values


@task
def get_satellite_variables_list(info: dict) -> List:
    """
    Extracts and returns a list of satellite variables from the given info dictionary.

    Args:
        info (dict): A dictionary containing information about the satellite data.

    Returns:
        List: A list of satellite variables extracted from the info dictionary.
    """
    return sorted([var.lower() for var in info["variable"]])
