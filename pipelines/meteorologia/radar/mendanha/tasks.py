# -*- coding: utf-8 -*-
# pylint: disable=W0612, W0613, W0102, W1514
# flake8: noqa: F841
"""
Tasks for setting rain dashboard using radar data.
"""
# from datetime import timedelta
# import json
import io
import os
from pathlib import Path
from time import time, sleep
from typing import Dict, List, Union, Tuple
import zipfile

import cartopy.crs as ccrs
import contextily as ctx
from google.cloud import storage
import matplotlib.pyplot as plt
import numpy as np
import pendulum
import pyart

# import wradlib as wrl
import xarray as xr

# from mpl_toolkits.axes_grid1 import make_axes_locatable
from prefect import task

# from pyart.map import grid_from_radars

from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed  # Skipped

from pipelines.constants import constants
from pipelines.meteorologia.radar.mendanha.utils import (
    create_colormap,
    extract_timestamp,
    # list_all_directories,
    open_radar_file,
    save_image_to_local,
)
from pipelines.utils_rj_cor import (
    download_blob,
    get_redis_output,
    list_files_storage,
    save_str_on_redis,
)
from pipelines.utils_api import Api
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.infisical import get_secret
from prefeitura_rio.pipelines_utils.gcs import get_gcs_client


@task()
def get_filenames_storage(
    bucket_name: str = "rj-escritorio-scp",
    files_saved_redis: list = [],
) -> List:
    """Esc
    Volumes vol_c and vol_d need's almost 3 minutes to be generated after vol_a.
    We will wait 5 minutes to continue pipeline or stop flow
    """
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

    # directories = list_all_directories(bucket, bucket_name)
    # log(f"Directories inside bucket {directories}")

    sorted_files = list_files_storage(bucket, prefix=vol_a, sort_key=extract_timestamp)
    log(f"{len(sorted_files)} files found in vol_a")
    log(f"Last 5 files found on {vol_a}: {sorted_files[-5:]}")

    # Identificar o último arquivo em vol_a
    last_file_vol_a = sorted_files[-1]
    last_timestamp_vol_a = extract_timestamp(last_file_vol_a)
    log(f"Last file on vol_a {last_file_vol_a}")

    # TODO: check if this file is already on redis ou mover os arquivos tratados para uma
    # data_partição e assim ter que ler menos nomes de arquivos

    # Encontrar os arquivos subsequentes em vol_b, vol_c e vol_d
    selected_files = [last_file_vol_a]
    for vol in volumes[1:]:
        start_time = time()
        elapsed_time = 0
        next_files = []
        while len(next_files) == 0 and elapsed_time <= 1 * 60:  # TO DO: change to 5 or 10
            sorted_files = list_files_storage(bucket, prefix=vol, sort_key=extract_timestamp)
            log(f"Last 5 files found on {vol}: {sorted_files[-5:]}")
            next_files = [
                file for file in sorted_files if extract_timestamp(file) > last_timestamp_vol_a
            ]
            if not next_files:
                end_time = time()
                elapsed_time = end_time - start_time
                sleep(30)

        if next_files:
            selected_files.append(next_files[0])
        else:
            message = f"It was not possible to find {vol}. Ending run"
            log(message)
            raise ENDRUN(state=Failed(message))

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
    files_path = []
    for file in files_to_download:
        log(f"Downloading file {file}")
        source_blob_name, destination_file_name = file, file.split("/")[-1]
        destination_file_name = Path(destination_path, destination_file_name)
        download_blob(bucket_name, source_blob_name, destination_file_name)
        files_path.append(destination_file_name)
        log(f"File saved on {destination_file_name}")
    log("Finished Downloading all files")
    log(files_path)
    return files_path


@task
def combine_radar_files(radar_files: list) -> pyart.core.Radar:
    """
    Combine files from same radar but with different angles sweeps
    """
    log(f"Start combining radar files {radar_files}")
    log("Opening file from vol_a")

    combined_radar = open_radar_file(radar_files[0])

    for i in radar_files[1:]:
        log(f"Opening file from {i}")
        radar_file_ = open_radar_file(i)
        if radar_file_:
            combined_radar = pyart.util.join_radar(combined_radar, radar_file_)
    return combined_radar


@task
def get_and_format_time(radar_files: list) -> str:
    """
    Get time from first file and convert it to São Paulo timezone
    """
    radar = pyart.aux_io.read_odim_h5(radar_files[0])
    utc_time_str = radar.time["units"].split(" ")[-1]
    utc_time = pendulum.parse(utc_time_str, tz="UTC")
    br_time = utc_time.in_timezone("America/Sao_Paulo")
    formatted_time = br_time.format("ddd MMM DD HH:mm:ss YYYY")

    log(f"Time of first file in São Paulo timezone: {formatted_time}")
    return formatted_time


@task(nout=2)
def get_radar_parameters(radar) -> Union[Tuple, Tuple]:
    """
    Get radar information
    https://github.com/syedhamidali/CAPPI-NETCDF/blob/main/simple_cappi.ipynb
    """
    log("Start getting radar parameters")

    # Max radar distance (meters)
    max_range = radar.range["data"][-1]

    # Max elevation (meters) - aproximated
    max_altitude = radar.altitude["data"][0] + max_range * np.sin(
        np.deg2rad(radar.elevation["data"].max())
    )

    # Horizontal limits (x e y)
    x_limits = (-max_range, max_range)
    y_limits = (-max_range, max_range)

    # Vertical limits (z)
    z_limits = (0, max_altitude)

    # Define grid_limits
    grid_limits = (z_limits, y_limits, x_limits)

    # Number of vertical levels
    z_levels = 21  # Você pode ajustar conforme necessário

    # Resolution in x and y (number of points on horizontal grade)
    x_points = int((x_limits[1] - x_limits[0]) / radar.range["meters_between_gates"])
    y_points = int((y_limits[1] - y_limits[0]) / radar.range["meters_between_gates"])

    # Define grid_shape
    grid_shape = (z_levels, y_points, x_points)

    log(f"grid_limits: {grid_limits}")
    log(f"grid_shape: {grid_shape}")

    return grid_shape, grid_limits


@task
def remap_data(radar, radar_products: list, grid_shape: tuple, grid_limits: tuple) -> xr.Dataset:
    """
    Interpolate radar products data to obtain a 2D map and convert it to an xarray type
    """
    log("Start interpolating data to a 2D grid from radars")
    grid = pyart.map.grid_from_radars(
        radar,
        grid_shape=grid_shape,
        grid_limits=grid_limits,
        weighting_function="Barnes2",
        fields=radar_products,
    )

    log("Start convert grid to an array")
    radar_2d = grid.to_xarray()
    return radar_2d


@task
def create_visualization_no_background(radar_2d, radar_product: str, cbar_title: str, title: str):
    """
    Plot radar 2D data over Rio de Janeiro's map using the same
    color as they used before on colorbar
    """
    log(f"Start creating {radar_product} visualization without background")
    cmap, norm, ordered_values = create_colormap()

    proj = ccrs.PlateCarree()

    fig, ax = plt.subplots(
        figsize=(10, 10), subplot_kw={"projection": proj}
    )  # pylint: disable=C0103
    ax.set_aspect("auto")

    # Extract data and coordinates from Xarray
    data = radar_2d[radar_product][0].max(axis=0).values
    lon = radar_2d["lon"].values
    lat = radar_2d["lat"].values

    # Plot data over base map
    contour = ax.contourf(
        lon, lat, data, cmap="pyart_NWSRef", levels=range(-10, 60), transform=proj, alpha=1
    )

    # Configure axes
    ax.set_title(
        title, position=[0.01, 0.01], fontsize=11, color="white", backgroundcolor="black"
    )  # , fontweight='bold', loc="left"

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.axis("off")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlim(lon.min(), lon.max())
    ax.set_ylim(lat.min(), lat.max())

    # Customize colorbar to show only the specified values on the center of each box
    cbar_ax = fig.add_axes([0.001, 0.5, 0.03, 0.3])  # [left, bottom, width, height]
    cbar = plt.colorbar(
        mappable=plt.cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        cax=cbar_ax,
        orientation="vertical",
    )
    cbar.set_ticks([int(value) + 2.5 for value in ordered_values])
    cbar.set_ticklabels([str(value) for value in ordered_values])
    cbar.ax.tick_params(size=0)
    cbar.ax.set_title(
        cbar_title, fontsize=12, fontweight="bold", pad=10, position=[2.2, 0.4]
    )  # left, height

    # Definir fundo transparente para a figura e os eixos
    fig.patch.set_alpha(0.0)  # Fundo da figura
    ax.patch.set_alpha(0.0)  # Fundo dos eixos
    # image_path = Path('radar_020.png')
    # log(f"Saving image to {image_path}")
    # plt.savefig(image_path, transparent=True, bbox_inches='tight', pad_inches=0.1)
    # plt.savefig(image_path,  bbox_inches='tight', pad_inches=0.1)

    # plt.show()
    return fig


@task
def img_to_base64(img):
    """Convert matplotlib fig to base64 to sent it to API"""
    log("Start converting fig to base64")
    img_base64 = io.BytesIO()
    img.savefig(img_base64, format="png", bbox_inches="tight", pad_inches=0.1)
    img_base64.seek(0)
    # img_base64 = base64.b64encode(img_base64.getvalue()).decode('utf-8')
    img_base64.getvalue()
    return img_base64


@task
def base64_to_bytes(img_base64):
    """Convert base64 to to bytes save it on redis"""
    log("Start converting base64 to bytes")
    img_base64.seek(0)  # Move o cursor para o início do BytesIO
    img_bytes = img_base64.getvalue()  # Obtém o conteúdo em bytes
    return img_bytes


@task
def add_new_image(image_dict: dict, img_bytes) -> Dict:
    """
    Adding new image to dictionary after changing the name of the old ones on redis
    """
    image_dict["radar_020.png"] = img_bytes
    return image_dict


@task
def rename_keys_redis(redis_hash: str, new_image) -> Dict:
    """
    Renaming redis keys so image_003
    TODO: put all redis functions that are inside pipelines/utils/utils.py
    on pipelines/utils_rj_cor.py on all flows
    """

    log("Start renaming images on redis")
    redis_images = get_redis_output(redis_hash)

    redis_images_list = list(redis_images.keys())
    log(f"Images names that are already on redis {redis_images_list}")

    list_len = len(redis_images_list)
    # TO DO: if list_len == 0 skip task
    if list_len < 20 and list_len != 0:
        redis_images_list = [
            "radar_001.png",
            "radar_002.png",
            "radar_003.png",
            "radar_004.png",
            "radar_005.png",
            "radar_006.png",
            "radar_007.png",
            "radar_008.png",
            "radar_009.png",
            "radar_010.png",
            "radar_011.png",
            "radar_012.png",
            "radar_013.png",
            "radar_014.png",
            "radar_015.png",
            "radar_016.png",
            "radar_017.png",
            "radar_018.png",
            "radar_019.png",
            "radar_020.png",
        ]
        redis_images_list = redis_images_list[-list_len - 1 :]

    redis_images_list.sort()

    img_base64_dict = {}
    for i in range(0, len(redis_images_list) - 1):
        key_name = redis_images_list[i]
        value = redis_images[redis_images_list[i + 1]]
        img_base64_dict[key_name] = value
        log(f"Renaming image to {key_name}")
        save_str_on_redis(redis_hash, key_name, value)

    img_base64_dict["radar_020.png"] = new_image
    return img_base64_dict


@task
def save_images_to_local(img_base64_dict: dict, folder: str = "temp") -> str:
    """
    Save images in a PNG file
    """
    log(f"Saving image(s): {img_base64_dict.keys()} to local path")

    os.makedirs(folder, exist_ok=True)
    for key, value in img_base64_dict.items():
        save_image_to_local(filename=key, img=value, path=folder)
        # log("antes decode", type(v))
        # img_data = base64.b64decode(v)
        # log("depois decode", type(img_data))
        # with open(k, 'wb') as img_file:
        #     img_file.write(img_data)
    return folder


@task
def compress_to_zip(zip_filename: str = "images.zip", folder: str = "temp"):
    """
    Compress all images to a zip file
    """
    log(f"Start compressing files that are inside folder {folder}")
    with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder):
            for file in files:
                if file.endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff")):
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.relpath(file_path, folder))

    log(f"Todas as imagens foram comprimidas em {zip_filename}")
    return zip_filename


@task
def save_img_on_redis(
    redis_hash: str,
    key: str,
    value: str,
):
    """
    Function to save a string on redis
    """
    save_str_on_redis(redis_hash, key, value)
    return True


@task
def send_zip_images_api(api, api_route, zip_file_path) -> dict:
    """
    Send zip images to COR API
    """
    with open(zip_file_path, "rb") as file:
        files = {"file": (os.path.basename(zip_file_path), file, "application/zip")}
        response = api.post(api_route, files=files)
        log(response.json())
    return response


# noqa E302, E303
@task()
def access_api():
    """# noqa E303
    Acess api and return it to be used in other requests
    """
    log("Start accessing API")
    infisical_url = constants.INFISICAL_URL.value
    infisical_username = constants.INFISICAL_USERNAME.value
    infisical_password = constants.INFISICAL_PASSWORD.value

    base_url = get_secret(infisical_url, path="/api_radar_mendanha")[infisical_url]
    username = get_secret(infisical_username, path="/api_radar_mendanha")[infisical_username]
    password = get_secret(infisical_password, path="/api_radar_mendanha")[infisical_password]
    api = Api(username=username, password=password, base_url=base_url, header_type="token")
    log("Accessed API")
    return api


@task
def get_colorbar_title(radar_product: str):
    """
    Get colorbar title based on radar product
    """
    colorbar_title = {"reflectivity_horizontal": "REFLECTIVITY\n(dBZ)"}
    return colorbar_title[radar_product]


@task
def create_visualization_with_background(radar_2d, radar_product: str, cbar_title: str, title: str):
    """
    Plot radar 2D data over Rio de Janeiro's map using the same
    color as they used before on colorbar
    """
    log(f"Start creating {radar_product} visualization with background")
    cmap, norm, ordered_values = create_colormap()

    proj = ccrs.PlateCarree()

    fig, ax = plt.subplots(
        figsize=(10, 10), subplot_kw={"projection": proj}
    )  # pylint: disable=C0103
    ax.set_aspect("auto")

    # Extract data and coordinates from Xarray
    data = radar_2d[radar_product][0].max(axis=0).values
    lon = radar_2d["lon"].values
    lat = radar_2d["lat"].values

    # Plot data over base map
    contour = ax.contourf(
        lon, lat, data, cmap="pyart_NWSRef", levels=range(-10, 60), transform=proj, alpha=1
    )

    # Adicionar o mapa de base usando contextily
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs="EPSG:4326")

    # Configure axes
    ax.set_title(
        title, position=[0.01, 0.01], fontsize=11, color="white", backgroundcolor="black"
    )  # , fontweight='bold', loc="left"

    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.axis("off")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlim(lon.min(), lon.max())
    ax.set_ylim(lat.min(), lat.max())

    # Customize colorbar to show only the specified values on the center of each box
    cbar_ax = fig.add_axes([0.001, 0.5, 0.03, 0.3])  # [left, bottom, width, height]
    cbar = plt.colorbar(
        mappable=plt.cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        cax=cbar_ax,
        orientation="vertical",
    )
    cbar.set_ticks([int(value) + 2.5 for value in ordered_values])
    cbar.set_ticklabels([str(value) for value in ordered_values])
    cbar.ax.tick_params(size=0)
    cbar.ax.set_title(
        cbar_title, fontsize=12, fontweight="bold", pad=10, position=[2.2, 0.4]
    )  # left, height

    # plt.show()
    return fig


@task
def upload_file_to_storage(bucket_name: str, destination_blob_name: str, source_file_name: str):
    """
    Upload files to GCS
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    # Cria um blob (o arquivo dentro do bucket)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    log(f"File {source_file_name} sent to {destination_blob_name} on bucket {bucket_name}.")


@task(nout=2)
def get_storage_destination(filename: str, path: str):
    """
    get storage
    """
    destination_blob_name = f"cor-clima/radar/mendanha/{filename}.png"
    source_file_name = f"{path}/{filename}.png"
    log(f"destination_blob_name, source_file_name: {destination_blob_name}, {source_file_name}")
    return destination_blob_name, source_file_name


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


def list_soft_deleted_objects_with_prefix(bucket_name, prefix):
    """List soft-deleted objects with a specific prefix in a bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Listando objetos da pasta .zombie com o prefixo fornecido
    blobs = bucket.list_blobs(prefix=f".zombie/{prefix}")

    soft_deleted_files = []
    for blob in blobs:
        soft_deleted_files.append(blob.name)
        print(f"Soft-deleted object found: {blob.name}")

    return soft_deleted_files


def restore_object(bucket_name, soft_deleted_object_name):
    """Restores a soft-deleted object by moving it back to its original path."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Pegando o objeto soft-deleted
    blob = bucket.blob(soft_deleted_object_name)

    # Definindo o nome original do objeto removendo a parte ".zombie/"
    restore_to_name = soft_deleted_object_name.replace(".zombie/", "")

    # Criando o novo blob no local de restauração (local original)
    restored_blob = bucket.blob(restore_to_name)

    # Copiando o arquivo para o local original
    restored_blob.rewrite(blob)

    # Deletando o arquivo da pasta .zombie para concluir a restauração
    blob.delete()

    print(f"Object {soft_deleted_object_name} restored to {restore_to_name}")


def restore_files_with_prefix(bucket_name, prefix):
    """Restore all soft-deleted files starting with a specific prefix to their original path."""
    # Listar todos os arquivos soft-deleted que começam com o prefixo
    soft_deleted_files = list_soft_deleted_objects_with_prefix(bucket_name, prefix)

    for soft_deleted_file in soft_deleted_files:
        # Restaurar o arquivo para seu caminho original
        restore_object(bucket_name, soft_deleted_file)


@task
def prefix_to_restore():
    """escolher prefix"""
    # Exemplo de uso:
    bucket_name = "rj-escritorio-scp"
    prefix = "mendanha/odimhdf5/vol_d"  # Caminho para onde os arquivos serão restaurados
    lista = []
    for i in range(3, 8):
        lista.append(f"{prefix}/MDN240{i}")
    name = []
    for i in range(1, 32):
        name.append(f"{prefix}/MDN2408{str(i).zfill(2)}")

    name = [i for i in name if not i.endswith(("14", "15", "16", "17", "18", "19", "20", "21"))]
    print(name)
    lista = lista + name
    lista.append(f"{prefix}/MDN2409")
    for i in range(3, 8):
        lista.append(f"{prefix}/MDN.20240{i}")
    name = []
    for i in range(1, 32):
        name.append(f"{prefix}/MDN.202408{str(i).zfill(2)}")

    name = [i for i in name if not i.endswith(("14", "15", "16", "17", "18", "19", "20", "21"))]

    lista = lista + name
    lista.append(f"{prefix}/MDN.202409")
    for prefix_ in lista:
        # Restaurar todos os arquivos que começam com MDN
        restore_files_with_prefix(bucket_name, prefix_)
