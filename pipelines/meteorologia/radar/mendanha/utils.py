# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0302
"""
General utils for setting rain dashboard using radar data.
"""
import base64
import io
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Union

import h5py
import matplotlib.colors as mcolors
import numpy as np
import pyart  # pylint: disable=E0611, E0401

from prefeitura_rio.pipelines_utils.logging import log


def extract_timestamp(filename) -> datetime:
    """
    Get timestamp from filename
    """
    match = re.search(r"(\d{8}T\d{6}Z|\d{8}\d{6})", filename)
    if not match:
        match = re.search(r"(\d{12})", filename)
    return datetime.strptime(
        match.group(), "%Y%m%dT%H%M%SZ" if "T" in match.group() else "%y%m%d%H%M%S"
    )


def eliminate_nan_times(filename):
    """
    Eliminate nan times inside file and try to reopen it
    """
    with h5py.File(filename, "a") as hfile:  # Use "a" mode to allow updates
        datasets = [k for k in hfile if k.startswith("dataset")]
        datasets.sort(key=lambda x: int(x[7:]))

        for dataset in datasets:
            how_group = hfile[dataset]["how"]

            # Get startazT and stopazT as numpy arrays
            startazT = np.array(how_group.attrs["startazT"])
            stopazT = np.array(how_group.attrs["stopazT"])

            if np.isnan(startazT).any() or np.isnan(stopazT).any():

                # Step 1: Remove NaN positions from stopazT based on NaNs in startazT
                valid_start_indices = ~np.isnan(startazT)
                startazT = startazT[valid_start_indices]
                stopazT = stopazT[valid_start_indices]
                # log("valid_start_ind", valid_start_indices.shape, startazT.shape, stopazT.shape)

                # Step 2: Remove NaN positions from startazT based on NaNs in stopazT
                valid_stop_indices = ~np.isnan(stopazT)
                startazT = startazT[valid_stop_indices]
                stopazT = stopazT[valid_stop_indices]
                # log("valid_stop_indices", valid_stop_indices.shape, startazT.shape, stopazT.shape)

                # Our array must have 360 positions, lets duplicated firsts elements considering the
                # total amount of nans found
                total_nans = 360 - np.isnan(startazT).shape[0]
                startazT = np.concatenate((startazT, startazT[:total_nans]))
                stopazT = np.concatenate((stopazT, stopazT[:total_nans]))

                log("after", startazT.shape, stopazT.shape)

                # Update the attributes in the file
                how_group.attrs["startazT"] = startazT
                how_group.attrs["stopazT"] = stopazT
                log(f"Updated dataset {dataset}: startazT and stopazT cleaned.")


def _read_file(file_path):
    """
    Abre o arquivo usando pyart e retorna o conteúdo.
    """
    return pyart.aux_io.read_odim_h5(file_path)


def _handle_value_error(file_path):
    """
    Executa ações corretivas para ValueError.
    """
    print(f"Handling ValueError for: {file_path}")
    eliminate_nan_times(file_path)


def _handle_os_error(file_path):
    """
    Trata erros de OSError, como imprimir tamanho do arquivo.
    """
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size} bytes")
    else:
        print("File not found.")


def open_radar_file(file_path: Union[str, Path]) -> Union[pyart.core.Radar, None]:
    """
    Open radar file with h5 extension.

    If file is compressed as a gzip file, it will be decompressed
    before being opened. Print file size if it has problem opening it.

    Parameters
    ----------
    file_path : Union[str, Path]
        Path to the file.

    Returns
    -------
    radar : pyart.core.Radar
        Radar object.
    """
    file_path = str(file_path)
    # if file_path.endswith(".gz"):
    #     uncompressed_file_path = file_path[:-3]
    #     with gzip.open(file_path, "rb") as f_in:
    #         with open(uncompressed_file_path, "wb") as f_out:
    #             shutil.copyfileobj(f_in, f_out)
    #     file_path = uncompressed_file_path

    try:
        print(f"Trying to open file: {file_path}")
        return _read_file(file_path)
    except ValueError as value_error:
        print(f"Value Error when opening {file_path}: {value_error}")
        _handle_value_error(file_path)
        return _read_file(file_path)
    except OSError as os_error:
        print(f"OS Error when opening: {os_error}")
        _handle_os_error(file_path)
        return None


def create_colormap():
    """
    Create colormap to match the same color as they used before on colorbar
    """
    print("Start creating color map")
    colors_hex = {
        "50": "#d11fcc",
        "45": "#f61c00",
        "40": "#ff7800",
        "35": "#c3d500",
        "30": "#004803",
        "25": "#0c6b11",
        "20": "#069008",
    }

    # Order colors based on its intensity
    ordered_values = sorted(colors_hex.keys(), reverse=False)
    ordered_colors = [colors_hex[value] for value in ordered_values]

    # Criate colormap usign defined colors
    cmap = mcolors.ListedColormap(ordered_colors)

    # Define values limits (norm)
    norm = mcolors.BoundaryNorm(
        boundaries=np.linspace(20, 55, len(colors_hex) + 1), ncolors=cmap.N, clip=True
    )
    return cmap, norm, ordered_values


def save_image_to_local(filename: str, img, path="temp") -> None:
    """
    Save image in a PNG file
    """
    print(f"Saving image {filename} to local")
    if isinstance(img, str):
        img_data = base64.b64decode(img)
    elif isinstance(img, io.BytesIO):
        img.seek(0)  # Certifique-se de que o ponteiro esteja no início
        img_data = img.read()  # Obtenha os bytes do BytesIO
    elif isinstance(img, bytes):
        img_data = img

    # Salvar a imagem em um arquivo PNG
    if not os.path.exists(path):
        os.makedirs(path)
    with open(f"{path}/{filename}", "wb") as img_file:
        img_file.write(img_data)
