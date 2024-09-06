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
import matplotlib.colors as mcolors
import numpy as np
import pyart


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


def open_radar_file(file) -> pyart.core.Radar:
    """
    Print file size if it has problem opening it
    """
    try:
        opened_file = pyart.aux_io.read_odim_h5(file)
        return opened_file
    except OSError as e:
        print(f"Erro ao abrir o arquivo: {e}")
        file_size = os.path.getsize(file)
        print(f"Tamanho do arquivo: {file_size} bytes")
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
