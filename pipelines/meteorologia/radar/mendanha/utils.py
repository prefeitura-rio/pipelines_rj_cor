# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0302
"""
General utils for setting rain dashboard using radar data.
"""
import re
from datetime import datetime
from google.cloud import storage


def extract_timestamp(filename):
    """
    Get timestamp from filename
    """
    match = re.search(r"(\d{8}T\d{6}Z|\d{8}\d{6})", filename)
    if not match:
        match = re.search(r"(\d{12})", filename)
    return datetime.strptime(
        match.group(), "%Y%m%dT%H%M%SZ" if "T" in match.group() else "%y%m%d%H%M%S"
    )


def list_files_storage(bucket, prefix: str) -> list:
    """List files from bucket"""
    blobs = list(bucket.list_blobs(prefix=prefix))
    files = [blob.name for blob in blobs if blob.name.endswith(".hdf")]
    sorted_files = sorted(files, key=extract_timestamp)
    return sorted_files


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    return blobs


def download_blob(bucket_name, source_blob_name, destination_file_name) -> None:
    """
    Downloads a blob mencioned on source_blob_name from bucket_name
    and save it on destination_file_name.
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to file path {destination_file_name}. successfully ")
