"""
I/O Utilities for File Handling

This module provides utility functions for handling file operations,
including downloading files from S3, parsing S3 paths, and loading data
into pandas DataFrames.

It is intended to be used as part of the data transfer pipeline for managing
manifest files and other related resources.
"""

import logging
import os
from os import path, scandir
from pprint import pformat
from typing import Callable
from urllib.parse import urlparse

import json
import pandas as pd
import requests

from d3b_api_client_cli.config import config


logger = logging.getLogger(__name__)

DEFAULT_TABLE_BATCH_SIZE = 1000
TIMEOUT_INFINITY = -1


def get_file_extension(file_path: str) -> str:
    """Get the file extension from a file path."""
    return path.splitext(file_path)[
        1
    ]  # Returns the file extension, e.g., '.txt'


def get_list_of_local_files_in_dir(dir_path: str) -> list[str]:
    """Get List of Files from Directory local"""
    return [f.name for f in scandir(path=dir_path) if f.is_file()]


def read_json(filepath: str, default: any = None):
    """
    Read JSON file into Python dict. If default is not None and the file
    does not exist, then return default.
    """
    if (default is not None) and (not os.path.isfile(filepath)):
        return default

    with open(filepath, "r") as json_file:
        return json.load(json_file)


def write_json(data: dict, filepath: str, **kwargs):
    """
    Write Python data to JSON file.
    """
    if "indent" not in kwargs:
        kwargs["indent"] = 2
    if "sort_keys" not in kwargs:
        kwargs["sort_keys"] = False
    with open(filepath, "w") as json_file:
        json.dump(data, json_file, **kwargs)


def chunked_dataframe_reader(
    filepath, batch_size=DEFAULT_TABLE_BATCH_SIZE, **read_csv_kwargs
):
    """
    Return a generator to iterate over the Dataframe rows in batches

    :param filepath: Path to the tabular file
    :type filepath: str
    :param batch_size: Number of rows to read from the df in one iteration
    :type batch_size: int
    :param read_csv_kwargs: pandas.read_csv kwargs
    :type read_csv_kwargs: dict
    :yields: pandas.Dataframe
    """
    read_csv_kwargs.pop("chunksize", None)
    count = 0
    for _, chunk in enumerate(
        pd.read_csv(filepath, chunksize=batch_size, **read_csv_kwargs)
    ):
        nrows = chunk.shape[0]
        count += nrows
        logger.debug("Reading %s rows of %d seen", nrows, count)
        yield chunk


def send_request(
    method: str,
    *args: any,
    ignore_status_codes: list[str] = None,
    timeout=TIMEOUT_INFINITY,
    **kwargs: any,
) -> requests.Response:
    """
    Send http request. Ignore any status codes that the user provides. Any
    other status codes >300 will result in an HTTPError

    Arguments:
        method: name of HTTP request method (i.e. get, post, put)
        *args: positional arguments passed to request method
        ignore_status_codes: list of HTTP status codes to ignore in the
        response
        **kwargs:

    Returns:
        Resonse object from HTTP method called by requests

    Raises:
        requests.exceptions.HTTPError
    """
    if isinstance(ignore_status_codes, str):
        ignore_status_codes = [ignore_status_codes]

    # NOTE: Set timeout so requests don't hang
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    if not timeout:
        # connect timeout, read timeout
        kwargs["timeout"] = (6.05, 120)

    # If timeout is negative, set to None so there is no timeout limit
    elif timeout == TIMEOUT_INFINITY:
        kwargs["timeout"] = None

    logger.info(
        "⌚️ Applying timeout: %s (connect, read)" " seconds to request", timeout
    )

    # Get http method
    requests_op = getattr(requests, method.lower())
    status_code = 0
    try:
        resp = requests_op(*args, **kwargs)
        status_code = resp.status_code
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # User said to ignore this status code so pass
        if ignore_status_codes and (status_code in ignore_status_codes):
            pass

        # Error that we need to log and raise
        else:
            body = "No request body found"
            try:
                body = pformat(resp.json())
            except json.JSONDecodeError:
                body = resp.text

            raise requests.exceptions.HTTPError(
                f"❌ Problem sending {method} request to server\n"
                f"{str(e)}\n"
                f"args: {args}\n"
                f"kwargs: {pformat(kwargs)}\n"
                f"{body}\n"
            ) from e

    return resp
