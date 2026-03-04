"""
Miscellaneous utility functions
"""

import os
import importlib
import re
import time
import logging
import json
import datetime
from pprint import pformat
from urllib.parse import urlparse
import pandas
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from d3b_api_client_cli.config import config

DEFAULT_TABLE_BATCH_SIZE = 1000

logger = logging.getLogger(__name__)

LOCAL_HOSTS = {
    "localhost",
    "127.0.0.1",
}


def df_exists(df: pandas.DataFrame) -> bool:
    """
    Check that DF is a pandas DataFrame and not empty
    """
    return isinstance(df, pandas.DataFrame) and (not df.empty)


def timestamp():
    """
    Helper to create an ISO 8601 formatted string that represents local time
    and includes the timezone info.
    """
    # Calculate the offset taking into account daylight saving time
    # https://stackoverflow.com/questions/2150739/iso-time-iso-8601-in-python
    if time.localtime().tm_isdst:
        utc_offset_sec = time.altzone
    else:
        utc_offset_sec = time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    t = (
        datetime.datetime.now()
        .replace(tzinfo=datetime.timezone(offset=utc_offset))
        .isoformat()
    )

    return str(t)


def get_id(link_name, entity):
    """
    Extract a KF ID from the _links dict in an entity that was in a Dataservice
    response json
    """
    return entity.get("_links", {}).get(link_name, "").split("/")[-1]


def get_total(base_url, endpoint, study_id, params={}):
    """
    Get total entity count in Dataservice
    """
    params.update({"study_id": study_id, "limit": 1})
    url = "/".join(part.strip("/") for part in [base_url, endpoint])
    headers = {"Content-Type": "application/json"}

    resp = send_request("get", url, headers=headers, params=params)
    return resp.json()["total"]


def is_localhost(url):
    """
    Determine whether url is on localhost
    """
    url = url.strip("/")
    host = urlparse(url).netloc.split(":")[0]
    return (host in LOCAL_HOSTS) or (
        any([url.startswith(h) for h in LOCAL_HOSTS])
    )


def delete_safety_check(url, error_msg=None):
    """
    Check if the url is on localhost and raise an exception if it is. This
    method is used in delete operations where you want to protect against
    deletions on hosts other than localhost
    """
    DELETE_SAFETY_CHECK = os.environ.get("DWDS_DELETE_SAFETY_CHECK", True)
    if str(DELETE_SAFETY_CHECK).lower() == "false":
        return

    if is_localhost(url):
        # If localhost, we are allowed delete
        pass
    else:
        if not error_msg:
            error_msg = (
                f"❌ Cannot delete from {url} because env variable"
                f" DWDS_DELETE_SAFETY_CHECK=True. Resources that are not in"
                f" {LOCAL_HOSTS} will not be deleted. To disable safety check,"
                f" set DWDS_DELETE_SAFETY_CHECK=False in your environment"
            )
        raise Exception(error_msg)


def kf_id_to_global_id(kfid, replace_prefix=None):
    """Convert Kids First ID to Dewrangle global ID

    :param kfid: the KF ID to convert
    :type kfid: str
    :param replace_prefix: the char seq to replace the KF ID prefix with
    :type replace_prefix: str
    :rtype: str
    :returns: global ID
    """
    parts = str(kfid).lower().split("_")
    prefix = parts[0]
    rest = parts[-1]

    if replace_prefix:
        prefix = replace_prefix

    return "-".join([prefix, rest])


def global_id_to_kf_id(global_id):
    """
    Convert global ID format to KF ID format
    """
    return global_id.replace("-", "_").upper()


def update_in_dictlist(list_, updates, key, value):
    """
    Find a dict in the list by matching key and value. Update the
    found item with updates. Return the new updated list
    """
    new_list = []
    for item in list_:
        if item.get(key) == value:
            item.update(updates)
        new_list.append(item)

    return new_list


def dataservice_count(entity_type, filter_kwargs=None, request_kwargs=None):
    """
    Get count for dataservice url
    """
    cfg = config["dataservice"]
    base_url = cfg["api_url"]
    endpoint = cfg["endpoints"].get(entity_type)
    url = f"{base_url}{endpoint}"

    params = filter_kwargs or {}
    request_kwargs = request_kwargs or {}
    params["limit"] = 1
    resp = send_request("get", url, params=params, **request_kwargs)

    return resp.json()["total"]


def import_module_from_file(filepath):
    """
    Import a Python module given a filepath
    """
    module_name = os.path.basename(filepath).split(".")[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    imported_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(imported_module)
    return imported_module


def multisplit(string, delimiters):
    """Split a string by multiple delimiters.

    :param string: the string to split
    :type string: str
    :param delimiters: the list of delimiters to split by
    :type delimiters: list
    :return: the split substrings
    :rtype: list
    """

    regexPattern = "|".join(map(re.escape, delimiters))
    return re.split(regexPattern, string)


def camel_to_snake(value):
    """Convert CamelCase string to snake_case string

    :param value: The camel case value to convert
    :type value: str
    :returns: snake_cased string
    :rtype: str
    """
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)

    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", value).lower()


def snake_to_camel(value):
    """Convert snake_case string to CamelCase string

    :param value: The value to convert
    :type value: str
    :returns: CamelCased string
    :rtype: str
    """
    return "".join([w.title() for w in value.split("_")])


def elapsed_time_hms(start_time):
    """Get time elapsed since start_time in hh:mm:ss str format

    :param start_time: The starting time from which to calc elapsed time
    :type start_time: datetime.datetime obj
    :returns: a time string formatted as hh:mm:ss
    :rtype: str
    """
    elapsed = time.time() - start_time
    return time.strftime("%H:%M:%S", time.gmtime(elapsed))


def requests_retry_session(
    session=None,
    retries=3,
    backoff_factor=0.3,
    allowed_methods=frozenset({"GET"}),
    status_forcelist=(500, 502, 503, 504),
):
    """
    Requests session that retries on recoverable errors

    See https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def send_request(method, *args, ignore_status_codes=None, **kwargs):
    """Send http request. Raise exception on status_code >= 300

    :param method: name of the requests method to call
    :type method: str
    :raises: requests.Exception.HTTPError
    :returns: requests Response object
    :rtype: requests.Response
    """
    if isinstance(ignore_status_codes, str):
        ignore_status_codes = [ignore_status_codes]

    # NOTE: Set timeout so requests don't hang
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    if not kwargs.get("timeout"):
        # connect timeout, read timeout
        kwargs["timeout"] = (6.05, 120)
    # If timeout is negative, remove it so there is no timeout limit
    elif kwargs.get("timeout") < 0:
        kwargs.pop("timeout", None)
    else:
        logger.info(
            f"⌚️ Applying user timeout: {kwargs['timeout']} (connect, read)"
            " seconds to request"
        )

    # Use retries if the user specifies it
    use_retries = kwargs.pop("use_retries", False)
    session = kwargs.pop("session", None)
    if use_retries:
        requests_op = getattr(
            session or requests_retry_session(), method.lower()
        )
    else:
        requests_op = getattr(requests, method.lower())

    status_code = 0
    try:
        resp = requests_op(*args, **kwargs)
        status_code = resp.status_code
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if ignore_status_codes and (status_code in ignore_status_codes):
            pass
        else:
            body = ""
            try:
                body = pformat(resp.json())
            except Exception:
                body = resp.text

            kwargs["use_retries"] = use_retries
            msg = (
                f"❌ Problem sending {method} request to server\n"
                f"{str(e)}\n"
                f"args: {args}\n"
                f"kwargs: {pformat(kwargs)}\n"
                f"{body}\n"
            )
            logger.error(msg)
            raise e

    return resp


def read_json(filepath, default=None):
    """
    Read JSON file into Python dict. If default is not None and the file
    does not exist, then return default.

    :param filepath: path to JSON file
    :type filepath: str
    :param default: default return value if file not found, defaults to None
    :type default: any, optional
    :returns: your data
    :rtype: dict
    """
    if (default is not None) and (not os.path.isfile(filepath)):
        return default

    with open(filepath, "r") as json_file:
        return json.load(json_file)


def write_json(data, filepath, **kwargs):
    r"""
    Write Python data to JSON file.

    :param data: your data
    :param filepath: where to write your JSON file
    :type filepath: str
    :param \**kwargs: keyword arguments to pass to json.dump
    :returns: None
    """
    if "indent" not in kwargs:
        kwargs["indent"] = 4
    if "sort_keys" not in kwargs:
        kwargs["sort_keys"] = False
    with open(filepath, "w") as json_file:
        json.dump(data, json_file, **kwargs)
