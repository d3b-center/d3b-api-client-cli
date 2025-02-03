"""
Miscellaneous Utility Functions

This module provides utility functions for common tasks that assist in various parts
of the data transfer pipeline. These utilities include checking DataFrame validity
and generating ISO 8601-compliant timestamps with timezone information.

Key Functions:
--------------
1. **df_exists**:
   - Verifies that a pandas DataFrame exists and is not empty.
   - Parameters:
     - `df` (pandas.DataFrame): The DataFrame to check.
   - Returns:
     - `bool`: `True` if `df` is a valid, non-empty DataFrame; `False` otherwise.

2. **timestamp**:
   - Generates a timestamp in ISO 8601 format, representing the current local time
     with timezone information.
   - Returns:
     - `str`: A string representation of the current local time, e.g.,
       `"2024-11-21T15:34:45-05:00"`.

"""

import time
from urllib.parse import urlparse
import datetime

import pandas

LOCAL_HOSTS = {
    "localhost",
    "127.0.0.1",
}


def df_exists(df: pandas.DataFrame) -> bool:
    """
    Check that DF is a pandas DataFrame and not empty
    """
    return isinstance(df, pandas.DataFrame) and (not df.empty)


def timestamp() -> str:
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


def is_localhost(url: str) -> bool:
    """
    Determine whether url is on localhost
    """
    url = url.strip("/")
    host = urlparse(url).netloc.split(":")[0]
    return (host in LOCAL_HOSTS) or (
        any([url.startswith(h) for h in LOCAL_HOSTS])
    )


def delete_safety_check(url: str, error_msg: str = None) -> None:
    """
    Check if the url is on localhost and raise an exception if it is.

    This method is used in delete operations where you want to protect against
    deletions on hosts other than localhost
    """
    if is_localhost(url):
        # If localhost, we are allowed delete
        pass
    else:
        if not error_msg:
            error_msg = (
                f"❌ Cannot delete from {url} because env variable"
                " DELETE_SAFETY_CHECK=True. Resources that are not in"
                f" {LOCAL_HOSTS} will not be deleted. To disable safety check,"
                " set DELETE_SAFETY_CHECK=False in your environment"
            )
        raise ValueError(error_msg)


def kf_id_to_global_id(kf_id: str, replace_prefix: str = None) -> str:
    """
    Convert Kids First ID to Dewrangle global ID

    Example

    KF_ID: SD_ME0WME0W -> Dewrnagle global id: sd-me0wme0w
    """
    parts = str(kf_id).lower().split("_")
    prefix = parts[0]
    rest = parts[-1]

    if replace_prefix:
        prefix = replace_prefix

    return "-".join([prefix, rest])


def global_id_to_kf_id(global_id: str) -> str:
    """
    Convert Dewrangle global ID format to Kids First ID format

    Example

    Dewrnagle global id: sd-me0wme0w -> KF_ID: SD_ME0WME0W
    """
    return global_id.replace("-", "_").upper()


def elapsed_time_hms(start_time: float) -> str:
    """
    Gets the time elapsed since `start_time` in hh:mm:ss string format.

    Args:
        start_time (datetime.datetime): The starting time from which to calculate the elapsed time.

    Returns:
        str: A time string formatted as hh:mm:ss.
    """
    elapsed = time.time() - start_time
    return time.strftime("%H:%M:%S", time.gmtime(elapsed))
