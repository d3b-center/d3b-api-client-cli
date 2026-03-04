"""
Miscellaneous utility functions for dealing with data
"""

import os
import re
from itertools import zip_longest
from pprint import pprint
from inspect import signature
from collections import defaultdict
import logging

import requests
import pandas

from d3b_api_client_cli.config import config

DEFAULT_TABLE_BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


def read_df(filepath, cleanup=True):
    """
    Read table in from disk and safely convert values to strings

    This is necessary since pandas does not do a good job coercing values to
    their correct types

    NaN convert to None
    All other values get converted to strings
    """
    fp = filepath
    fn = os.path.split(fp)[-1]
    df = None

    logger.info(f"📚  Reading data for {fn} ...")
    df = pandas.read_csv(fp, index_col=False)

    if cleanup:
        df = clean_up_df(df)
        logger.info(f"🧼 Cleaning up dataframe {fn}, m x n: {df.shape}")

    return df


def write_df(df, filepath, msg=None, cleanup=False, **kwargs):
    """
    Write Pandas Dataframe to disk. Optionally call clean_up_df on it before
    writing it to file
    """
    fn = os.path.split(filepath)[-1]
    if cleanup:
        logger.info(f"🧼 Cleaning up dataframe {fn} before write")
        df = clean_up_df(df)

    df.to_csv(filepath, index=False, **kwargs)


def df_exists(df):
    """
    Check that DF is a pandas DataFrame and not empty
    """
    return isinstance(df, pandas.DataFrame) and (not df.empty)


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
    for i, chunk in enumerate(
        pandas.read_csv(filepath, chunksize=batch_size, **read_csv_kwargs)
    ):
        nrows = chunk.shape[0]
        count += nrows
        logger.debug(f"Reading {nrows} rows of {count} seen")
        yield chunk


def merge_wo_duplicates(left, right, left_name=None, right_name=None, **kwargs):
    """
    Merge two dataframes and return a dataframe with no duplicate columns.

    If duplicate columns result from the merge, resolve duplicates by
    filling nans on the left column with values from the right column.

    :param left: left dataframe
    :type left: Pandas.DataFrame
    :param left_name: Optional name of left DataFrame to use in logging
    the DataFrame's uniques using nunique()
    :type left_name: str
    :param right_name: Optional name of right DataFrame to use in logging
    the DataFrame's uniques using nunique()
    :type right_name: str
    :param right: right dataframe
    :type right: Pandas.DataFrame
    :param kwargs: keyword args expected by Pandas.merge function
    """
    left = left.astype(object)
    right = right.astype(object)
    left_name = left_name or "Left"
    right_name = right_name or "Right"

    # Check if merge col(s) are present in DataFrame
    def check_merge_col(merge_on, df_name, df, err_msgs):
        if isinstance(merge_on, str):
            merge_on = [merge_on]
        for col in merge_on:
            if col not in df.columns:
                err_msgs.append(f"'{col}' not found in {df_name}: {df.columns}")
        return err_msgs

    err = []
    if "on" in kwargs:
        on = kwargs["on"]
        err = check_merge_col(on, left_name, left, err)
        err = check_merge_col(on, right_name, right, err)
    elif ("left_on" in kwargs) and ("right_on" in kwargs):
        err = check_merge_col(kwargs["left_on"], left_name, left, err)
        err = check_merge_col(kwargs["right_on"], right_name, right, err)
    else:
        err = [
            (
                "Missing merge column keyword argument(s). Must supply either `on`"
                " or both `left_on` and `right_on` arguments."
            )
        ]

    if err:
        raise Exception("\n".join(err))

    def resolve_duplicates(df, suffixes):
        l_suffix = suffixes[0]
        r_suffix = suffixes[1]

        while True:
            to_del = set()
            for coll in df.columns:
                if coll.endswith(l_suffix):
                    firstpart = coll.split(l_suffix)[0]
                    colr = firstpart + r_suffix
                    inconsistent = (
                        (df[coll] != df[colr])
                        & df[coll].notna()
                        & df[colr].notna()
                    )
                    if any(inconsistent):
                        raise Exception(
                            "Inconsistent data between left and right DFs.\n"
                            f"Kwargs: {kwargs}\n"
                            f"Left side was:\n{left}\n"
                            f"Right side was:\n{right}\n"
                            f"Intermediate was:\n{df}\n"
                            f"Merge collision between: {coll} and {colr}\n"
                            "Mismatching values:\n"
                            f"{df[[coll, colr]][inconsistent]}"
                        )
                    df[firstpart] = df[coll].fillna(df[colr])
                    to_del.update([coll, colr])
            if not to_del:
                break
            else:
                for c in to_del:
                    del df[c]
        return df

    merged = pandas.merge(left, right, **kwargs)
    reduced = resolve_duplicates(merged, kwargs.pop("suffixes", ("_x", "_y")))

    default_how = signature(pandas.merge).parameters["how"].default

    # Hopefully this will help us know that we didn't lose anything important
    # in the merge
    collective_uniques = defaultdict(set)
    for c in left.columns:
        collective_uniques[c] |= set(left[c])
    for c in right.columns:
        collective_uniques[c] |= set(right[c])
    collective_uniques = pandas.DataFrame(
        {c: [len(v)] for c, v in collective_uniques.items()}
    )
    msg = (
        f'*** {kwargs.get("how", default_how).title()} merge {left_name} with '
        f"{right_name}***\n"
        f"-- Left+Right Collective Uniques --\n{collective_uniques}\n"
        f"-- Merged DataFrame Uniques --\n{reduced.nunique()}"
    )
    logger.debug(msg)

    return reduced


def convert_to_downcasted_str(val, replace_na=False, na=None):
    """
    Converts values to stripped strings while collapsing downcastable floats.

    Examples:
        to_str_with_floats_downcast_to_ints_first(1) -> "1"
        to_str_with_floats_downcast_to_ints_first(1.0) -> "1"
        to_str_with_floats_downcast_to_ints_first("1_1  ") -> "1_1"
        to_str_with_floats_downcast_to_ints_first(None) -> None
        to_str_with_floats_downcast_to_ints_first(None, True, "") -> ""

    If you're wondering what this is good for, try the following:
        import pandas
        df1 = pandas.DataFrame({"a":[1,2,3,None]}, dtype=object)
        df2 = pandas.read_json(df1.to_json(), dtype=object)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns False. Yuck.
        df1 = df1.map(to_str_with_floats_downcast_to_ints_first)
        df2 = df2.map(to_str_with_floats_downcast_to_ints_first)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns True. Good.

    :param val: any basic type
    :param replace_na: should None/NaN/blank values be replaced with something
    :type replace_na: boolean
    :param na: if replace_na is True, what should None/NaN/blank values be
        replaced with


    :return: new representation of `val`
    """
    if isinstance(val, list):
        # make hashable without changing style or losing comparability
        return str(sorted(convert_to_downcasted_str(v) for v in val))
    if isinstance(val, dict):
        # make hashable without changing style or losing comparability
        return str(
            dict(
                sorted(
                    (k, convert_to_downcasted_str(v)) for k, v in val.items()
                )
            )
        )
    if pandas.isnull(val):
        if replace_na:
            return na
        else:
            return val

    val = str(val).strip()
    if val != "":
        # Try downcasting val
        i_val = None
        f_val = None
        try:
            f_val = float(val)
            i_val = int(f_val)
        except Exception:
            pass

        # Don't automatically change anything with leading zeros
        # (except something that equates to int 0), scientific
        # notation, or underscores (I don't care what PEP 515 says).
        if (i_val != 0) and (
            (val[0] == "0") or (not re.fullmatch(r"[\d.]+", val))
        ):
            return val

        # Return str version of downcasted val
        if i_val == f_val:
            return str(i_val)

    elif replace_na:
        return na

    return val


def str_to_obj(var):
    """
    Convert a string that looks like a list, dict, tuple, or bool back into its
    native object form.
    """
    if not isinstance(var, str):
        return var
    elif var.startswith(("[", "{", "(")):
        try:
            return ast.literal_eval(var)
        except Exception:
            pass
    else:
        lowvar = var.strip().lower()
        if lowvar == "false":
            return False
        elif lowvar == "true":
            return True
    return var


def recover_containers_from_df_strings(df):
    """
    Undo one bit of necessary madness imposed by clean_up_df where lists and
    dicts (both unhashable) are sorted and then converted to strings for
    safekeeping. This finds strings that look like lists, dicts, or tuples and
    converts them back to their native forms.

    :param df: a pandas DataFrame
    :return: Dataframe with object-like strings converted to native objects
    :rtype: DataFrame
    """
    return df.map(str_to_obj)


def clean_up_df(df):
    """
    We can't universally control which null type will get used by a data
    file loader, and it might also change, so let's always push them all
    to None because other nulls are not our friends. It's easier for a
    configurator to equate empty spreadsheet cells with None than e.g.
    numpy.nan.

    Typed loaders like pandas.read_json force us into storing numerically
    typed values. And then nulls, which read_json does not let you handle
    inline, cause pandas to convert perfectly good ints into ugly floats.
    So here we get any untidy values back to nice and tidy strings.

    :param df: a pandas DataFrame
    :return: Dataframe with numbers converted to strings and NaNs/blanks
        converted to None
    :rtype: DataFrame
    """

    return df.map(
        lambda x: convert_to_downcasted_str(x, replace_na=True, na=None)
    ).drop_duplicates()
