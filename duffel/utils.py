from typing import Iterable, IO
import csv
import json

from .na import NA
from .df import _DuffelDataFrame
from .col import _DuffelCol
from . import base_utils


def _isna(val):
    pass


def _notna(val):
    pass


def _fillna(data):
    pass


def _dropna(data):
    pass


def _merge(data, on, how="inner"):
    pass


def _concat(values, axis=0, ignore_index=False):
    """
    uses base_utils _base_concat
    """
    data, index, columns = base_utils._base_concat(values, axis, ignore_index)
    return _DuffelDataFrame(data, index=index, columns=columns)


def _asnumeric(obj):
    try:
        return int(obj)
    except ValueError:
        pass

    try:
        return float(obj)
    except ValueError:
        pass

    if obj in ("True", "true", "TRUE"):
        return True
    elif obj in ("False", "false", "FALSE"):
        return False
    elif obj == "":
        return NA
    else:
        return obj


def _read_csv(
    reader,
    header=True,
    skiprows=0,
    numeric=True,
    columns=None,
    index=None,
    index_col=None,
):
    """
    Reads a file in as a DataFrame.

    :param reader: A file handle or filename.
    :param headers: True if headers are on the first line of data, false otherwise.
    :param skiprows: Skip this number of rows before reading data.
    :param numeric: True if data should be converted to numeric (if possible).
    :return: A DataFrame with the resulting data.
    """
    fname = None
    if type(reader == "str"):
        # is a filename
        if not isinstance(reader, str):
            raise ValueError("Reader parameter is not an open file or a filename")
        fname = reader
        freader = open(fname, "r")

    csvreader = csv.reader(freader)
    records = []
    detected_columns = None
    for line in csvreader:
        if skiprows > 0:
            skiprows -= 1
            continue
        if not records and not detected_columns:
            # look for data
            if any([bool(c) for c in line]):
                if header:
                    detected_columns = line
                else:
                    if numeric:
                        records.append([_asnumeric(c) for c in line])
                    else:
                        records.append(line)
            else:
                # no data
                continue
        else:
            # already a df
            if numeric:
                records.append([_asnumeric(c) for c in line])
            else:
                records.append(line)
    if fname:
        freader.close()
    if columns is None:
        columns = detected_columns

    #
    # TODO - known issue
    # sometimes you want to specify index values AND specify index_col that the given index should replace
    # currently you can't specify an index and an index_col;
    # to fix this, I need pop the index col values as I would have before, then pass the index values to the DataFrame constructor
    # I also need to add error checking to make sure there are >= index_col columns, and also normal index checking
    #

    # deal with index col by popping values that don't belong
    if index_col is not None:
        assert (
            index is None
        ), f"duffel.read_csv must specify either index_col OR index values, not both"
        assert isinstance(
            index_col, int
        ), f"DF index_col must be an interger; index col was ({index_col})"
        index = [x.pop(index_col) for x in records]
        if columns is not None:
            index_col_name = columns.pop(index_col)
        else:
            index_col_name = None
    else:
        index_col_name = None

    return _DuffelDataFrame(
        records, columns=columns, index=index, _index_name=index_col_name
    )


def _read_json(path_or_buf, orient: str = "dict", typ: str = "frame"):
    """
    reads the file or buffer to dict
    returns a DataFrame based on the orient
    """
    # parameter checking
    assert isinstance(path_or_buf, IO) or isinstance(
        path_or_buf, str
    ), f"duffel.read_json only accepts path-like or buffer-like objects, not {type(path_or_buf)}"
    
    assert typ in (
        "frame",
        "series",
    ), f"duffel.read_json typ must be 'frame' or 'series', not {typ}"
    
    assert orient in {
        "dict",
        "records",
        "index",
        "split",
        "series",
        "list",
    }, f"duffel.read_json orient must be in ('dict','records','index','split','series', 'list'), not {orient}"

    # set up to read into either a series or a dataframe
    typ_d = {"series": _DuffelCol, "frame": _DuffelDataFrame}

    # finish up
    if isinstance(path_or_buf, IO):
        return typ_d[typ](json.load(path_or_buf))

    elif isinstance(path_or_buf, str):
        return typ_d[typ](json.load(open(path_or_buf)))


def read_sql(q, con):
    pass
