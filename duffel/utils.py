from typing import Iterable
import csv
from .na import NA
from .df import _DuffelDataFrame



def _isna(val):
    pass

def _notna(val):
    pass

def _fillna(data):
    pass

def _dropna(data):
    pass

def _merge(data,on,how='inner'):
    pass

def _concat(values,axis=0, ignore_index=False):
    '''
    standard join is outer - this is supported first
    only able to do axis 0 and 1
    ignore index counts for columns and for index
    '''
    
    # must be duffel objects
    assert isinstance(values, Iterable), "concat values must be a list or iterable"
    for item in values:
        assert any( (type(item).__name__=='_DuffelDataFrame', type(item).__name__=='_DuffelCol')), f"duffel.concat values must be type duffel.col.Col or duffel.df.DataFrame, not {type(item)}"
    
    # check index overlap
    if not ignore_index:
        indexes = [ i for y in [x.index for x in values] for i in y ]
        set_indexes = set([ i for y in [x.index for x in values] for i in y ])
        assert len(indexes) == len(set_indexes), "concat indexes must be unique if ignore_index=False"
    else:
        pass

    # create set of columns
    columns = []
    for x in values:
        if hasattr(x,'columns'): # is dataframe
            columns = columns + [c for c in x.columns if not c in columns]
        else: # is column
            columns = columns + [c for c in [x.name] if not c in columns]

    # concat the data
    data = {col:[] for col in columns}
    for x in values:
        # is dataframe
        if hasattr(x,'columns'):
            for col in columns:
                if col in x.columns:
                    data[col].extend([row[x._rep_columns[col]] for row in x.values])
                else:
                    data[col].extend([None for row in range(x._nrow)])
        # is dataframe  
        else:
            if col == x.name:
                data[col].append([val for val in x.values])
            else:
                data[col].extend([None for row in range(x._nrow)])

    nrow = len(data[ list(data.keys())[0] ])
        
    # create index
    if ignore_index==True:
        index = list(range(nrow))
    else:
        index = [i for y in [x.index for x in values] for i in y]
        assert len(set(index))==nrow, "concat index values must be unique if ignore_index=False"
        
    # print([len(data[x]) for x in columns],index,columns)
    
    # finish up
    return _DuffelDataFrame(
        data,
        index=index,
        columns = columns
    )





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
            columns.pop(index_col)

    return _DuffelDataFrame(records, columns=columns, index=index)


def read_json(reader):
    pass


def read_sql(q, con):
    pass
