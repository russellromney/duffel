from typing import Iterable, Mapping, Optional, List
from functools import reduce
from collections import Counter
import random
import json
import csv

from .na import ndim, NA
from .row import _DuffelRow
from .col import _DuffelCol
from .loc import _Loc, _ILoc
from . import base_utils


class _DuffelDataFrame:
    def __init__(self, values, columns=None, index=None, **kwargs):
        # ingest an existing dataframe

        if columns is not None:
            self.columns = tuple(list(columns))  # throws error if columns not iterator
        else:
            self.columns = columns
        self.empty = False

        # ingest values (all values will be iterable)
        assert isinstance(values, Iterable), "DF values must be an iterable"

        if isinstance(values, Mapping):
            # dict of dicts
            if sum([isinstance(x, Mapping) for x in values.values()]) == len(values):
                # extract index and turn into list of dicts
                self.index = list(values.keys())
                values = list(values.values())
                self._ingest_iterable(values, columns=columns, index=index)
            else:
                # dict of iterables
                self._ingest_mapping(values, columns=columns, index=index)
        else:
            # iterable of dicts or iterable of iterables
            self._ingest_iterable(values, columns=columns, index=index)

        # create index
        if index is not None:
            assert isinstance(index, Iterable), "DF index must be an iterable"
            assert len(index) == len(set(index)), "DF index values must be unique"
            assert len(index) == len(
                self.values
            ), f"DF index length ({len(index)}) must match number of rows ({len(self.values)})"
            self.index = index
        elif hasattr(self, "index") and self.index is not None:
            # it was created by being a dict of dicts
            pass
        else:
            self.index = [x for x in range(len(self.values))]

        # named index
        if "_index_name" in kwargs and not kwargs["_index_name"] is None:
            self._index_name = kwargs["_index_name"]
        else:
            self._index_name = "index"

        # deal with columns
        if self.columns is not None:
            columns = self.columns
            assert isinstance(columns, Iterable), "DF columns must be an iterable"
            assert len(columns) == len(
                set(columns)
            ), f"DF columns values must be unique - duplicates are {[item for item, count in Counter(columns).items() if count > 1]}"
            assert len(columns) == len(
                self.values[0]
            ), f"DF columns length ({len(columns)}) must match length of values ({len(values[0])})"
            self.columns = tuple(columns)
        else:
            self.columns = tuple([x for x in range(len(self.values))])

        # create enumerated internal representation of index and columns
        self._get_rep_index()
        self._get_nrow()
        self._get_rep_columns()
        self._get_shape()

        # loc and iloc
        self.iloc = _ILoc(self)
        self.loc = _Loc(self)

    #####################################################################################
    # internals
    #####################################################################################

    def _ingest_iterable(self, values, columns=None, index=None):
        """ingest values of type Iterable (non-Mapping)"""
        # blank - make it 2d
        if not len(values):
            self.values = [[]]
            self.empty = True
        else:
            # put values into memory - iterator fix - this is inefficient, sure
            values = list(values)

            # dim 1
            if ndim(values) == 1:
                self.values = [values]

            # dim 2
            else:
                # iterable of iterables
                if isinstance(values[0], Iterable) and not isinstance(
                    values[0], Mapping
                ):
                    maxlen = max([len(x) for x in values])
                    self.values = [
                        [y for y in x] + [None] * (maxlen - len(x)) for x in values
                    ]

                # iterable of dicts
                elif isinstance(values[0], Mapping):
                    # keep the columns in order of first encounter
                    cols = reduce(
                        lambda l, x: l.append(x) or l if x not in l else l,
                        [y for x in values for y in list(x.keys())],
                        [],
                    )
                    self.columns = tuple(cols)
                    # create values
                    self.values = [[x.get(k, None) for k in cols] for x in values]

    def _ingest_mapping(self, values, columns=None, index=None):
        """
        ingest maps of iterables
        
        TODO: need to standardize the mapping ingest based on the orient types
            a) detect orientation or take as argument
            b) read in
            See to_dict for reference
        """
        if not len(values):
            self.values = [[]]
            self.empty = True
        else:
            self.columns = tuple(values.keys())

            # put scalar objects in single-len lists
            for col in values:
                if isinstance(values[col], Iterable):
                    values[col] = list(values[col])
                else:
                    values[col] = list([values[col]])

            # expand out smaller columns with None
            maxlen = max([len(values[col]) for col in values])
            for col in values:
                values[col] = values[col] + [None] * (maxlen - len(values[col]))

            # create values
            self.values = [list(x) for x in zip(*values.values())]

    def _row(self, i, columns=None):
        if i >= self._nrow:
            raise IndexError("Row index out of range: %s" % i)
        if columns is None:
            columns = self.columns
        return _DuffelRow(
            [val for col, val in zip(self.columns, self.values[i]) if col in columns],
            index=i,
            columns=columns,
        )

    def _col(self, column, rows=None):
        if rows is None:
            rows = self.index
        return _DuffelCol(
            [
                val
                for val in [
                    x
                    for ix, x in zip(
                        self.index,
                        [row[self._rep_columns[column]] for row in self.values],
                    )
                    if ix in rows
                ]
            ],
            name=column,
            index=rows,
        )

    def _invert_rep_index(self):
        return {v: k for k, v in self._rep_index.items()}

    def _invert_rep_columns(self):
        return {v: k for k, v in self._rep_columns.items()}

    def _subset_loc(self, rows, columns=None):
        """
        implement .loc indexing behavior
        transform rows to index values, transform columns to column names, and return subset
        """
        ### columns (goal is end up with list of column strings)
        # no column specified
        if columns is None:
            columns = self.columns
        # one column specified
        elif isinstance(columns, str):
            self._rep_columns[columns]  # this will throw a keyerror if column not exist
        elif isinstance(columns, int):
            columns = self._invert_rep_index()[columns]  # returns "real" value
            self._rep_columns[columns]  # this will throw a keyerror if column not exist
        # slice of columns by number
        elif isinstance(columns, slice):
            columns = self.columns[columns]
        # list of column names
        elif isinstance(columns, Iterable):
            [
                self._rep_columns[col] for col in columns
            ]  # missing columns throw an error
        # not the right type
        else:
            raise ValueError(
                "Must subset columns with slice, str, int, or iterable of column names"
            )

        ### rows
        if isinstance(rows, slice):
            # get list of row index values based on the slicer
            rows = self.index[rows]
        # rows is a string or an int - leave it be but check validity
        elif isinstance(rows, str) or isinstance(rows, int):
            self._rep_index[rows]  # throws keyerror if index doesn't exist
        # rows is a list of row index values
        elif isinstance(rows, Iterable):
            # if list of booleans
            if list(set([type(x) for x in rows])) == [bool]:
                thislen = len(rows)
                assert (
                    thislen == self._nrow
                ), f"Boolean subsetter length ({thislen}) must match length of data ({self._nrow})"
                rows = [i for truth, i in zip(rows, self.index) if truth]

            # list of row indexes
            else:
                [
                    self._rep_index[i] for i in rows
                ]  # throws keyerror if any index value doesn't exist
        else:
            raise ValueError(
                "Must subset rows with slice, index value, or iterable of index values"
            )

        ###
        # at this point:
        # rows is either a valid row index value or a list of valid index values
        # columns is either a valid column name or a list of valid column names
        #
        # this means we can freely use self._rep_columns and self._rep_index
        # to call integer index locations of each
        ###

        ### return subset
        # single row and col => return single value
        if ndim(rows) == 0 and ndim(columns) == 0:
            return self.values[self._rep_index[rows]][self._rep_columns[columns]]

        # single row, multiple columns => _DuffelRow
        elif ndim(rows) == 0 and ndim(columns) == 1:
            return self._row(rows, columns=columns)

        # multiple rows, one column => _DuffelCol
        elif ndim(rows) == 1 and ndim(columns) == 0:
            return self._col(columns, rows=rows)

        # multiple rows, multiple columns => _DuffelDataFrame
        elif ndim(rows) == 1 and ndim(columns) == 1:
            return _DuffelDataFrame(
                [
                    [
                        val
                        for include_col, val in zip(
                            [x in columns for x in self._rep_columns],
                            self.values[self._rep_index[row]],
                        )
                        if include_col
                    ]
                    for row in rows
                ],
                columns=columns,
                index=rows,
            )
        else:
            raise ValueError(
                f"Not sure how to .loc index for rows {rows} and cols {columns}"
            )

    def _subset_iloc(self, rows, columns=None):
        """
        implement .iloc indexing behavior
        only goal is to transform rows to row index values and pass to .loc
        """
        # a slice or int indexer can operate directly on the list of indexes
        if isinstance(rows, slice) or isinstance(rows, int):
            rows = self.index[rows]

        # if list of numbers, then check valid type and convert to index values
        elif isinstance(rows, Iterable):
            assert list(set([type(x) for x in rows])) == [
                int
            ], ".iloc rows must be integer or iterable of integers"
            inverted = self._invert_rep_index()
            rows = [inverted[i] for i in rows]

        # bad type
        else:
            raise ValueError(f".iloc rows must be integer or iterable of integers")

        # pass to .loc
        return self._subset_loc(rows, columns=columns)

    @classmethod
    def _from_dataframe(cls, df):
        """
        create a dataframe from another Dataframe (i.e. no copy)
        """
        if hasattr(df, "_index_name"):
            _index_name = df._index_name

        # this includes the columns and index
        copy = cls(df.to_dict(), _index_name=_index_name)

        # finish up
        return copy

    def _set_index(self, data: List, name=None):
        """
        internal function for setting the index
        change the index value, change the 
        """
        assert type(name) in (
            int,
            float,
            str,
        ), "DF column name must be int, float, or string"
        self.index = list(data)
        self._get_rep_index()
        self._index_name = name

    def _get_shape(self):
        self.shape = (self._nrow, len(self.columns))

    def _get_nrow(self):
        self._nrow = len(self.values)

    def _get_rep_columns(self):
        self._rep_columns = {k: v for v, k in enumerate(self.columns)}

    def _get_rep_index(self):
        self._rep_index = {k: v for v, k in enumerate(self.index)}

    #####################################################################################
    # interface
    #####################################################################################

    def iterrows(self):
        for i in range(self._nrow):
            yield i, self[i]

    def iteritems(self):
        for col in self.columns:
            yield col, self[col]

    def items(self):
        return self.iteritems()

    def sort_values(self, columns):
        # can sort with:
        # sorted(data, key=lambda x: (x[2], x[5]) ) sort ascending, ascending
        # sorted(data, key=lambda x: (x[2], -x[5]) ) sorts ascending, descending
        # maybe - how to include index:
        # add index as last element & sort and recreate index from last element and drop
        # sort dict of lists on some element and recreate index from keys (idk if this works)
        # NOTE - any same values in sorted column are then sorted by index i.e. by first appearence a la Python norms

        # method 1 append index value
        temp = [x + [i] for x, i in zip(self.values, self.index)]

        if ndim(columns) == 0:
            temp = sorted(temp, key=lambda x: (x[self._rep_columns[columns]]))
        else:
            temp = sorted(
                temp,
                key=lambda x: tuple(
                    [x[self._rep_columns[columns[i]]] for i in range(len(columns))]
                ),
            )
        self.index = [x[-1] for x in temp]
        self.values = [x[:-1] for x in temp]
        self._get_rep_index()
        return self

    def sort_index(self):
        # easy - just add index as an item, do sort_values, and re-map ._rep_index
        self.values = [x + [i] for i, x in zip(self.index, self.values)]
        new = "".join(
            [random.choice("1234567890abcdefghijklmnopqrstuvwxyz") for x in range(20)]
        )
        self.columns = [*self.columns, new]
        self._get_rep_columns()

        # sort the list of self._rep_index and then recreate with list comprehension including range()
        self = self.sort_values(new)

        # finish up
        self.index = [x[-1] for x in self.values]
        self._get_rep_index()
        self.columns = self.columns[:-1]
        self._get_rep_columns()
        return self

    def set_index(self, column):
        assert column in self.columns, "DF set_index column ({column}) not in columns"
        colindex = self._rep_columns[column]

        # create the data and edit the values by popping the values
        data = [x.pop(colindex) for x in self.values]

        # column ramifications
        self.columns = tuple([x for x in self.columns if x != column])
        self._get_rep_columns()

        # call the internal function
        self._set_index(data, column)
        return self

    def reset_index(self, drop: bool = False, name=None, index_name=None):
        # add a column if needed
        if not drop:
            assert name is None or type(name) in (
                int,
                float,
                str,
            ), "DF column name must be int, str, float"
            assert index_name is None or type(index_name) in (
                int,
                float,
                str,
            ), "DF index name must be int, str, float"

            # if a name is not specified, new column is the index name
            if name is None:
                name = self._index_name

            assert (
                not name in self.columns
            ), "DF index name must not overwrite an existing column"

            # actually change column
            self[name] = self.index

        # set actual index values
        if index_name is None:
            index_name = "index"
        self._set_index(list(range(self._nrow)), name=index_name)

        # edit columns
        self.columns = (*self.columns, index_name)
        self._get_rep_columns()

        # finish up
        self._get_shape()
        return self

    def transpose(self):
        # transpose values
        self.values = list(map(list, zip(*self.values)))

        # switch index and columns
        temp_columns = self.columns
        temp_index = self.index
        self.columns = tuple(temp_index)
        self.index = list(temp_columns)

        # finish up
        self._get_rep_index()
        return self

    def append(self, values: Iterable, index):
        """
        first [bad] implementation:
            only append one row to the end of the values
            index must be a unique value relative to current index

            adds index to _rep_index
            adds row to values
        """
        # # create a DataFrame out of the values to check len, type, etc.
        # tempdf = _DuffelDataFrame(values, index=index, columns=self.columns)
        # if index is not None or tempdf.index!=list(range(tempdf._nrow)):
        #     assert len(set(self.index)) == len(self.index)+len(index), "DF append index must be unique"

        # values length
        assert len(values) <= len(
            self.columns
        ), f"DF append values must container <= number of values as columns in DF; {len(values)} is too many"
        values = [*values, *[None for x in range(len(self.columns) - len(values))]]

        # index type validity
        assert isinstance(
            index, (int, float, str)
        ), f"DF append index value must be int, str, float, not {type(index)}"

        # index uniqueness
        assert (
            index not in self.index
        ), "DF append index value must be unique relative to DF index"

        # add to index
        self.index = [*self.index, index]
        self._get_rep_index()

        # add value
        self.values.append(values)

    def T(self):
        return self.transpose()

    def max(self, column=None):
        pass

    def min(self, column=None):
        pass

    def median(self, column=None):
        pass

    def mode(self, column=None):
        pass

    def sum(self, column=None):
        pass

    def abs(self, column=None):
        pass

    def corr(self, columns=None):
        pass

    def cov(self, columns=None):
        pass

    def quantile(self):
        pass

    def round(self, n):
        pass

    def idxmax(self, column=None):
        """
        returns the index of the max value of a column or index

        takes in column str - must be a column value
        if column is None, returns maximum value of index
        if multiple occurences, returns index of first occurrence
        """
        if column is not None:
            assert (
                isinstance(column, (str, int, float)) and column in self.columns
            ), f"DF idxmax column must be str/float/int in columns, invalid: {column}"

            vals = [x[self._rep_columns[column]] for x in self.values]
        else:
            # get the vals
            vals = self.index

        vals = [(i, x) for i, x in zip(self.index, vals) if x is not None]

        # get the max val and first index occurence (safe max)
        _max = max(vals, key=lambda x: x[1])

        return _max[0]

    def idxmin(self, column=None):
        """
        returns the index of the min value of a column or index

        takes in column str - must be a column value
        if column is None, returns minimum value of index
        if multiple occurences, returns index of first occurrence
        """
        if column is not None:
            assert (
                isinstance(column, (str, int, float)) and column in self.columns
            ), f"DF idxmin column must be str/float/int in columns, invalid: {column}"

            vals = [x[self._rep_columns[column]] for x in self.values]
        else:
            # get the vals
            vals = self.index

        vals = [(i, x) for i, x in zip(self.index, vals) if x is not None]

        # get the max val and first index occurence (safe max)
        _min = min(vals, key=lambda x: x[1])

        # finish
        return _min[0]

    def dropna(self, columns=None):
        pass

    def fillna(self, columns=None):
        pass

    def isna(self, columns=None):
        pass

    def notna(self, columns=None):
        pass

    def groupby(self, columns=None):
        pass

    def pivot_table(self, columns=None):
        pass

    def merge(self, data, on=None, how=None):
        pass

    def drop(self, index, axis=0):
        """
        takes index value(s)
        """
        assert axis in (0, 1), f"DF drop axis must be 0 or 1, not {axis}"

        if ndim(index) == 0:
            index = [index]

        # check that each index exists in axis
        for ind in index:
            if axis == 0:
                assert (
                    index in self.index
                ), f"DF drop error - index value {index} is not in DF index"
            elif axis == 1:
                assert (
                    index in self.columns
                ), f"DF drop error - column {index} is not in DF columns"

        # then do the actual changes to the data
        for ind in index:
            if axis == 0:
                # values
                self.values.pop(self._rep_index(ind))

            elif axis == 1:
                # values
                [x.pop(self._rep_columns[ind]) for x in self.values]

                # columns
                self.columns.pop(self._rep_columns[ind])
                self._get_rep_columns()

        # finish up - fix shape and _nrow
        self._get_shape()
        self._get_nrow()
        return self

    def drop_duplicates(self, columns=None):
        pass

    def isin(self, value):
        pass

    def sample(
        self,
        n: int = None,
        frac: float = None,
        random_state: int = None,
        columns: Iterable = None,
        replace: bool = False,
        weights: Iterable = None,
    ):
        """
        return a random subset of the dataframe by index

        selecting number
            n, int is number of items, default = 1
            OR
            frac: float, default=None
                0 < frac < 1
                computes number of values with integer division

            
            default is n=1
            if both are specified, ignore frac and use n

        random_state: int, default=None
            random start seed to pass to random module
        
        columns: str or iterable, default=None
            return data from one or more columns
            if 1, returns Col
            if multiple, returns DataFrame
            if None, returns all columns
        
        replace: bool, default=False
            allow sampling from entire distiribution every time?
            if True:
                index WILL NOT be maintained
                uses random.choices
            if False:
                maintains index
                uses random.sample
        
        weights: iterable of weight int/float, default=None
            only used if replace=True (i.e. sampling with replacement i.e. random.choices)

        returns:
            DataFrame
            OR
            Series
        """
        # track original columns
        og_cols = columns

        # args to pass to random
        args = {}

        ## prepare args and check parameters
        # n OR frac - if neither, n=1
        if n is not None:
            assert isinstance(n, int), f"DF sample n must be int; invalid: {n}"
            args["n"] = n
        elif frac is not None:
            assert isinstance(
                frac, float
            ), f"DF sample frac must be float such that 0 < frac < 1; invalid: {frac}"
            args["frac"] = frac
        else:
            args["n"] = 1

        # columns
        if columns is not None:
            assert isinstance(
                columns, (str, Iterable)
            ), f"DF sample columns must be str or iterable; invalid type: {type(columns)}"
            if ndim(columns) == 0:
                columns = [columns]

            for col in columns:
                assert (
                    col in self.columns
                ), f"DF sample columns must be in DF columns; invalid: {col}"
        else:
            columns = self.columns

        # weights
        if weights is not None:
            assert isinstance(weights, Iterable) and sum(
                [type(x) in (int, float) for x in weights]
            ) == len(weights), f"DF sample weights must be iterable of floats/ints"
            assert (
                len(weights) == self._nrow
            ), f"DF sample weights must match number of rows; rows: {self._nrow} weights: {len(weights)}"

        # replace
        if replace is not None:
            assert isinstance(
                replace, bool
            ), f"DF sample replace must be type bool; invalid: {type(replace)}"

            assert (
                replace <= self._nrow
            ), f"DF sample n must be <= number of rows if replace=False"

        # random_state - seed if wanted
        if random_state is not None:
            assert isinstance(
                random_state, int
            ), f"DF sample random_state must be integer; invalid: {random_state}"
            random.seed(random_state)

        if "frac" in args:
            sample_n = round(self._nrow * frac)
        else:
            sample_n = args["n"]

        # random index vals
        if not replace:
            # without replacement
            index_vals = random.sample(self.index, sample_n)
        else:
            # with replacement
            index_vals = random.choices(self.index, k=sample_n, weights=weights)

        # prepare index; only use the index_vals if sampling with replacement
        if replace:
            index = None
        else:
            index = index_vals

        ### get the sample values and return
        # if explicitly a single sample value, return a row
        if n is None and frac is None:
            done = self._row(index_vals[0], columns=columns)

        # elif explicitly a single column, but not explicitly a single row
        if ndim(og_cols) == 0 and not og_cols == None:
            col = columns[0]
            done = _DuffelCol(
                [
                    x[self._rep_columns[col]]
                    for x in [self.values[ix] for ix in index_vals]
                ],
                name=col,
                index=index,
            )

        # explicitly multiple values and columns
        else:
            done = _DuffelDataFrame(
                [
                    [x[self._rep_columns[col]] for col in columns]
                    for x in [self.values[self._rep_index[ix]] for ix in index_vals]
                ],
                columns=columns,
                index=index,
            )

        return done

    def from_dict(self, data, orient: str = "dict", columns: Optional[Iterable] = None):
        pass

    def to_csv(self, filename, index=False):
        """
        writes values to CSV located at filename

        if index==True, write the index as the first row
        """
        if index:
            self.values = [[x] + v for x, v in zip(self.index, self.values)]
        with open(filename, "w") as CSVreport:
            wr = csv.writer(CSVreport)
            wr.writerow([None, *self.columns])
            wr.writerows(self.values)
            CSVreport.close()
        if index:
            self.values = [x[1:] for x in self.values]
        return True

    def to_json(self, filename, orient: str = "dict"):
        """
        take a input filename, orient str
        save self.data as JSON to path at filename in orient format
        object is in dict form: {<index>: { field: value, ...}, ... }

        TODO: add ability to save as dict of lists (i.e. no index - faster to read later and smaller on disk)
        """
        # check orient type
        assert orient in {
            "dict",
            "records",
            "index",
            "split",
            "series",
            "list",
        }, f"DF.to_dict orient must be in ('dict','records','index','split','series', 'list'), not {orient}"

        with open(filename, "w") as fp:
            json.dump(self.to_dict(orient), fp)

        return True

    def to_sql(self, con):
        pass

    def to_dict(self, orient: str = "dict"):
        """
        return self.data and index in dict form: {<index> : { field:value, ...}, ... }
        """
        assert orient in {
            "dict",
            "records",
            "index",
            "split",
            "series",
            "list",
        }, f"DF.to_dict orient must be in ('dict','records','index','split','series', 'list'), not {orient}"
        if orient == "dict":
            return {
                col: {
                    i: val
                    for i, val in zip(
                        self.index, [x[self._rep_columns[col]] for x in self.values]
                    )
                }
                for col in self.columns
            }
        elif orient == "records":
            return [{col: v for col, v in zip(self.columns, x)} for x in self.values]
        elif orient == "split":
            return {
                "index": self.index,
                "columns": list(self.columns),
                "data": self.values,
            }
        elif orient == "index":
            return {
                i: {col: row[self._rep_columns[col]] for col in self.columns}
                for i, row in zip(self.index, self.values)
            }
        elif orient == "series":
            return {col: self.loc[:, col] for col in self.columns}
        elif orient == "list":
            return {
                col: [x[self._rep_columns[col]] for x in self.values]
                for col in self.columns
            }

    def head(self, n=5):
        """returns .loc of first 5 rows"""
        return self._subset_loc(slice(0, n, None), None)

    def tail(self, n=5):
        """returns .loc of last 5 rows"""
        return self._subset_loc(slice(-n, None, None), None)

    #####################################################################################
    # special methods
    #####################################################################################

    def __setitem__(self, col, values):
        """create a column"""
        # check if col in columns
        if col in self.columns:
            col_index = self._rep_columns[col]
        else:
            col_index = len(self.columns)

        # check data
        if isinstance(values, Iterable):
            # vector
            l = len(values)
            assert (
                l == self._nrow
            ), f"DF column values must match DF len; DF len {self._nrow}, values {l}"
        else:
            # scalar
            values = [values for x in range(self._nrow)]

        # edit data
        self.values = [
            [*x[:col_index], v, *x[col_index:]] for x, v in zip(self.values, values)
        ]

        # edit columns
        cols = list(self.columns)
        if col_index == len(cols):
            # new column
            cols.append(col)
        else:
            # edit current column
            cols[col_index] = col
        self.columns = tuple(cols)
        self._get_rep_columns()

        # shape & _nrow
        self._get_nrow()
        self._get_shape()

    def __getitem__(self, index):
        """2D indexing on the data with slices and integers"""
        # grab _DuffelCol by column name
        if ndim(index) == 0:
            return self._col(index)
        elif isinstance(index, slice) or isinstance(index, Iterable):
            return self.loc[:, index]
        else:
            raise ValueError("Not sure how to pull that column")
        # # return column(s)
        # if (
        #     isinstance(index, str)
        #     or (
        #         isinstance(index, Iterable)
        #         and sum([isinstance(x, str) for x in index]) == len(index)
        #     )
        #     and sum([x in self.columns for x in index]) == len(index)
        # ):
        #     if isinstance(index, str):
        #         # single column
        #         return [row[self._rep_columns[index]] for row in self.values]
        #     else:
        #         # multiple columns
        #         return [
        #             [row[self._rep_columns[col]] for col in index]
        #             for row in self.values
        #         ]

        # # deal with single index or slice
        # if not hasattr(index, "__len__"):
        #     assert isinstance(index, int) or isinstance(index, slice)
        #     return self.values[index]

        # len_ = len(index)

        # # multiple indexes - return the second index for all the first index
        # if (
        #     len_ == 2
        #     and sum(
        #         [
        #             isinstance(x, int)
        #             or isinstance(x, slice)
        #             or isinstance(x, list)
        #             or isinstance(x, tuple)
        #             for x in index
        #         ]
        #     )
        #     == 2
        # ):
        #     if isinstance(index[0], int):
        #         return self.values[index[0]][index[1]]
        #     return [x[index[1]] for x in self.values[index[0]]]

        # # deal with boolean lists including 1s and 0s
        # else:
        #     assert len_ == len(
        #         self.values
        #     ), "Boolean selection iterable must be same length as data"
        #     assert sum(
        #         [isinstance(x, bool) or x == 0 or x == 1 for x in index]
        #     ), "Boolean selection iterable must only contain bool values"
        #     return [x for truth, x in zip(index, self.values) if truth]

    def _repr_html_(self):
        """
        Jupyter Notebook magic repr function.
        """
        head = "<tr><td></td>%s</tr>\n" % "".join(
            ["<td><strong>%s</strong></td>" % c for c in self.columns]
        )
        rows = [
            "<td><strong>%d</strong></td>" % i
            + "".join(["<td>%s</td>" % c for c in row])
            for i, row in self.iterrows()
        ]
        html = "<table>{}</table>".format(
            head + "\n".join(["<tr>%s</tr>" % row for row in rows])
        )
        return html

    def __repr__(self):
        strcols = [self._index_name, " --"] + [(" " + str(i)) for i in self.index[:10]]
        strcols = [strcols] + [
            [str(col), "----"]
            + [
                str(val)
                for val in [x[self._rep_columns[col]] for x in self.values[:10]]
            ]
            for col in self.columns
        ]
        nchars = [max(len(val) for val in col) + 2 for col in strcols]

        rows = []
        i = 0
        for row in zip(*strcols):
            if i > 10:
                rows.append(
                    "".join(
                        "..." + " " * (nchars[j] - len("...")) for j in range(len(row))
                    )
                )
                break
            row = list(row)
            rows.append(
                "".join(
                    row[j] + " " * (nchars[j] - len(row[j])) for j in range(len(row))
                )
            )
            i += 1
        rows.append(" ".join(["duffel.DataFrame", str(self.shape)]))

        return "\n" + "\n".join(rows) + "\n"

    def __len__(self):
        return self._nrow

    def __iter__(self):
        return self.columns.__iter__()

    def __eq__(self, comp):
        pass
        # 1D - test equality rowwise

        # 2D - must match size unless each row has 1 thing

        # error

