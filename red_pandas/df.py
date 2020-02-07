from typing import Iterable, Mapping
from functools import reduce
from collections import Counter

from .na import ndim, NA
from .row import _Row
from .col import _Col
from .loc import _Loc, _ILoc


class DataFrame:
    def __init__(self, values, columns=None, index=None, copy=True):
        if columns is not None:
            self.columns = tuple(list(columns)) # throws error if columns not iterator
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
                values
            ), f"DF index length ({len(index)}) must match number of rows ({len(values)})"
            self.index = index
        elif hasattr(self, "index") and self.index is not None:
            # it was created by being a dict of dicts
            pass
        else:
            self.index = [x for x in range(len(self.values))]

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
        self._rep_index = {k: v for v, k in enumerate(self.index)}
        self._nrow = len(self._rep_index)
        self._rep_columns = {k: v for v, k in enumerate(self.columns)}
        self.shape = ( self._nrow, len(self.columns) )

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

            # iterable of iterables
            if isinstance(values[0], Iterable) and not isinstance(values[0], Mapping):
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
        """ingest maps of iterables """
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

    # def __setitem__(self, index, value):
    #     self.values[index] = value

    def __getitem__(self, index):
        """2D indexing on the data with slices and integers"""
        # grab _Col by column name
        if ndim(index)==0:
            return self._col(index)
        elif isinstance(index,slice) or isinstance(index,Iterable):
            return self.loc[:,index]
        else:
            raise ValueError('Not sure how to pull that column')
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

    def _row(self, i, columns=None):
        if i >= self._nrow:
            raise IndexError("Row index out of range: %s" % i)
        if columns is None:
            columns = self.columns
        return _Row(
            [val for col, val in zip(self.columns, self.values[i]) if col in columns],
            index=i,
            columns=columns,
        )

    def _col(self, column, rows=None):
        if rows is None:
            rows = self.index
        else:
            return _Col(
                [
                    val
                    for val in [
                        x
                        for ix, x in zip(
                            self.index, [row[self._rep_columns[column]] for row in self.values]
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

        # single row, multiple columns => _Row
        elif ndim(rows) == 0 and ndim(columns) == 1:
            return self._row(rows, columns=columns)

        # multiple rows, one column => _Col
        elif ndim(rows) == 1 and ndim(columns) == 0:
            return self._col(columns, rows=rows)

        # multiple rows, multiple columns => DataFrame
        elif ndim(rows) == 1 and ndim(columns) == 1:
            return DataFrame(
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

    def head(self, n=6):
        return self._subset_loc(slice(0, n, None), None)

    def tail(self, n=6):
        return self._subset_loc(slice(0, n, None), None)

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
        strcols = [" ", " --"] + [(" " + str(i)) for i in self.index]
        strcols = [strcols] + [
            [str(col), "----"] + [str(val) for val in self.loc[:, col] ]
            for col in self.columns
        ]
        nchars = [max(len(val) for val in col) + 2 for col in strcols]

        rows = []
        i = 0
        for row in zip(*strcols):
            if i > 10:
                rows.append(
                    "".join(
                        '...' + " " * (nchars[j] - len('...')) for j in range(len(row))
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
        rows.append(
            " ".join(
                ['red_pandas.DataFrame', str(self.shape) ]
            )
        )

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

