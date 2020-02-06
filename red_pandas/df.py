from typing import Iterable, Mapping
from functools import reduce
import collections

from .na import ndim, NA
from .row import _Row
from .loc import _Loc, _ILoc


class DataFrame:
    def __init__(self, values, columns=None, index=None):
        self.columns = columns

        # ingest iterable
        if isinstance(values, Iterable) and not isinstance(values, Mapping):
            # blank - make it 2d
            if not len(values):
                self.values = [[]]
                self.empty = True
            else:
                # put values into memory - iterator fix - this is inefficient, sure
                values = list(values)

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
                    self.columns = cols
                    # create values
                    self.values = [[x.get(k, None) for k in cols] for x in values]

        # ingest dict of iterables
        elif isinstance(values, Mapping):
            if not len(values):
                self.values = [[]]
                self.empty = True
            else:
                self.columns = list(values.columns())

                # put noniterables in single-len lists
                for col in values:
                    if isinstance(values[col], Iterable) and not isinstance(
                        values[col], Mapping
                    ):
                        values[col] = list(values)
                    else:
                        values[col] = list([values[col]])

                # expand out smaller columns with None
                maxlen = max([len(values[col]) for col in values])
                for col in values:
                    values[col] = values[col] + [None] * (maxlen - len(values[col]))

                # create values
                self.values = [list(x) for x in zip(*values.items())]

        # create index
        if index is not None:
            assert isinstance(index, list) or isinstance(
                index, tuple
            ), "DF index must be an iterable"
            assert len(index) == len(set(index)), "DF index values must be unique"
            assert len(index) == len(
                values
            ), f"DF index length ({len(index)}) must match number of rows ({len(values)})"
            self.index = index
        else:
            self.index = [x for x in range(len(self.values))]

        # deal with columns
        if self.columns is not None:
            columns = self.columns
            assert isinstance(columns, list) or isinstance(
                columns, tuple
            ), "DF columns must be an iterable"
            assert len(columns) == len(
                set(columns)
            ), f"DF columns values must be unique - duplicates are {[item for item, count in collections.Counter(columns).items() if count > 1]}"
            assert len(columns) == len(
                self.values[0]
            ), f"DF columns length ({len(columns)}) must match length of values ({len(values[0])})"
            self.columns = columns
        else:
            self.columns = [x for x in range(len(self.values))]

        # create enumerated internal representation of index and columns
        self._rep_index = {k: v for v, k in enumerate(self.index)}
        self._nrow = len(self._rep_index)
        self._rep_columns = {k: v for v, k in enumerate(self.columns)}

        # loc and iloc
        self.iloc = _ILoc(self)
        self.loc = _Loc(self)

    #####################################################################################
    # internals
    #####################################################################################

    # def __setitem__(self, index, value):
    #     self.values[index] = value

    def __getitem__(self, index):
        """2D indexing on the data with slices and integers"""

        # return column(s)
        if (
            isinstance(index, str)
            or (
                isinstance(index, Iterable)
                and sum([isinstance(x, str) for x in index]) == len(index)
            )
            and sum([x in self.columns for x in index]) == len(index)
        ):
            if isinstance(index, str):
                # single column
                return [row[self._rep_columns[index]] for row in self.values]
            else:
                # multiple columns
                return [
                    [row[self._rep_columns[col]] for col in index]
                    for row in self.values
                ]

        # deal with single index or slice
        if not hasattr(index, "__len__"):
            assert isinstance(index, int) or isinstance(index, slice)
            return self.values[index]

        len_ = len(index)

        # multiple indexes - return the second index for all the first index
        if (
            len_ == 2
            and sum(
                [
                    isinstance(x, int)
                    or isinstance(x, slice)
                    or isinstance(x, list)
                    or isinstance(x, tuple)
                    for x in index
                ]
            )
            == 2
        ):
            if isinstance(index[0], int):
                return self.values[index[0]][index[1]]
            return [x[index[1]] for x in self.values[index[0]]]

        # deal with boolean lists including 1s and 0s
        else:
            assert len_ == len(
                self.values
            ), "Boolean selection iterable must be same length as data"
            assert sum(
                [isinstance(x, bool) or x == 0 or x == 1 for x in index]
            ), "Boolean selection iterable must only contain bool values"
            return [x for truth, x in zip(index, self.values) if truth]

    def _row(self, i, columns=None):
        if i >= self._nrow:
            raise IndexError("Row index out of range: %s" % i)
        if columns is None:
            columns = self.columns
        return _Row(
            columns, [self[i, col] if col in self.columns else NA for col in columns]
        )

    def _col(self, item, rows=None):
        if rows is None:
            return self[:, item]
        else:
            return self[rows, item]

    def _subset_loc(self, rows, columns=None):
        # return a dataframe of the specified subset using the named index and columns
        if isinstance(rows, slice):
            return DataFrame(self[rows], columns=self.columns)

        if columns is None:
            columns = self.columns
        elif isinstance(columns, slice):
            # slice of columns...need to turn into column names
            if columns.start is None:
                colstart = 0
            elif columns.start not in self.columns:
                raise KeyError("Column not found: %s" % columns.start)
            else:
                colstart = self._rep_columns[columns.start]

            if columns.stop is None:
                colstop = len(self.columns)
            elif columns.stop not in self.columns:
                raise KeyError("Column not found: %s" % columns.stop)
            else:
                colstop = self._rep_columns[columns.stop]

            columns = self.columns[colstart : colstop : columns.step]

        # if rows are a list, they need to be an array
        if ndim(rows) == 1:
            rows = [rows]

        if ndim(columns) == 1:
            # list of columns
            if isinstance(rows, int):
                # single row
                return self._row(rows, columns)
            elif isinstance(rows, slice) or ndim(rows) == 1:
                # data frame subset
                if all(col not in self.columns for col in columns):
                    raise ValueError(
                        "None of %s were found in columns" % ", ".join(columns)
                    )

                newdata = {}
                for col in columns:
                    newdata[col] = self[rows, col]
                return DataFrame(newdata, columns=columns)
            else:
                raise ValueError(
                    "Don't know how to subset with rows of type %s"
                    % type(rows).__name__
                )

        elif columns in self.columns:
            # single column
            if isinstance(rows, int):
                # single value
                return self[rows, columns]
            elif isinstance(rows, slice) or ndim(rows) == 1:
                # subset of column
                print(rows, columns)
                return self[rows, columns]
            else:
                raise ValueError(
                    "Don't know how to subset with rows of type %s"
                    % type(rows).__name__
                )

        else:
            raise ValueError(
                "Don't know how to subset with columns of type %s"
                % type(columns).__name__
            )

    def _subset_iloc(self, rows, columns=None):
        if columns is None:
            return self._subset_loc(rows, self.columns)
        elif isinstance(columns, int) or isinstance(columns, slice):
            return self._subset_loc(rows, self.columns[columns])
        elif ndim(columns) == 1:
            # can be an array booleans or ints
            columns = list(columns)
            if type(columns[0]) in ("i", "b"):
                return self._subset_loc(
                    rows, [self.columns[self._rep_columns[c]] for c in columns]
                )
            else:
                raise ValueError(
                    f"Don't know how to subset with column array of type {type(columns[0])}"
                )

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
        strcols = [" ", " --"] + [(" " + str(i)) for i in range(self._nrow)]
        strcols = [strcols] + [
            [str(col), "----"] + [str(val) for val in self[:, self._rep_columns[col]]]
            for col in self.columns
        ]
        nchars = [max(len(val) for val in col) + 2 for col in strcols]

        rows = []
        i = 0
        for row in zip(*strcols):
            if i > 15:
                break
            row = list(row)
            rows.append(
                "".join(
                    row[j] + " " * (nchars[j] - len(row[j])) for j in range(len(row))
                )
            )
            i += 1
        return "\n" + "\n".join(rows) + "\n"

    def __len__(self):
        return self._nrow

    def __iter__(self):
        return self.columns.__iter__()

