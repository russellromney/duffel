from typing import Iterable, Mapping, Collection

from .loc import _Loc, _ILoc
from .na import ndim


class _DuffelCol(object):
    def __init__(self, values, name=None, index=None):
        self.name = name

        # create index
        if index is not None:
            assert isinstance(index, Iterable), "_DuffelCol index must be an iterable"
            assert len(index) == len(
                set(index)
            ), "_DuffelCol index values must be unique"
            assert len(index) == len(
                values
            ), f"_DuffelCol index length ({len(index)}) must match number of rows ({len(values)})"
            self.index = index
        else:
            self.index = [x for x in range(len(values))]

        # ingest values
        if isinstance(values, str):
            if index is not None:
                self.values = [x for x in index]
            else:
                self.values = [values]
        elif isinstance(values, Mapping):
            self.index = [y for y in values.keys()]
            self.values = [y for y in values.values()]
        elif isinstance(values, Iterable):
            self.values = values
        elif ndim(values) == 0:
            if index is not None:
                self.values = [x for x in index]
            else:
                self.values = [values]

        self._rep_index = {k: v for v, k in enumerate(self.index)}
        self._nrow = len(self._rep_index)
        self.loc = _Loc(self)
        self.iloc = _ILoc(self)
        self.shape = (self._nrow, 1)

    #####################################################################################
    # internals
    #####################################################################################
    def _invert_rep_index(self):
        return {v: k for k, v in self._rep_index.items()}

    def _col(self, rows):
        return _DuffelCol(
            [
                val
                for val in [x for ix, x in zip(self.index, self.values) if ix in rows]
            ],
            name=self.name,
            index=rows,
        )

    def _subset_loc(self, rows):
        """
        implement .loc indexing behavior
        transform rows to index values and return subset
        """
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
        #
        # this means we can freely use self._rep_index to call integer index locations of each
        ###

        ### return subset
        # single row => return single value
        if ndim(rows) == 0:
            return self.values[self._rep_index[rows]]

        # multiple rows => _DuffelCol
        elif ndim(rows) == 1:
            return self._col(rows)

    def _subset_iloc(self, rows):
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
        return self._subset_loc(rows)

    #####################################################################################
    # interface
    #####################################################################################

    def iterrows(self):
        for i in range(self._nrow):
            yield i, self[i]

    def iteritems(self):
        for i, v in zip(self.index, self.values):
            yield i, v

    def items(self):
        return self.iteritems()

    def head(self, n=6):
        return self._subset_loc(slice(0, n, None))

    def tail(self, n=6):
        return self._subset_loc(slice(0, n, None))

    def value_counts(self, dropna=False):
        pass

    #####################################################################################
    # special methods
    #####################################################################################

    def __setitem__(self, index, value):
        assert (
            index in self.index
        ), f"_DuffelCol index value must exist; ({index}) not in index"
        self.values[self._rep_index[index]] = value

    def __getitem__(self, index):
        return self._subset_loc(index)

    def __len__(self):
        return self._nrow

    def __iter__(self):
        return self.values.__iter__()

    def _repr_html_(self):
        """
        Jupyter Notebook magic repr function.
        """
        head = "<tr><td></td>%s</tr>\n" % "".join(
            ["<td><strong>%s</strong></td>" % c for c in [self.name]]
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
            [str(col), "----"] + [str(val) for val in self.values]
            for col in [self.name]
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
        rows.append(" ".join(["duffel.Col", str(self.shape)]))
        return "\n" + "\n".join(rows) + "\n"

