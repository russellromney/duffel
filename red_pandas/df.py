from typing import Callable, Iterable, Dict, Optional, List, Mapping, Sequence


# class DataFrame(object):
#     def __init__(
#         self,
#         data: Sequence,
#         columns: Optional[Iterable[str]] = None,
#         # dtypes: Optional[Dict, Iterable[str]] = None,
#     ):
#         self.values = {}
#         self.index = {}
#         self._iloc_index = {}

#         if not columns:
#             # if DataFrame - pd or rp - get columns and map into Dict
#             if isinstance(data, dict) or type(data).__name__ == "DataFrame":
#                 columns = [x for x in data]
#                 self.columns = columns

#                 for col in data.columns:
#                     # set data
#                     pass

#                     # set

#             # if dict, get unique names
#             elif data:
#                 pass

#             # otherwise, create columns col1-colN


class DataFrame:
    def __init__(
        self,
        values=[[1, 2, 3, 4, 5, 6, 7], [x for x in "abcdefg"]],
        columns=None,
        index=None,
    ):
        # ingest values
        self.values = values

        # create index
        if index is not None:
            assert isinstance(index, list) or isinstance(
                index, tuple
            ), "DF index must be an iterable"
            assert len(index) == len(set(index)), "DF index values must be unique"
            assert len(index) == len(values), "DF index length must match values length"
            self.index = index
        else:
            self.index = [x for x in range(len(self.values))]

        # deal with columns
        if columns is not None:
            assert isinstance(columns, list) or isinstance(
                columns, tuple
            ), "DF columns must be an iterable"
            assert len(columns) == len(set(columns)), "DF columns values must be unique"
            assert len(columns) == len(values[0])
            self.columns = columns
        else:
            self.columns = [x for x in range(len(self.values))]

        # create enumerated internal representation of index and columns
        self._rep_index = {k: v for v, k in enumerate(self.index)}
        self._rep_columns = {k: v for v, k in enumerate(self.columns)}

        # loc and iloc
        self.iloc = _ILoc(self)
        self.loc = _Loc(self)

    #####################################################################################
    # internals
    #####################################################################################

    def __setitem__(self, index, value):
        self.values[index] = value

    def __getitem__(self, index):
        """2D indexing on the data with slices and integers"""

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

    def _subset_loc(self, rows, columns=None):
        pass

    def _subset_iloc(self, rows, columns=None):
        pass


class _Loc(object):
    def __init__(self, df):
        self.df = df

    def __getitem__(self, item):
        if not isinstance(item, tuple):
            return self.df._subset_loc(item)
        else:
            return self.df._subset_loc(*item)


class _ILoc(object):
    def __init__(self, df):
        self.df = df

    def __getitem__(self, item):
        if not isinstance(item, tuple):
            return self.df._subset_iloc(item)
        else:
            return self.df._subset_iloc(*item)
