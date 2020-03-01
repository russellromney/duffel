# anything that multiple other duffel submodules need to pull from

from typing import Iterable


def _base_concat(values, axis=0, ignore_index=True):
    """
    define the base _concat function that DataFrame and _concat will use; this avoids overlapping imports

    standard join is outer - this is supported first
    only able to do axis 0 and 1
    ignore index counts for columns and for index
    """
    # must be duffel objects
    assert isinstance(values, Iterable), "concat values must be a list or iterable"
    for item in values:
        assert any(
            (
                type(item).__name__ == "_DuffelDataFrame",
                type(item).__name__ == "_DuffelCol",
            )
        ), f"duffel.concat values must be type duffel.col.Col or duffel.df.DataFrame, not {type(item)}"

    # check index overlap
    if not ignore_index:
        indexes = [i for y in [x.index for x in values] for i in y]
        set_indexes = set([i for y in [x.index for x in values] for i in y])
        assert len(indexes) == len(
            set_indexes
        ), "concat indexes must be unique if ignore_index=False"
    else:
        pass

    # create set of columns
    columns = []
    for x in values:
        if hasattr(x, "columns"):  # is dataframe
            columns = columns + [c for c in x.columns if not c in columns]
        else:  # is column
            columns = columns + [c for c in [x.name] if not c in columns]

    # concat the data
    data = {col: [] for col in columns}
    for x in values:
        # is dataframe
        if hasattr(x, "columns"):
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

    nrow = len(data[list(data.keys())[0]])

    # create index
    if ignore_index == True:
        index = list(range(nrow))
    else:
        index = [i for y in [x.index for x in values] for i in y]
        assert (
            len(set(index)) == nrow
        ), "concat index values must be unique if ignore_index=False"

    return data, index, columns
