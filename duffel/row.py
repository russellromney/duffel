class _DuffelRow(object):
    def __init__(self, values, columns=None, index=None):
        self.values = values
        self.columns = columns
        self.index = index
        self.shape = (1, len(self.columns))

        self._rep_columns = {k: v for v, k in enumerate(self.columns)}

    def __getitem__(self, column):
        return self.values[self._rep_columns[column]]

    #     if item in self:
    #         return super().__getitem__(item)
    #     else:
    #         try:
    #             return super().__getitem__(self._keys[item])
    #         except IndexError:
    #             raise KeyError("No such key in row")

    # def __iter__(self):
    #     for key in self._keys:
    #         yield self[key]

    # def _repr_html_(self):
    #     title = "".join(["<td><strong>%s</strong></td>" % key for key in self._keys])
    #     vals = "".join(["<td>%s</td>" % self[key] for key in self._keys])
    #     return "<table><tr>%s</tr><tr>%s</tr></table>" % (title, vals)

    def __repr__(self):
        strcols = [" ", " --"] + [(" " + str(self.index))]
        strcols = [strcols] + [
            [str(col), "----"]
            + [str(val) for val in [self.values[self._rep_columns[col]]]]
            for col in self.columns
        ]
        nchars = [max(len(val) for val in col) + 2 for col in strcols]

        rows = []
        for row in zip(*strcols):
            row = list(row)
            rows.append(
                "".join(
                    row[j] + " " * (nchars[j] - len(row[j])) for j in range(len(row))
                )
            )
        rows.append(" ".join(["duffel.Row", str(self.shape)]))

        return "\n" + "\n".join(rows) + "\n"

    # def keys(self):
    #     return self._keys

    # def items(self):
    #     for key in self._keys:
    #         yield key, self[key]
