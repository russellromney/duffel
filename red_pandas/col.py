from .loc import _Loc, _ILoc


class _Col:
    def __init__(self, data, name=None, index=None):
        self.values = data
        self.index = index
        self.name = name
        self.columns = [name]

        self._rep_index = {k: v for v, k in enumerate(self.index)}
        self._nrow = len(self._rep_index)

        self.loc = _Loc(self)
        self.iloc = _ILoc(self)

    #####################################################################################
    # internals
    #####################################################################################

    def __getitem__(self, index):
        pass

    def _row(self, i):
        pass

    def head(self, n=6):
        pass

    def tail(self, n=6):
        pass

    def _subset_loc(self, item):
        pass

    def _subset_iloc(self, item):
        pass

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
            [str(col), "----"] + [str(val) for val in self[:]] for col in self.columns
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

