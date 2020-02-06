

class _Row(dict):

    def __init__(self, columns, vals):
        super().__init__(zip(columns, vals))
        self._keys = tuple(columns)

    def __getitem__(self, item):
        if item in self:
            return super().__getitem__(item)
        else:
            try:
                return super().__getitem__(self._keys[item])
            except IndexError:
                raise KeyError("No such key in row")

    def __iter__(self):
        for key in self._keys:
            yield self[key]

    def _repr_html_(self):
        title = "".join(["<td><strong>%s</strong></td>" %
                         key for key in self._keys])
        vals = "".join(["<td>%s</td>" % self[key] for key in self._keys])
        return "<table><tr>%s</tr><tr>%s</tr></table>" % (title, vals)

    def keys(self):
        return self._keys

    def items(self):
        for key in self._keys:
            yield key, self[key]