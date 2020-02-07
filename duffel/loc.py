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
