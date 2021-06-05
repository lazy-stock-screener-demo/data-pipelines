import pandas as pd
import pandas.api.types as pd_type
import numpy as np
from pipelines.shared.Logger import dumpFunc


class OutputPandasBase:
    def __init__(self, child_self, **kwargs):
        self._output = pd.DataFrame()
        self._cik = kwargs.get("cik")

    def base(self):
        return pd

    def sum(self, item_list):
        seriesA = (
            item_list.pop(0)
            if isinstance(item_list[0], pd.Series)
            else pd.Series(data=item_list.pop(0))
        )
        for item in item_list:
            if isinstance(item, pd.Series):
                dropna_serie = item.dropna()
                return seriesA.dropna().add(dropna_serie.dropna())
            else:
                dropna_serie = pd.Series(data=item)
                return seriesA.dropna().add(dropna_serie.dropna())

    # @dumpFunc
    def divide(self, serieA, serieB):
        # schema inconsistent after dropNa
        return np.divide(serieA, serieB)

    def shift(self, data, shift_pars):
        if isinstance(data, pd.Series):
            return data.shift(**shift_pars)
        elif isinstance(data, pd.DataFrame):
            return data.shift(**shift_pars)
        else:
            return pd.Series(data=data).shift(**shift_pars)

    def dropna(self, data):
        if isinstance(data, pd.Series):
            return data.dropna()
        else:
            return pd.Series(data=data).dropna

    def dump_series(self, **kwargs):
        index = kwargs.get("index")
        data = kwargs.get("data")
        if not index:
            return data
        else:
            return pd.Series(
                data,
                index,
                dtype=kwargs.get("dtype"),
            )

    def head(self, **pars):
        data = pars.get("data")
        headNum = pars.get("head")
        if isinstance(data, pd.Series):
            return data.head(headNum)
        elif isinstance(data, pd.DataFrame):
            return data.head(headNum)
        else:
            return pd.Series(data=data).head(headNum)

    def avg(self, data):
        if isinstance(data, pd.Series):
            return data.mean()
        elif isinstance(data, pd.DataFrame):
            return data.mean()
        else:
            return pd.Series(data=data).mean()

    def max(self, data):
        if isinstance(data, pd.Series):
            return data.max()
        elif isinstance(data, pd.DataFrame):
            return data.max()
        else:
            return pd.Series(data=data).max()

    def min(self, data):
        if isinstance(data, pd.Series):
            return data.min()
        elif isinstance(data, pd.DataFrame):
            return data.min()
        else:
            return pd.Series(data=data).min()

    def dump_df(self):
        pass

    def dump_cell_value(self):
        pass

    def concat(self):
        pass

    def get_avg_in_x(self, data, head):
        pars = dict(data=data, head=head)
        return self.avg(self.head(**pars))

    def get_max_in_x(self, data, head):
        pars = dict(data=data, head=head)
        return self.max(self.head(**pars))

    def get_min_in_x(self, data, head):
        pars = dict(data=data, head=head)
        return self.min(self.head(**pars))

    def cal_growth(self, data, shift_pars):
        return self.dropna(self.divide(data, self.shift(data, shift_pars)) - 1)

    def cal_n_year_avg(self, data, **pars):
        shift_pars = dict(periods=pars.get("periods"))
        if isinstance(data, pd.Series):
            dropna_data = data.dropna()
            return dropna_data.add(self.shift(dropna_data, shift_pars)).divide(2)
        else:
            dumped_data = pd.Series(data=data)
            return dumped_data.add(self.shift(dumped_data, shift_pars)).divide(2)
