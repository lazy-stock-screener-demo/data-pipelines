import pandas as pd
import numpy as np
import inspect
import pandas.api.types as pd_type
from pipelines.shared.utils import getYearOrRange
from pipelines.shared.Logger import dumpFunc


def traceWrongRowIndex(func):
    def wrapper(*args, **kwargs):
        (filename, line_number, function_name, lines, index) = inspect.getframeinfo(
            inspect.currentframe().f_back.f_back
        )
        print("Inspection: ")
        print(f"{function_name} found no index.")
        print(f"Location: {filename}:{line_number}\n")
        result = func(*args, **kwargs)
        return result

    return wrapper


class InputPandasBase:
    def __init__(self, child_self, **kwargs):
        self._fs_df = kwargs.get("fs_df")

    # @dumpFunc
    @traceWrongRowIndex
    def dump_default_series(self):
        # let the integrated dataframe decide the schema
        columns = self._fs_df.columns
        return pd.Series(
            [np.NaN for i in columns],
            index=columns,
            dtype="float",
        )

    # @dumpFunc
    def dump_through_more(self, index_list):
        result = ""
        while len(index_list) > 0:
            index = index_list.pop(0)
            if self.is_index_exist(index):
                result = self.dump(index)
                break
            else:
                continue
        if pd_type.is_float_dtype(result):
            return result
        else:
            return self.dump_default_series()

    # @dumpFunc
    def is_index_exist(self, index):
        return index in self._fs_df.index

    def dump(self, index):
        return pd.Series(data=self._fs_df.loc[index], dtype=float)
