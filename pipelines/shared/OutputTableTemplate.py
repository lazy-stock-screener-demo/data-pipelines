from pipelines.shared.OutputPandasBase import OutputPandasBase


class OutputTableTemplate(OutputPandasBase):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def set_output(self, *args, **kwargs):
        column_head = kwargs.get("column_name")
        column_series = kwargs.get("column_series")
        row_as_index = kwargs.get("row_as_index")
        if isinstance(row_as_index, list) and isinstance(column_head, list):
            self._output = self.base().DataFrame(
                [column_series[row_as_index]],
                columns=column_head,
                index=[self._cik],
            )
        else:
            self._output.at[self._cik, column_head] = (
                0
                if self.base().isnull(column_series[row_as_index])
                else column_series[row_as_index]
            )

    def get_output(self):
        return self._output