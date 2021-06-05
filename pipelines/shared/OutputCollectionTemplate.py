from pipelines.shared.OutputPandasBase import OutputPandasBase


class OutputCollectionTemplate(OutputPandasBase):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def set_output(self, *args, **kwargs):
        column_head = kwargs.get("column_name")
        column_series = kwargs.get("column_series")
        self._output = self.base().concat(
            [
                self._output,
                column_series.rename(column_head).sort_values(ascending=False),
            ],
            axis=1,
        )

    def get_output(self):
        return self._output