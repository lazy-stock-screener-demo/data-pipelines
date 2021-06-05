from pipelines.shared.OutputCollectionTemplate import OutputCollectionTemplate
from pipelines.shared.InputTemplate import InputTemplate
from pipelines.shared.utils import getYearOrRange

# print(("dump \n{0}").format(self.dump_operating_cash_flows()))


class Builder(OutputCollectionTemplate, InputTemplate):
    def __init__(self, child_self, **kwargs):
        OutputCollectionTemplate.__init__(self, **kwargs)
        InputTemplate.__init__(self, **kwargs)

    def get_shares(self):
        pars = dict(
            data=self.dump_shares(),
        )
        return self.dump_series(**pars)

    def get_prices(self):
        pass

    def get_free_cash_flows(self):
        pars = dict(
            data=self.sum(
                [self.dump_operating_cash_flows(), self.dump_capital_expenditures()]
            ),
        )
        return self.dump_series(**pars)

    def get_total_dividends(self):
        pars = dict(
            data=self.dump_total_dividends(),
        )
        return self.dump_series(**pars)
