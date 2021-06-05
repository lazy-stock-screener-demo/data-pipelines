from .Builder import Builder
from pipelines.config.nameConfig import ProfitName
from pipelines.shared.Logger import dumpFunc


class ProfitBuilder(Builder):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self._col_name = ProfitName

    def get_net_income(self):
        pars = dict(
            data=self.dump_net_income(),
        )
        return self.dump_series(**pars)

    def get_operating_cash_flow(self):
        pars = dict(
            data=self.dump_operating_cash_flows(),
        )
        return self.dump_series(**pars)

    def get_net_income_margin(self):
        pars = dict(
            data=self.divide(self.dump_net_income(), self.dump_revenue()),
        )
        return self.dump_series(**pars)

    # @dumpFunc
    def get_financial_leverage(self):
        pars = dict(
            data=self.divide(self.dump_total_assets(), self.dump_stockholders_equity()),
        )
        return self.dump_series(**pars)

    # @dumpFunc
    def get_asset_turnover_ratio(self):
        pars = dict(
            data=self.divide(
                self.dump_revenue(),
                self.cal_n_year_avg(self.dump_total_assets(), **dict(periods=1)),
            ),
        )
        return self.dump_series(**pars)

    def get_ROE(self):
        pars = dict(
            data=self.get_net_income_margin()
            * self.get_asset_turnover_ratio()
            * self.get_financial_leverage()
        )
        return self.dump_series(**pars)

    def get_ROS(self):
        pars = dict(data=self.divide(self.dump_operating_income(), self.dump_revenue()))
        return self.dump_series(**pars)

    def get_ROA(self):
        pars = dict(data=self.get_net_income_margin() * self.get_asset_turnover_ratio())
        return self.dump_series(**pars)

    def get_EPS(self):
        pars = dict(
            data=self.divide(self.dump_net_income(), self.dump_shares()),
        )
        return self.dump_series(**pars)

    ######### Set ###########
    def set_net_income(self):
        output = dict(
            column_name=self._col_name["net_income"],
            column_series=self.get_net_income(),
        )
        self.set_output(**output)

    def set_operating_cash_flow(self):
        output = dict(
            column_name=self._col_name["operating_cash_flow"],
            column_series=self.get_operating_cash_flow(),
        )
        self.set_output(**output)

    def set_ROE(self):
        output = dict(
            column_name=self._col_name["ROE"],
            column_series=self.get_ROE(),
        )
        self.set_output(**output)

    def set_ROS(self):
        output = dict(
            column_name=self._col_name["ROS"],
            column_series=self.get_ROS(),
        )
        self.set_output(**output)

    def set_ROA(self):
        output = dict(
            column_name=self._col_name["ROA"],
            column_series=self.get_ROA(),
        )
        self.set_output(**output)

    def set_EPS(self):
        output = dict(
            column_name=self._col_name["EPS"],
            column_series=self.get_EPS(),
        )
        self.set_output(**output)

    @staticmethod
    def create(**kwargs):
        builder = ProfitBuilder(**kwargs)
        builder.set_net_income()
        builder.set_operating_cash_flow()
        builder.set_ROE()
        builder.set_ROS()
        builder.set_ROA()
        builder.set_EPS()
        return builder.get_output()
