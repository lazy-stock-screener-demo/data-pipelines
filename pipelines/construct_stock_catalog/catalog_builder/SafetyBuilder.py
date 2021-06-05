from .Builder import Builder
from pipelines.config.nameConfig import SafetyName


class SafetyBuilder(Builder):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self._col_name = SafetyName

    def get_total_assets(self):
        pars = dict(data=self.dump_total_assets())
        return self.dump_series(**pars)

    def get_long_term_debt(self):
        pars = dict(data=self.dump_long_term_debt())
        return self.dump_series(**pars)

    def get_total_liabilities(self):
        pars = dict(data=self.dump_total_liabilities())
        return self.dump_series(**pars)

    def get_current_ratio(self):
        pars = dict(
            data=self.divide(
                self.dump_current_assets(), self.dump_current_liabilities()
            )
        )
        return self.dump_series(**pars)

    def get_quick_ratio(self):

        print(self.dump_inventory())
        self.dump_current_assets()
        self.dump_other_current_assets()
        pars = dict(
            # TODO
        )
        return self.dump_series(**pars)

    def get_debt_equity_ratio(self):
        pars = dict(
            # TODO
        )
        return self.dump_series(**pars)

    def get_debt_capital_ratio(self):
        pars = dict(
            # TODO
        )
        return self.dump_series(**pars)

    def get_debt_asset_ratio(self):
        pars = dict(
            # TODO
        )
        return self.dump_series(**pars)

    def get_shares_capital(self):
        pars = dict(
            # TODO
        )
        return self.dump_series(**pars)

    ######### Set ###########
    def set_total_assets(self):
        output = dict(
            column_name=self._col_name["total_assets"],
            column_series=self.get_total_assets(),
        )
        self.set_output(**output)

    def set_total_liabilities(self):
        output = dict(
            column_name=self._col_name["total_liabilities"],
            column_series=self.get_total_liabilities(),
        )
        self.set_output(**output)

    def set_long_term_debt(self):
        output = dict(
            column_name=self._col_name["long_term_debt"],
            column_series=self.get_long_term_debt(),
        )
        self.set_output(**output)

    def set_current_ratio(self):
        output = dict(
            column_name=self._col_name["current_ratio"],
            column_series=self.get_current_ratio(),
        )
        self.set_output(**output)

    def set_quick_ratio(self):
        output = dict(
            column_name=self._col_name["quick_ratio"],
            column_series=self.get_quick_ratio(),
        )
        self.set_output(**output)

    def set_free_cash_flows(self):
        output = dict(
            column_name=self._col_name["free_cash_flow"],
            column_series=self.get_free_cash_flows(),
        )
        self.set_output(**output)

    def set_debt_equity_ratio(self):
        output = dict(
            column_name=self._col_name["debt_equity_ratio"],
            column_series=self.get_debt_equity_ratio(),
        )
        self.set_output(**output)

    def set_debt_capital_ratio(self):
        output = dict(
            column_name=self._col_name["debt_capital_ratio"],
            column_series=self.get_debt_capital_ratio(),
        )
        self.set_output(**output)

    def set_debt_asset_ratio(self):
        output = dict(
            column_name=self._col_name["debt_asset_ratio"],
            column_series=self.get_debt_asset_ratio(),
        )
        self.set_output(**output)

    def set_shares_capital(self):
        output = dict(
            column_name=self._col_name["share_capital"],
            column_series=self.get_shares_capital(),
        )
        self.set_output(**output)

    @staticmethod
    def create(**kwargs):
        builder = SafetyBuilder(**kwargs)
        builder.set_total_assets()
        builder.set_total_liabilities()
        builder.set_long_term_debt()
        builder.set_current_ratio()
        # builder.set_quick_ratio()
        builder.set_free_cash_flows()
        # builder.set_debt_equity_ratio()
        # builder.set_debt_capital_ratio()
        # builder.set_debt_asset_ratio()
        # builder.set_shares_capital()
        return builder.get_output()
