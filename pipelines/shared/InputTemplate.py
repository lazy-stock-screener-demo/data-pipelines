from pipelines.shared.InputPandasBase import InputPandasBase
from pipelines.shared.Logger import dumpFunc


class InputTemplate(InputPandasBase):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def dump_net_income(self):
        pars = ["Net income"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_revenue(self):
        pars = ["Revenue"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_total_assets(self):
        pars = ["Total assets"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_total_liabilities(self):
        pars = ["Total liabilities"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_current_assets(self):
        pars = ["Total current assets"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_other_current_assets(self):
        pars = ["Other current assets"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_current_liabilities(self):
        pars = ["Total current liabilities"]
        return self.dump_through_more(pars)

    @dumpFunc
    def dump_inventory(self):
        pars = ["Inventories"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_long_term_debt(self):
        pars = ["Long-term debt"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_stockholders_equity(self):
        pars = ["Total stockholders’ equity"]
        return self.dump_through_more(pars)

    # @dumpFunc
    def dump_shares(self):
        pars = [
            "Common stock, issued",
            "Common stock, outstanding",
        ]
        return self.dump_through_more(pars) * 1e-6

    def dump_total_dividends(self):
        pars = [
            "aa",
            "Cash Dividends Paid",
            "bbb",
        ]
        return self.dump_through_more(pars)

    def dump_capital_expenditures(self):
        pars = [
            "Capital Expenditure, Reported",
            "Additions to property and equipment",
            "Purchase/Sale and Disposal of Property, Plant and Equipment, Net",
        ]
        return self.dump_through_more(pars)

    def dump_operating_income(self):
        pars = [
            "Operating income",
        ]
        return self.dump_through_more(pars)

    def dump_operating_cash_flows(self):
        pars = [
            "Net cash from operations",
            "Cash Generated from Operating Activities",
            "Net cash flows used in operating activities",
        ]
        return self.dump_through_more(pars)