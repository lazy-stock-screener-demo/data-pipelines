import pandas as pd
import re
from pipelines.shared.TableCommand import TableCommand
from pipelines.shared.TableInvoker import TableInvoker


######################Integrate Financial Statements Invoker###########################


class IntegrateFinancialStatementsInvoker(TableInvoker):
    def __init__(self, **kwargs):
        super().__init__(self)
        self._input_config = kwargs.get("input_config")
        self._output_config = kwargs.get("output_config")
        self._command_queue_arr = [
            CombineFinancialStatementsCommand(**self.set_default_pars),
        ]
        super().__init__(self._command_queue_arr)


class CombineFinancialStatementsCommand(TableCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self):
        new_df = pd.DataFrame()
        # print(
        #     ("FS_List \n {0}").format(self._invoker._input_config["fs_list"]["state_2"])
        # )
        for statement in self._invoker._input_config["fs_list"]["state_2"]:
            new_df = new_df.combine_first(statement)
        # self._invoker._payload["statements"] = pd.concat(
        #     self._invoker._input_config["fs_list"]["state_2"], axis=1
        # ).T
        self._invoker._payload["statements"] = new_df.T
        # print(
        #     ("Intergration Statements\n {0}").format(
        #         self._invoker._payload["statements"]
        #     )
        # )
        self._invoker._payload["fs_df"] = {
            "fs_df": self._invoker._payload["statements"]
        }


def integrate_financial_statements(pars):
    print("==> Integrating of raw financial statements...")
    integrate_fs_invoker = IntegrateFinancialStatementsInvoker(**pars)
    integrate_fs_invoker.initiate_command()
    return integrate_fs_invoker._payload["fs_df"]
