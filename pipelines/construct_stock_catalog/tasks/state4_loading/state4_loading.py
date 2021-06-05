from pipelines.shared.SQLRunCommand import SQLRunCommand
from pipelines.shared.SQLInvoker import SQLInvoker

# from pipelines.construct_stock_catalog.infra.ormSqlalchemy.ParsRepoSqlalchemy import (
#     parsRepo,
# )
from pipelines.construct_stock_catalog.infra.mongoEngine.CatalogRepoMongoEngine import (
    catalogRepo,
)


class StockCatalogSQLInvoker(SQLInvoker):
    def __init__(self, **kwargs):
        super().__init__(self)
        self._input_config = kwargs.get("input_config")
        self._command_queue_arr = [
            StockCatalogSQLRunCommand(**self.set_default_pars),
        ]
        super().__init__(self._command_queue_arr)


class StockCatalogSQLRunCommand(SQLRunCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self):
        report = self._invoker._input_config["pars_table"]["state_4"]
        cik = self._invoker._input_config["cik"]["state_4"]
        catalogRepo().save(report, cik)


def load_finanacial_statements_table(pars):
    print("==> Loading Financial Statements Tables...")
    load_sql_invoker = StockCatalogSQLInvoker(**pars)
    load_sql_invoker.initiate_command()
    return load_sql_invoker._payload["table_data"]
