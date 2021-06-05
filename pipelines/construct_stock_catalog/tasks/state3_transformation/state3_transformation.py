from pipelines.shared.TableCommand import TableCommand
from pipelines.shared.TableInvoker import TableInvoker
from pipelines.construct_stock_catalog.collection_factory.CollectionFactories import (
    CreateCollectionApp,
    CatalogCollectionFactory,
    PriceCollectionFactory,
)


class CreateFinancialStatementsInvoker(TableInvoker):
    def __init__(self, **kwargs):
        super().__init__(self)
        self._input_config = kwargs.get("input_config")
        # self._is_enable_local_cache = kwargs.get("is_enable_local_cache")
        # self._fs_df_list_obj = kwargs.get("fs_df")
        self._command_queue_arr = [
            CreateParsCollectionCommand(**self.set_default_pars),
            # CreatePriceCollectionCommand(**self.set_default_pars),
        ]
        super().__init__(self._command_queue_arr)


class CreateParsCollectionCommand(TableCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self):
        # print(("df \n {0}").format(self._invoker._input_config["fs_df"]["state_3"]))
        pars = dict(
            cik="MSFT",
            fs_df=self._invoker._input_config["fs_df"]["state_3"],
        )
        factory = dict(factory=CatalogCollectionFactory(**pars))
        app = CreateCollectionApp(**factory)
        self._invoker._payload["table_data"]["cik"] = "MSFT"
        self._invoker._payload["table_data"]["pars_table"] = app.create()
        # print(self._invoker._payload["table_data"]["pars_table"])


class CreatePriceCollectionCommand(TableCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self):
        factory_pars = dict(fs_df=self._invoker._input_config["fs_df"]["state_2"])
        factory = dict(factory=PriceCollectionFactory(**factory_pars))
        app = CreateCollectionApp(**factory)
        self._invoker._payload["table_data"]["price_table"] = app.create()


def create_finanacial_statements_table(pars):
    print("==> Creating Financial Statements Tables...")
    tfs_invoker = CreateFinancialStatementsInvoker(**pars)
    tfs_invoker.initiate_command()
    return tfs_invoker._payload["table_data"]