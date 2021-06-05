from .AbstractInvoker import AbstractInvoker


class TableInvoker(AbstractInvoker):
    def __init__(self, command_queue_arr):
        self._command_queue_arr = command_queue_arr
        self._payload = {
            "statements": {},
            "cache_data": {},
            "table_data": {"pars_table": None, "price_table": None},
        }
        self.set_default_pars = {
            "table_invoker": self,
        }
