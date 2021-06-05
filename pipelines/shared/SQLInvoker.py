from .AbstractInvoker import AbstractInvoker


class SQLFetcher:
    def __init__(self, res):
        self._is_error = None
        self._res = res


class SQLInvoker(AbstractInvoker):
    def __init__(self, command_queue_arr):
        self._command_queue_arr = command_queue_arr
        self._payload = {"table_data": {"pars_table": "", "price_table": ""}}
        self.set_default_pars = {
            "sql_invoker": self,
            "sql_reader": SQLFetcher(self._payload),
        }
