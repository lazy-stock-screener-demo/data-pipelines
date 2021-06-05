from .AbstractCommand import AbstractCommand


class TableCommand(AbstractCommand):
    def __init__(self, **kwargs):
        self._invoker: TableInvoker = kwargs.get("table_invoker")

    def is_abort_invoker(self):
        return False

    def is_skip_command(self):
        return False
