from .AbstractCommand import AbstractCommand
from .SQLInvoker import SQLInvoker


class SQLRunCommand(AbstractCommand):
    def __init__(self, **kwargs):
        self._invoker: SQLInvoker = kwargs.get("sql_invoker")

    def is_abort_invoker(self):
        return False

    def is_skip_command(self):
        return False
