from .AbstractCommand import AbstractCommand


class FileCommand(AbstractCommand):
    def __init__(self, **kwargs):
        self._invoker: FileInvoker = kwargs.get("file_invoker")
        self._reader: FileReader = kwargs.get("file_reader")

    def is_abort_invoker(self):
        return False

    def is_skip_command(self):
        return False
