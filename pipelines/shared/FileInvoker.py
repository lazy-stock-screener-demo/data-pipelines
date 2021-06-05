import pandas as pd
from .AbstractInvoker import AbstractInvoker
from .FileCommand import FileCommand


class FileReader:
    def __init__(self, res):
        self._res = res

    @classmethod
    def dispatch_read_file(cls, file_url):
        return cls(cls.read_file(file_url))

    @staticmethod
    def read_file(file_url):
        try:
            return pd.read_csv(
                file_url,
                index_col=0,
                error_bad_lines=False,
                warn_bad_lines=False,
            )
        except Exception as x:
            print("Read CSV failed :(", x.__class__.__name__)


class FileInvoker(AbstractInvoker):
    def __init__(self, command_queue_arr):
        self._command_queue_arr = command_queue_arr
        self._payload = {
            "raw_data": None,
            "report_url_list": None,
            "filing_summary_url_list": None,
        }
        self.set_default_pars = {
            "file_invoker": self,
            "file_reader": FileReader(self._payload),
        }
