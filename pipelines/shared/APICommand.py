from .AbstractCommand import AbstractCommand


class APICommand(AbstractCommand):
    def __init__(self, **kwargs):
        self._invoker: APIInvoker = kwargs.get("api_invoker")
        self._fetcher: APIFetcher = kwargs.get("api_fetcher")

    def is_abort_invoker(self):
        return self._fetcher._is_error or False

    def is_skip_command(self):
        return False
