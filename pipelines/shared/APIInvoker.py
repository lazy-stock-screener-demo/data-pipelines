import time
import requests
import logging
import pandas as pd
import re
import sys
import json
import traceback
import os
import boto3
import threading
from io import StringIO
from botocore.exceptions import ClientError
from datetime import datetime, date, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from .AbstractInvoker import AbstractInvoker
from .APICommand import APICommand
from .Result import Result
from .EitherError import errorInstance, successInstance

log = logging.getLogger(__name__)


class ProgressPercentage(object):
    def __init__(self, filename, size=None):
        self._filename = filename
        self._size = float(size)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


class APIFetcher:
    def __init__(self, res):
        self._is_error = None
        self._res = res

    def flush_res(self):
        self._is_error = None
        self._res = None

    def panda_read_from_html(self, url, url_name):
        self._is_error = False
        self._res = pd.read_html(url, index_col=0)

    def dispatch_fetch_to_s3(self, caller, url_name):
        eitherError = self.fetch_with_boto3(caller, url_name)
        self.fetch_callback(eitherError)

    @staticmethod
    def fetch_with_boto3(caller, url_name):
        t0 = time.time()
        try:
            try:
                response = caller()
            except ClientError as e:
                print("ClientError:(", e.__class__.__name__)
                return errorInstance(Result.fail(e))
            else:
                if response is None:
                    return successInstance(Result.ok(response))
                elif "ResponseMetadata" in response:
                    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                        print("==> Process:", url_name)
                        print(
                            "==> AWS api status code",
                            response["ResponseMetadata"]["HTTPStatusCode"],
                        )
                        if "Body" in response:
                            return successInstance(
                                Result.ok(
                                    pd.read_csv(
                                        StringIO(
                                            response["Body"].read().decode("utf-8")
                                        ),
                                        index_col=0,
                                    )
                                )
                            )
                        elif "Contents" in response:
                            return successInstance(Result.ok(response["Contents"]))
                        else:
                            return successInstance(
                                Result.ok(response["ResponseMetadata"])
                            )
                    else:
                        return errorInstance(Result.fail(response["ResponseMetadata"]))
                else:
                    return successInstance(Result.ok(response))  # ??
            finally:
                t1 = time.time()
                print("==> Took", t1 - t0, "seconds to fetch with boto 3.")

        except Exception as x:
            print("Unexpected Error: ", traceback.print_exc())
            return errorInstance(Result.fail(x))

    def dispatch_fetch_to_sec(self, url, url_name):
        def wrap_request_get_as_caller():
            return self.request_retry_session().get(url)

        eitherError = self.fetch_with_request(wrap_request_get_as_caller, url_name)
        self.fetch_callback(eitherError)

    def fetch_callback(self, eitherError):
        if eitherError.isSuccess():
            self._is_error = eitherError.isError()
            self._res = eitherError.result.getValue()
        else:
            self._is_error = eitherError.isError()
            self._res = eitherError.result.errorValue()

    @staticmethod
    def fetch_with_request(api_caller, url_name):
        t0 = time.time()
        try:
            try:
                response = api_caller()
            except requests.exceptions.RequestException as x:
                print("Request failed :(", x.__class__.__name__)
                return errorInstance(x)
            else:
                t1 = time.time()
                print("Took", t1 - t0, "seconds to fetch with request from", url_name)
                print("==> request api status code", response.status_code)
                if response.status_code == 200 or response.status_code == 201:
                    # log.info('Request status %s', response.status_code)
                    return successInstance(Result.ok(response.content))
                else:
                    return errorInstance(Result.fail(response))
        except Exception as x:
            print("UnexpectedError", x.__class__.__name__)
            return errorInstance(x)

    @staticmethod
    def request_retry_session(
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # @classmethod
    # def dispatch_fetch(cls, url, url_name):
    #     # will create another instance compared with self._fetcher => idea of immutable?
    #     eitherError = cls.fetch_with_request(
    #         url, cls.request_retry_session, url_name)
    #     if (eitherError.isSuccess()):
    #         return cls(eitherError.isSuccess(), eitherError.result.getValue())
    #     else:
    #         return cls(eitherError.isSuccess(), eitherError.result.errorValue())

    # @classmethod
    # def panda_read_from_html(cls, url, url_name):
    #     return cls(pd.read_html(url, index_col=0))


class APIInvoker(AbstractInvoker):
    def __init__(self, command_queue_arr):
        self._command_queue_arr = command_queue_arr
        self._payload = {
            "raw_res": None,
            "pd_full_table": None,
            "document_url": {
                "10-K": None,
                "financial_statements_list": [],
            },
            "table_document": {},
            "financial_statements_df_list": [],
            "financial_statements_df_dict": {},
        }
        self.set_default_pars = {
            "api_invoker": self,
            "api_fetcher": APIFetcher(self._payload),
        }

    def isWorkDay(self):
        return True
        if 0 <= datetime.today().weekday() <= 4:
            return True
        return False

    # def getSeasons(self):
    #     # https://stackoverflow.com/questions/16139306/determine-season-given-timestamp-in-python-using-datetime
    #     now = self.transformFromDatetime2Date(datetime.today())
    #     this_year = now.year
    #     seasons = [
    #         ("QTR1", (date(this_year, 1, 1), date(this_year, 3, 31))),
    #         ("QTR2", (date(this_year, 4, 1), date(this_year, 6, 30))),
    #         ("QTR3", (date(this_year, 7, 1), date(this_year, 9, 30))),
    #         ("QTR4", (date(this_year, 10, 1), date(this_year, 12, 31))),
    #     ]
    #     return next(season for season, (start, end) in seasons if start <= now <= end)

    # def getDate(self):
    #     # print(datetime.today())
    #     # print(datetime.today() - timedelta(1))
    #     return self.transformFromDatetime2Date(
    #         datetime.today() - timedelta(1)
    #     ).strftime("%Y%m%d")

    # def transformFromDatetime2Date(self, now):
    #     if isinstance(now, datetime):
    #         now = now.date()
    #     return now

    # def extract_pars(self, url):
    #     # idea is from https://stackoverflow.com/questions/6260777/python-regex-to-parse-string-and-return-tuple
    #     match = re.compile(
    #         r"""
    #                          (?P<base>^.*\/data\/?)
    #                          (?P<cik>\d*)
    #                          \/(?P<submission>\d*)
    #                          \/\w+
    #                          """,
    #         re.VERBOSE,
    #     ).match(url)
    #     base = match.group("base")
    #     cik = match.group("cik")
    #     submission = match.group("submission")
    #     return (base, cik, submission)
