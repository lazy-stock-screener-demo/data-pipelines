from bs4 import BeautifulSoup
from io import StringIO
import boto3
import re
import pandas as pd
from pipelines.shared.APICommand import APICommand
from pipelines.shared.APIInvoker import APIInvoker, ProgressPercentage
import pipelines.config.pathConfig as pathConfig


class FetchFilingSummaryAPIInvoker(APIInvoker):
    def __init__(self, **kwargs):
        super().__init__(self)
        self._input_config = kwargs.get("input_config")
        self._output_config = kwargs.get("output_config")
        self._command_queue_arr = [
            DispatchFetchFilingSummaryCommand(**self.set_default_pars),
            ExtractReportUrlFromFilingSummaryCommand(**self.set_default_pars),
            TransformFinancialStatements2TableCommand(**self.set_default_pars),
            SaveParticularDocumentInStorageCommand(**self.set_default_pars),
        ]
        super().__init__(self._command_queue_arr)
        print(
            "--------------- deal with cik, submission: ",
            (
                self._input_config["base_url"]["state_3"]["cik"],
                self._input_config["base_url"]["state_3"]["submission"],
            ),
            "---------------",
        )

    def get_report_url_from_filing_summary(self, soup):
        reports_arr = []
        matching_arr = []
        base_url = "%s%s/%s" % (
            self._input_config["base_url"]["state_3"]["base"],
            self._input_config["base_url"]["state_3"]["cik"],
            self._input_config["base_url"]["state_3"]["submission"],
        )
        financial_statements_specification = [
            (r"(.*\(comprehensive\))?\s*(income statements)\s*"),
            (r"(statements of comprehensive income)"),
            (r"(statements of income)"),
            (r"(\(consolidated\))?\s*(balance sheets)\s*(\(parenthetical\))?\s*"),
            (r"(cash\s*flows)\s*(statements)?"),
            (r"stockholders$|stockholders' equity$(\s*\(parenthetical\))?"),
        ]
        for report in soup.find("myreports").find_all("report")[:-1]:
            report_dict = {}
            report_regex = "|".join(
                "%s" % statement_name
                for statement_name in financial_statements_specification
            )
            matching_result = re.finditer(report_regex, report.shortname.text.lower())
            matching_result_arr = [i for i in matching_result]
            if len(matching_result_arr) > 0:
                report_dict["name_short"] = report.shortname.text.lower().capitalize()
                report_dict["name_long"] = report.longname.text.lower().capitalize()
                report_dict["category"] = report.menucategory.text
                report_dict["url"] = base_url + "/" + report.htmlfilename.text
                reports_arr.append(report_dict)
                matching_arr += [(report.shortname.text.lower(), *matching_result_arr)]
        print("==> Found total number of statements:", len(reports_arr))
        if len(reports_arr) < 10:
            print(*matching_arr, sep="\n")
        return reports_arr


class DispatchFetchFilingSummaryCommand(APICommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self):
        self._fetcher.dispatch_fetch_to_sec(
            self._invoker._input_config["sec"]["url"]["state_3"],
            self._invoker._input_config["sec"]["url_name"]["state_3"],
        )
        self._invoker._payload["raw_res"] = self._fetcher._res


class ExtractReportUrlFromFilingSummaryCommand(APICommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_xml(self, content):
        return BeautifulSoup(content, "lxml")

    def execute(self):
        soup = self.parse_xml(self._invoker._payload["raw_res"])
        self._invoker._payload["document_url"][
            "financial_statements_list"
        ] = self._invoker.get_report_url_from_filing_summary(soup)


class TransformFinancialStatements2TableCommand(APICommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def from_list_to_pd_table(content):
        return pd.DataFrame(content)

    def execute(self):
        self._invoker._payload["pd_full_table"] = self.from_list_to_pd_table(
            self._invoker._payload["document_url"]["financial_statements_list"]
        )


class SaveParticularDocumentInStorageCommand(APICommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def test_is_file_exist(self):
        if self._fetcher._is_error:
            print("==> File is Not Exist: %s" % self._fetcher._res)
            return False
        print(
            "==> Financial Statements Index File: ",
            self._invoker._output_config["file_name"]["state_3"],
            " is already exist at: ",
            self._invoker._output_config["s3"]["bucket_name"]["state_3"],
        )
        return True

    def before_save_to_s3(self):
        """
        If the file has already existed, we rather fetch get than directly post to s3 bucket.
        Get: 0.0004 USD
        PUT, COPY, POST, LIST: 0.005 USD
        """
        s3_client = boto3.client("s3")

        def check_object_is_exist_on_s3_bucket():
            return s3_client.get_object(
                Bucket=self._invoker._output_config["s3"]["bucket_name"]["state_3"],
                Key=self._invoker._output_config["file_name"]["state_3"],
            )

        self._fetcher.flush_res()
        self._fetcher.dispatch_fetch_to_s3(
            check_object_is_exist_on_s3_bucket,
            self._invoker._input_config["s3"]["post"]["state_3"],
        )

        return self.test_is_file_exist()

    def save_to_file(self, df_content):
        df_content.to_csv(
            pathConfig.data + self._invoker._output_config["file_name"]["state_3"],
            mode="w",
        )

    def save_to_s3(self, df_content):
        # equivalent with "with open"  => transfer into binary mode
        csv_buffer = StringIO()
        df_content.to_csv(csv_buffer)
        s3_client = boto3.client("s3")

        def wrap_s3_upload_fileobj_as_caller():
            print(
                "==> Save Financial Statements Index File: ",
                self._invoker._output_config["file_name"]["state_3"],
                " into ",
                self._invoker._output_config["s3"]["bucket_name"]["state_3"],
            )
            return s3_client.put_object(
                Body=csv_buffer.getvalue(),
                Bucket=self._invoker._output_config["s3"]["bucket_name"]["state_3"],
                Key=self._invoker._output_config["file_name"]["state_3"],
            )
            # Callback=ProgressPercentage(csv_buffer.getvalue(),
            #                             csv_buffer.seek(0, os.SEEK_END)))

        self._fetcher.dispatch_fetch_to_s3(
            wrap_s3_upload_fileobj_as_caller,
            self._invoker._output_config["s3"]["post"]["state_3"],
        )

    def execute(self):
        if self._invoker._output_config["local"]["is_enable"]:
            self.save_to_file(self._invoker._payload["pd_full_table"])
        if self._invoker._output_config["s3"]["is_enable"]:
            self.before_save_to_s3()
            self.save_to_s3(self._invoker._payload["pd_full_table"])


def scrape_filing_summary_file(pars):
    fsummary_api_invoker = FetchFilingSummaryAPIInvoker(**pars)
    fsummary_api_invoker.initiate_command()
