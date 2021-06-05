from .step1_fetch_daily_index_file_list_s3 import (
    fetch_daily_index_file_list as step1_fetch_daily_index_file_list,
)
from .step1_read_daily_index_file_list_local import (
    read_daily_index_file_list as step1_read_daily_index_file_list,
)
from .step2_fetch_filing_summary_url_list_s3 import (
    fetch_filing_summary_url_list as step2_fetch_filing_summary_url_list,
)
from .step2_read_filing_summary_url_list_local import (
    read_filing_summary_url_list as step2_read_filing_summary_url_list,
)
from .step3_scrape_filing_summary_file import (
    scrape_filing_summary_file as step3_scrape_filing_summary_file,
)
from pipelines.shared.utils import extract_pars, decorator_config


def build_pars_func(dataPars=dict()):
    input_config = dict(
        sec=dict(
            url_name=dict(
                state_3="Fetch Filing Summary File from sec",
            ),
        ),
        s3=dict(
            bucket_name=dict(
                state_1="reports-list-from-edgar-daily-index",
                state_2="reports-list-from-edgar-daily-index",
            ),
            url_name=dict(
                get=dict(
                    state_1="Query and Dump Daily Index File List from S3 bucket: reports-list-from-edgar-daily-index",
                    state_2="Query and Dump Daily Index File from S3",
                    state_3="Query Financial Statements File Index from s3",
                )
            ),
            archive_bucket_name=dict(
                state_2="reports-list-from-edgar-daily-index-archived"
            ),
        ),
    )
    output_config = dict(
        local=dict(is_enable=True),
        s3=dict(
            is_enable=False,
            url_name=dict(
                post=dict(state_3="Create Financial Statements File Index on s3"),
                delete=dict(state_3="Delete Financial Statements File Index on s3"),
                copy=dict(state_3="Copy Financial Statements File Index on s3"),
            ),
            bucket_name=dict(state_3="financial-index-from-filing-summary"),
        ),
    )
    if "url" in dataPars.keys():
        base_url = dict(
            base=extract_pars(dataPars["url"])[0],
            cik=extract_pars(dataPars["url"])[1],
            submission=extract_pars(dataPars["url"])[2],
        )
        input_config["base_url"] = dict(state_3=base_url)
        input_config["sec"]["url"] = dict(state_3=dataPars["url"])
        output_config["file_name"] = dict(
            state_3="fs_index-%s-%s.txt"
            % (
                base_url["cik"],
                base_url["submission"],
            )
        )
    if (
        "Key" in dataPars.keys()
    ):  # TODO: Update this key name as file name, should be s3 independent
        input_config["key"] = dict(state_2=dataPars["Key"])

    return dict(input_config=input_config, output_config=output_config)


# TODO separate s3 and local condig
def build_local_pars_func(dataPars=dict()):
    input_config = dict()
    output_config = dict()
    return dict(input_config=input_config, output_config=output_config)


def main():
    input_source = "local"
    run(dict(input_source=input_source))


def run(pars):
    # state1
    daily_index_file_list = []
    # [{
    #     'Key':'daily_20201030_10-K_index.txt'
    # }]
    if pars["input_source"] == "s3":
        daily_index_file_list = step1_fetch_daily_index_file_list(build_pars_func())
        print("S3: daily_index_file_list", daily_index_file_list)
    else:
        daily_index_file_list = step1_read_daily_index_file_list()
        print("Local: daily_index_file_list", daily_index_file_list)

    # state2
    for daily_index_file in daily_index_file_list:
        # daily_index_file = {
        #   'Key':'daily_20201030_10-K_index.txt'
        # }
        filing_summary_url_list = []
        if pars["input_source"] == "s3":
            filing_summary_url_list = decorator_config(
                step2_fetch_filing_summary_url_list
            )(build_pars_func, daily_index_file)
        else:
            filing_summary_url_list = decorator_config(
                step2_read_filing_summary_url_list
            )(build_pars_func, daily_index_file)
            print("Local: filing_summary_url_list", filing_summary_url_list)

        # state3: Common process for different data sources
        for filing_summary_url in filing_summary_url_list:
            # filing_summary_url = {
            #     "url": "https://www.sec.gov/Archives/edgar/data/59860/000105291820000283/FilingSummary.xml",
            # }
            decorator_config(step3_scrape_filing_summary_file)(
                build_pars_func, filing_summary_url
            )


if __name__ == "__main__":
    main()
