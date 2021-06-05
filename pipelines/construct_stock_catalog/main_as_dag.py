import pandas as pd
from .tasks.state1_scrape_fs_file_from_s3.state1_1_fetch_fs_filelist_s3 import (
    fetch_raw_financial_statements_file_list,
)
from .tasks.state1_scrape_fs_file_from_local.state1_1_read_fs_filelist_local import (
    read_raw_financial_statements_file_list,
)
from .tasks.state1_scrape_fs_file_from_s3.state1_2_scrape_fs_file_s3 import (
    scrape_raw_financial_statements_from_s3,
)
from .tasks.state1_scrape_fs_file_from_local.state1_2_scrape_fs_file_local import (
    scrape_raw_financial_statements_from_local,
)
from .tasks.state2_integration_and_cleaning.state2_1_integration import (
    integrate_financial_statements,
)
from .tasks.state2_integration_and_cleaning.state2_2_cleaning import (
    clean_finanacial_statements,
)
from .tasks.state3_transformation.side_effects.build_records_index import (
    build_finanacial_records_index,
)
from .tasks.state3_transformation.state3_transformation import (
    create_finanacial_statements_table,
)
from .tasks.state4_loading.state4_loading import load_finanacial_statements_table
from pipelines.shared.utils import extract_pars, decorator_config
from .algorithm.main import main as generate_inverted_index


def build_par_func(dataPars=dict()):
    input_config = dict(
        s3=dict(
            bucket_name=dict(
                state_1="financial-statements-raw-reports",
            ),
            url_name=dict(
                get=dict(
                    state_1="Query and Dump Daily Index File List from S3 bucket: financial-statements-raw-reports"
                )
            ),
        )
    )
    output_config = dict(
        local=dict(
            file_name=dict(state_2="fs.txt"),
            index_file_name=dict(state_2="fs_index.txt"),
        )
    )
    if "cik" in dataPars.keys():
        input_config["cik"] = dict(
            state_1=dataPars["cik"], state_3=dataPars["cik"], state_4=dataPars["cik"]
        )
    if "financial_statements_list" in dataPars.keys():
        input_config["financial_statements_list"] = dict(
            state_1=dataPars["financial_statements_list"]
        )
    if "fs_list" in dataPars.keys():
        input_config["fs_list"] = dict(state_2=dataPars["fs_list"])
    if "fs_df" in dataPars.keys():
        input_config["fs_df"] = dict(
            state_2=dataPars["fs_df"], state_3=dataPars["fs_df"]
        )
    if "pars_table" in dataPars.keys():
        input_config["pars_table"] = dict(state_4=dataPars["pars_table"])
    return dict(input_config=input_config, output_config=output_config)


def main():
    # config_setting = dict(is_enable_local_cache=False)
    # config_name = dict(
    #     input_s3_bucket_name="financial-statements-raw-reports",
    #     output_s3_bucket_name="",
    # )
    # pars = dict(**config_setting, **config_name)
    # run(pars)

    # Test Inverted Index building
    generate_inverted_index()
    # Enable this
    # input_source = "local"
    # run(dict(input_source=input_source))


def run(pars):
    # state1_1
    raw_financial_statements_file_list = []
    # @raw_financial_statements_file_list = [{'cik': '59860','financial_statements_list': ["59860/fs-59860-000105291820000283-8._stockholders'_equity.txt"]}]
    if pars["input_source"] == "s3":
        # @raw_financial_statements_file_list = fetch_raw_financial_statements_file_list(pars)
        raw_financial_statements_file_list = fetch_raw_financial_statements_file_list(
            build_par_func()
        )
    else:
        raw_financial_statements_file_list = read_raw_financial_statements_file_list()
    # print("raw_financial_statements_file_list", raw_financial_statements_file_list)
    # state1_2
    for raw_financial_statements_file_obj in raw_financial_statements_file_list:
        print(
            "------------------------ cik: %s -------------------------------"
            % raw_financial_statements_file_obj["cik"],
        )
        # per Cik
        # @raw_financial_statements_file_obj = {
        #     "cik": "59860",
        #     "financial_statements_list": [
        #         "59860/fs-59860-000105291820000283-8._stockholders'_equity.txt"
        #     ],
        # }
        # print("raw_financial_statements_file_obj", raw_financial_statements_file_obj)
        if pars["input_source"] == "s3":
            financial_statements_df_list_obj = decorator_config(
                scrape_raw_financial_statements_from_s3
            )(build_par_func, raw_financial_statements_file_obj)
        else:
            financial_statements_df_list_obj = decorator_config(
                scrape_raw_financial_statements_from_local
            )(build_par_func, raw_financial_statements_file_obj)

        # state2
        # @financial_statements_df_list_obj = {
        #     "fs_list": [
        #         stockholders_equity_df,
        #         cash_flow_df,
        #         income_sheets_df,
        #         balance_df,
        #     ]
        # }
        fs_df = decorator_config(integrate_financial_statements)(
            build_par_func, financial_statements_df_list_obj
        )
        fs_df = decorator_config(clean_finanacial_statements)(build_par_func, fs_df)
        # @fs_df: {'fs_df': pd.DataFrame()}

        decorator_config(build_finanacial_records_index)(build_par_func, fs_df)

        # state3
        # @table_data = {'pars_table': {'MSFT': {'Net Income': XX, 'Operating Cash Flow': YY}},
        #                'price_table': {'MSFT':{'Price': XX } }}
        table_data = decorator_config(create_finanacial_statements_table)(
            build_par_func, fs_df
        )
        # state4 Loading to PostgreSQL
        decorator_config(load_finanacial_statements_table)(build_par_func, table_data)


def test_scrape_raw_financial_statements(pars):
    raw_financial_statements_obj = {
        "cik": "59860",
        "financial_statements_list": [
            "59860/fs-59860-000105291820000283-8._stockholders'_equity.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_balance_sheets.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_balance_sheets_-_parenthetical.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_statements_of_cash_flows.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_statements_of_income.txt",
        ],
    }
    scrape_raw_financial_statements(pars, raw_financial_statements_obj)


def test_integrate_financial_statements(pars):
    financial_statements_df_list_obj = {
        "financial_statements_df_list_obj": [
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]
    }
    integrate_financial_statements(pars, financial_statements_df_list_obj)


def test_clean_finanacial_statements(pars):
    cache_fs_df_obj_list = read_df_from_local_cache()
    for cache_fs_df_obj in cache_fs_df_obj_list:
        fs_df_obj = clean_finanacial_statements(pars, cache_fs_df_obj)
        create_finanacial_statements_table(pars, fs_df_obj)


def test_s_2(pars):
    raw_financial_statements_obj = {
        "cik": "59860",
        "financial_statements_list": [
            "59860/fs-59860-000105291820000283-8._stockholders'_equity.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_balance_sheets.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_balance_sheets_-_parenthetical.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_statements_of_cash_flows.txt",
            "59860/fs-59860-000105291820000283-Goldrich_mining_company_consolidated_statements_of_income.txt",
        ],
    }
    financial_statements_df_list_obj = scrape_raw_financial_statements(
        pars, raw_financial_statements_obj
    )

    integrate_financial_statements(pars, financial_statements_df_list_obj)


if __name__ == "__main__":
    main()
