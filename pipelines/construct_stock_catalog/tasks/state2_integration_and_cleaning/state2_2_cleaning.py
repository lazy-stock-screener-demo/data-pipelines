import re
import os
import copy
import numpy as np
import datetime
from pipelines.shared.TableCommand import TableCommand
from pipelines.shared.TableInvoker import TableInvoker
import pipelines.config.pathConfig as pathConfig
import logging

logger = logging.getLogger("Clean Financial Statements Invoker")

######################Clean Financial Statements Invoker###########################
class CleanFinancialStatementsInvoker(TableInvoker):
    def __init__(self, **kwargs):
        super().__init__(self)
        self._input_config = kwargs.get("input_config")
        self._output_config = kwargs.get("output_config")
        # self._fs_df = kwargs.get("fs_df")
        self._command_queue_arr = [
            CleanValueInDataFrameCommand(**self.set_default_pars),
            SaveFinancialStatements2FileCommand(**self.set_default_pars),
        ]
        super().__init__(self._command_queue_arr)


class CleanValueInDataFrameCommand(TableCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def clean_value(item):
        fs_groups_specification = [
            ("numberized", r"[$,)]"),
            ("turn_brackets_2_minus", r"[(]"),
        ]
        report_regex = "|".join(
            "(?P<%s>%s)" % group for group in fs_groups_specification
        )
        return re.sub(
            report_regex,
            lambda x: {
                "numberized": "",
                "turn_brackets_2_minus": "-",
            }[x.lastgroup],
            item,
        )

    def get_index_column_based_on_year(self, df):
        date_dict = {}
        date_index_list = []

        def find_column_name_end_with_point_one(column_head):
            pattern = "^[\w\W]*(.1)$"
            return re.match(pattern, column_head)

        # year_as_column_head: Jun. 30, 2018
        for index, year_as_column_head in enumerate(df):
            # date_obj: 2017-06-30 00:00:00
            if not find_column_name_end_with_point_one(year_as_column_head):
                date_obj = datetime.datetime.strptime(
                    year_as_column_head[:13], "%b. %d, %Y"
                )
                if date_obj.year in date_dict:
                    date_dict[date_obj.year].append(index)
                else:
                    date_dict[date_obj.year] = [index]
        # date_dict = {2019: [0, 1, 3], 2018: [2]}
        return date_dict

    def build_df_with_unique_year_column(self, df, index_dict):
        def exclude_nan(x):
            # print(("Exclude_nan\n {0}").format(x.dropna()))
            condition = x.dropna().size
            if condition > 0:
                # print("Wrong", x.dropna().sort_values(ascending=False).iloc[0])
                # print("wrong2", x.dropna())
                return x.dropna().sort_values(ascending=False).iloc[0]
            else:
                return np.nan

        # print(("index_dict\n {0}").format(index_dict))
        for year in index_dict:
            if len(index_dict[year]) > 1:
                # df = df
                df[str(year) + "_new"] = df[df.columns[index_dict[year]]].apply(
                    lambda x: exclude_nan(x), axis=1
                )
            else:
                df[str(year) + "_new"] = df[df.columns[index_dict[year]]]
        # print(("df \n {0}").format(df))
        return df

    def use_unique_columns_as_record(self, df):
        df = df[df.columns[df.columns.str.contains("_new")]]
        df.columns = df.columns.str.rstrip("_new")
        return df

    def execute(self):
        df = self._invoker._input_config["fs_df"]["state_2"].astype(str)
        df = df.applymap(self.clean_value)
        df = df.replace("nan", np.nan)
        df = self.build_df_with_unique_year_column(
            df, self.get_index_column_based_on_year(df.columns)
        )
        df = self.use_unique_columns_as_record(df)
        # print(("df \n {0}").format(df))
        self._invoker._payload["fs_df"] = {"fs_df": df}


class SaveFinancialStatements2FileCommand(TableCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def save_to_file(self, df_content, file_name):
        df_content.to_csv(
            file_name,
            mode="w",
        )

    def execute(self):
        self.save_to_file(
            self._invoker._payload["fs_df"]["fs_df"],
            pathConfig.data
            + self._invoker._output_config["local"]["file_name"]["state_2"],
        )

        self.save_to_file(
            self._invoker._payload["fs_df"]["fs_df"].index.to_series(),
            pathConfig.data
            + self._invoker._output_config["local"]["index_file_name"]["state_2"],
        )


def clean_finanacial_statements(pars):
    print("==> Cleaning of financial statements...")
    try:
        clean_fs_invoker = CleanFinancialStatementsInvoker(**pars)
        clean_fs_invoker.initiate_command()
        return clean_fs_invoker._payload["fs_df"]
    except Exception as e:
        logger.info(logging.INFO, "Error message is:" + str(e))