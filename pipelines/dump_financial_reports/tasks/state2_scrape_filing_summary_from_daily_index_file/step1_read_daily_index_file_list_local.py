import os


def read_daily_index_file_list():
    companysPath = os.path.expanduser("./data")
    return [
        {"Key": dailyIndex.path}
        for dailyIndex in os.scandir(companysPath)
        if dailyIndex.name.startswith("daily_")
    ]


# def main_with_local_file():
#     for daily_index_file in read_daily_index_file_list():
#         readDailyFilingIndexFileInvoker = ReadDailyFilingIndexFileInvoker(
#             daily_index_file
#         )
#         readDailyFilingIndexFileInvoker.initiate_command()
#         filing_summary_url_list = readDailyFilingIndexFileInvoker._payload[
#             "filing_summary_url_list"
#         ]

#     return filing_summary_url_list
