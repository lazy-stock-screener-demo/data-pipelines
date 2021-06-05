from .tasks.state1_fetch_ticker_cik_mapping_list.main_as_task import (
    main as fetch_ticker_cik_mapping_list,
)
from .tasks.state1_fetch_daily_index_list.main_as_task import (
    main as fetch_daily_index_list,
)
from .tasks.state1_fetch_full_index_list.main_as_task import (
    main as fetch_full_index_list,
)
from .tasks.state2_scrape_filing_summary_from_daily_index_file.main_as_task import (
    main as scrape_filing_summary,
)
from .tasks.state3_scrape_financial_statements_from_filing_summary_file.main_as_task import (
    main as state3_scrape_financial_statements,
)

# It is a dag: assemable tasks in dump financial reports dags
def main():
    run()


def run():
    # 1. Prerequisite
    fetch_ticker_cik_mapping_list()

    # 2. Fetching Index list
    fetch_full_index_list()
    fetch_daily_index_list()  # => output to local/s3

    # 3.1 scrape filling summary based on Index list
    scrape_filing_summary()  # => output?

    # 3.2 scrape document 10k file object based on Index list
    scrape_document_10k()  # => output

    # 4. scrape financial statement
    state3_scrape_financial_statements()  # => output


if __name__ == "__main__":
    main()
