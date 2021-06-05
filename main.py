from pipelines.construct_stock_catalog.main_as_dag import (
    main as construct_stock_catalog_dag,
)

if __name__ == "__main__":
    # this main file is like local dag file, which assemable any given task together.

    # 1. dags: assemable tasks in dump financial reports dags

    # 2. dag: assemable tasks in construct stock catalog dags
    construct_stock_catalog_dag()
