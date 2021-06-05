# Stock-Pipeline

![Preview](https://drive.google.com/uc?export=view&id=1Sb9thNNYSxRPbZ3wul-sy4rXRVg4TjNv)

## What is it?

It is a project that is part of lazy-stock-screener which is a full-stack micro-service prototype product.

## Why did I share this project in public?

The full product is still under constructing and the full product is committed to gitlab. I only share part of the task or `dag` in order to demonstrate how I built up the data-pipeline.

## What features do this project have?

- Abstract Factory pattern to create parameter catalog of each stock.
- Build profit/safety report with builder pattern
- Pipeline constructed by command pattern
- Separate panda out of from outputtemplate pattern in order to integrate with different data processing engine, e.g. spark.
- You have freedom to decide how to run on airflow as each `main_as_dag.py` was designed to execute as `dag` in airflow or execute [main.py](http://main.py) directly by cron job.

## How did I build this project?

Inspiration by [https://github.com/rjurney/Agile_Data_Code_2](https://github.com/rjurney/Agile_Data_Code_2)
![Data Pyramid](https://drive.google.com/uc?export=view&id=16RK6GezxAUrX6cbKPKKgPuq-L04622EU)

### Structure Overview

![Data Pipeline Process](https://drive.google.com/uc?export=view&id=1zztVdQUVK8AVJUPwQ1bek2_BJn1OQmyK)

### File Structure

- shared
- calculate_score
- dump_financial_reports
- construct_stock_catalog
  - catalog_builder
  - collection_factory
  - tasks
    - state_1_scrape_fs_file_from_s3
    - state_2_integration_and_cleaning
    - state_3_transformation
    - state_4_loading

## TechStacks

- Python
- Pandas

## References

- [https://github.com/rjurney/Agile_Data_Code_2](https://github.com/rjurney/Agile_Data_Code_2)
- [https://www2.slideshare.net/rjurney/predictive-analytics-with-airflow-and-pyspark](https://www2.slideshare.net/rjurney/predictive-analytics-with-airflow-and-pyspark)
- [https://blog.usejournal.com/testing-in-airflow-part-1-dag-validation-tests-dag-definition-tests-and-unit-tests-2aa94970570c](https://blog.usejournal.com/testing-in-airflow-part-1-dag-validation-tests-dag-definition-tests-and-unit-tests-2aa94970570c)
- [https://github.com/chandulal/airflow-testing/tree/master/src](https://github.com/chandulal/airflow-testing/tree/master/src)
