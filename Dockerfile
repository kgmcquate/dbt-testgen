FROM python:3.11

RUN pip install dbt-core duckdb dbt-duckdb dbt-postgres dbt-redshift dbt-snowflake dbt-bigquery

WORKDIR /run/dbt-testgen/

ENTRYPOINT [ "bash" ]