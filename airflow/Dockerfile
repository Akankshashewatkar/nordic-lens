FROM apache/airflow:2.9.1-python3.11

# Install project dependencies into the Airflow Python environment.
# The ingestion/ package itself is mounted at runtime (see docker-compose volumes),
# so we only install its third-party requirements here.
RUN pip install --no-cache-dir \
    # Data / Ingestion
    requests==2.31.0 \
    pandas==2.2.2 \
    pyarrow==15.0.2 \
    faker==24.3.0 \
    numpy==1.26.4 \
    scipy==1.13.0 \
    scikit-learn==1.4.2 \
    # AWS
    boto3==1.34.69 \
    # Snowflake
    snowflake-connector-python==3.9.1 \
    # dbt (for dag_dbt_transform)
    dbt-snowflake==1.8.3
