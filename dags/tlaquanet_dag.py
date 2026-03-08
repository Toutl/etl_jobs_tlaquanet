from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "tlaquanet_etl_snowflake",
    default_args=default_args,
    description="ETL TlaquaNet from Postgres to Snowflake with SCD2 and Engagement",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tlaquanet", "spark", "snowflake"],
) as dag:

    # Task 1: Run  Spark ETL (Postgres -> Snowflake + SCD2)
    run_spark_etl = DockerOperator(
        task_id="run_spark_snowflake_etl",
        image="etl_jobs_tlaquanet-spark:latest",
        api_version="auto",
        auto_remove=True,
        environment={
            "POSTGRES_URL": "jdbc:postgresql://host.docker.internal:5432/tlaquanet",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
            "SNOWFLAKE_DATABASE": "TLAQUANET_ANALYTICS",
            "SNOWFLAKE_SCHEMA": "PUBLIC",
        },
        network_mode="bridge",
        command="opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.1,net.snowflake:spark-snowflake_2.12:6.1.0-spark_3.5,net.snoflake:snoflake-jdbc:3.15.0 /app/spark_job_all_tables.py",
    )

    # Task 2: Run Engagement Aggregation Job
    run_engagement_aggregation = DockerOperator(
        task_id="run_engagement_aggregation",
        image="etl_jobs_tlaquanet-spark:latest",
        api_version="auto",
        auto_remove=True,
        environment={
            "SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
            "SNOWFLAKE_DATABASE": "TLAQUANET_ANALYTICS",
            "SNOWFLAKE_SCHEMA": "PUBLIC",
        },
        network_mode="bridge",
        command="python3 /app/engagement_job.py",
    )

    run_spark_etl >> run_engagement_aggregation
