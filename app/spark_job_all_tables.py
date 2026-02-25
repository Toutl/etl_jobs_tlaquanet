import os
import snowflake.connector
from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder.appName("Appto_SnowFlake").getOrCreate()


def get_postgres_df(spark, table_name):
    # Read Postgres Data
    jdbc_url = str(os.getenv("POSTGRES_URL"))

    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    print("Data Extracted from Application")

    return spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties,
    )


def write_to_snowflake(df, table_name, mode="append"):
    # Write Data to Snowflake
    sfOptions = {
        "sfURL": f"{os.getenv("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_"),
    }

    print("Data Written to Snowflake")

    (
        df.write.format("snowflake")
        .options(**sfOptions)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )

    return None


def run_scd2_merge():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    merge_sql = """
                CREATE
                """

    conn.cursor().execute(merge_sql)
    conn.close()


def main():
    spark = get_spark()

    ###
    # Load Fact Tables: comments, events, likes, posts
    ###
    fact_tables = ["comments", "events", "likes", "posts"]
    fact_tables_analytics = [
        "comments_analytics",
        "events_analytics",
        "likes_analytics",
        "posts_analytics",
    ]

    for table, table_analytics in zip(fact_tables, fact_tables_analytics):
        print(f"Loading {table} ...")
        df = get_postgres_df(spark, table)
        write_to_snowflake(df, table_analytics)

    ###
    # Load Users to Staging
    ###
    print("Loading users staging ...")
    user_df = get_postgres_df(spark, "users")
    write_to_snowflake(user_df, "stg_users", mode="overwrite")

    ###
    # Execute SCD2 Merge
    ###
    print("Running SCD Type 2 Merge ...")
    run_scd2_merge()

    print("Pipeline Finished Successfully")
    spark.stop()


if __name__ == "__main__":
    main()
