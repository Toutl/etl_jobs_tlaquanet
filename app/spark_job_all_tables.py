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

    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties,
    )

    print("Data Extracted from Application")

    return df


def write_to_snowflake(df, table_name, mode="append"):
    # Write Data to Snowflake
    sfOptions = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    (
        df.write.format("snowflake")
        .options(**sfOptions)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )

    print("Data Written to Snowflake")

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
                MERGE INTO dim_users AS tgt
                USING stg_users AS src
                    ON tgt.user_id = src.user_id
                    AND tgt.is_current = TRUE
                WHEN MATCHED AND tgt.display_name <> src.display_name THEN
                    UPDATE SET
                        tgt.to_date = CURRENT_TIMESTAMP(),
                        tgt.is_current = FALSE
                WHEN NOT MATCHED THEN
                    INSERT (
                        user_id,
                        username,
                        display_name,
                        created_at,
                        from_date,
                        to_date,
                        is_current
                    )
                    VALUES(
                        src.user_id,
                        src.username,
                        src.display_name,
                        src.created_at,
                        CURRENT_TIMESTAMP(),
                        NULL,
                        TRUE
                    );
                """

    conn.cursor().execute(merge_sql)
    conn.close()


def main():
    spark = get_spark()

    ###
    # Load Fact Tables: comments, events, likes, posts
    ###
    fact_tables = ["comments", "events", "likes", "posts"]

    for table in fact_tables:
        print(f"Loading {table} ...")
        df = get_postgres_df(spark, table)
        write_to_snowflake(df, table)

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
