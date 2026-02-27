import os
from pyspark.sql import SparkSession


def main(table: str) -> None:
    spark = SparkSession.builder.appName("Appto_SnowFlake").getOrCreate()

    # Read Postgres Data
    jdbc_url = str(os.getenv("POSTGRES_URL"))

    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties=connection_properties,
    )

    print("Data Extracted from Application")
    df.show()

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
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )

    print("Data Written to Snowflake")
    spark.stop()

    return None


if __name__ == "__main__":
    main("users")
