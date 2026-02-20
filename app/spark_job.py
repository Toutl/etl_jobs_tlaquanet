import os
from pyspark.sql import SparkSession


def main() -> None:
    spark = (SparkSession.builder.appName("Appto_SnowFlake").getOrCreate(),)

    # Read Postgres Data
    jdbc_url = os.getenv("POSTGRES_URL")

    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    df = (
        spark.read.jdbc(
            url=jdbc_url,
            table="user",
            properties=connection_properties,
        ),
    )

    print("Data Extracted from Application")
    df.show()

    # Write Data to Snowflake
    sfOptions = {
        "sfURL": f"{os.getenv("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_"),
    }

    (
        df.write.format("snowflake")
        .options(**sfOptions)
        .option("dbtable", "users")
        .mode("overwrite")
        .save()
    )

    print("Data Written to Snowflake")
    spark.stop()

    return None


if __name__ == "__main__":
    main()
