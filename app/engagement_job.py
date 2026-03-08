import os
import snowflake.connector


def run_engagement_job():
    # Read SQL from file
    sql_path = "app/engagement_job.sql"
    if not os.getenv("IS_LOCAL") and not os.path.exists(sql_path):
        sql_path = "/app/engagement_job.sql"  # Local fallback

    with open(sql_path, "r") as f:
        engagement_sql = f.read()

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    print("Running engagement aggregation in Snowflake from SQL file...")
    conn.cursor().execute(engagement_sql)
    conn.close()
    print("Engagement job finished successfully")


if __name__ == "__main__":
    run_engagement_job()
