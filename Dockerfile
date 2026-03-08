FROM apache/spark:3.5.0

USER root

# Setup workspace and permissions
RUN mkdir -p /home/spark/.ivy2/cache /app && \
    chown -R spark:spark /home/spark /app

# Install system dependencies and python libraries
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir snowflake-connector-python cryptography

# set environment variables for Spark/Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER spark
WORKDIR /app

# Copy the app contents and ensure the 'spark' user owns them
COPY --chown=spark:spark app/ /app/

# Default command (can be overridden by Airflow)
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", \
     "org.postgresql:postgresql:42.7.1,net.snowflake:spark-snowflake_2.12:6.1.0-spark_3.5,net.snowflake:snowflake-jdbc:3.15.0", \
     "/app/spark_job_all_tables.py"]
