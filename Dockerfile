FROM apache/spark:3.5.0

USER root

RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark

USER spark

WORKDIR /app

COPY app/ /app/

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", \
     "org.postgresql:postgresql:42.7.1,net.snowflake:spark-snowflake_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.15.0", \
     "/app/spark_job_all_tables.py"]
