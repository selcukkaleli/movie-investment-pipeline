FROM apache/spark:3.5.0

USER root

RUN pip install google-cloud-storage google-cloud-bigquery pyspark

# GCS connector JAR'ını indir
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# BigQuery connector JAR'ını indir  
RUN curl -L https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
    -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar