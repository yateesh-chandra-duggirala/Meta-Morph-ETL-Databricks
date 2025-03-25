FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

# Install required dependencies
RUN apt update && \
    apt-get install -y openjdk-17-jdk ant wget && \
    apt-get clean;

# Install Google Cloud Storage Connector for Spark
RUN mkdir -p /usr/local/airflow/jars/ && \
    wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P /usr/local/airflow/jars/

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set Google Cloud Authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json"

USER astro