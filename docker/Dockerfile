# docker/Dockerfile
# Use Debian bookworm so JRE 17 is available (Spark 3.5 is happy with Java 17)
FROM python:3.12-bookworm

# Java 17 + tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl ca-certificates make \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV SPARK_VERSION=3.5.1 HADOOP_VERSION=3
RUN curl -fsSLo /tmp/spark.tgz \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
 && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace