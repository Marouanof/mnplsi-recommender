FROM apache/airflow:2.7.0

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

USER airflow
RUN pip install pyspark==3.5.0 kafka-python pandas joblib numpy

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin