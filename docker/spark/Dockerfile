FROM eclipse-temurin:17-jre-jammy

ENV SPARK_VERSION=3.5.3 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
        | tar -xz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
    && curl -fSL -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar" \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/spark
