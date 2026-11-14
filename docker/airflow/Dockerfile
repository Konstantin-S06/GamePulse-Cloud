FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s $(dirname $(dirname $(readlink -f $(which java)))) /opt/java
ENV JAVA_HOME=/opt/java

RUN mkdir /opt/gamepulse-venv && chown airflow:root /opt/gamepulse-venv

USER airflow
RUN python -m venv /opt/gamepulse-venv
COPY requirements/ /tmp/requirements/
RUN /opt/gamepulse-venv/bin/pip install --no-cache-dir -r /tmp/requirements/dev.txt \
    && curl -fSL -o /opt/gamepulse-venv/lib/python3.11/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fSL -o /opt/gamepulse-venv/lib/python3.11/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

ENV PYTHONPATH=/opt/gamepulse/src \
    PYSPARK_PYTHON=/opt/gamepulse-venv/bin/python \
    GP_PYTHON=/opt/gamepulse-venv/bin/python
