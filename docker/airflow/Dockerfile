# Orchestration + batch-layer image: Airflow 2.10 with a local PySpark runtime.
# Batch Spark jobs run as spark-submit (local mode) launched by Airflow tasks; in production
# the same job would be submitted to a YARN / Kubernetes / standalone cluster instead.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17 \
 && mkdir -p /opt/jars \
 && curl -fsSL -o /opt/jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
ENV JAVA_HOME=/usr/lib/jvm/java-17 TZ=UTC

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt

COPY --chown=airflow:root src /opt/smartgrid/src
COPY --chown=airflow:root tests /opt/smartgrid/tests
COPY --chown=airflow:root scripts /opt/smartgrid/scripts
ENV PYTHONPATH=/opt/smartgrid/src PYSPARK_PYTHON=python3
