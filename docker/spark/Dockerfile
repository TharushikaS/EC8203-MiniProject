# Speed-layer image: Apache Spark 3.5 + Kafka connector + pipeline code.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ENV TZ=UTC PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# Kafka source/sink for Structured Streaming and its transitive dependencies are baked into
# the image (instead of --packages at runtime) so start-up needs no internet access.
COPY docker/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python3 /tmp/fetch_jars.py /opt/spark/jars

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/smartgrid/src
ENV PYTHONPATH=/opt/smartgrid/src PYSPARK_PYTHON=python3
WORKDIR /opt/smartgrid

# 4040 = Spark UI (Structured Streaming tab), 9108 = Prometheus metrics
EXPOSE 4040 9108
CMD ["/opt/spark/bin/spark-submit", "--master", "local[4]", "--driver-memory", "1g", \
     "--conf", "spark.driver.extraJavaOptions=-XX:+UseG1GC", \
     "/opt/smartgrid/src/smartgrid/speed/streaming_job.py"]
