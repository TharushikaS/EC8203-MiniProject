"""Download the Kafka connector jars for Spark Structured Streaming at image build time."""
import sys
import urllib.request

MAVEN = "https://repo1.maven.org/maven2"
JARS = [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
]

target = sys.argv[1]
for jar in JARS:
    name = jar.rsplit("/", 1)[1]
    urllib.request.urlretrieve(f"{MAVEN}/{jar}", f"{target}/{name}")
    print("downloaded", name)
