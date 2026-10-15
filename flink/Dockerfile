FROM flink:1.20.3-scala_2.12-java17
USER root
RUN wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar -P /opt/flink/lib/ \
 && wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.3/flink-sql-avro-confluent-registry-1.20.3.jar -P /opt/flink/lib/
USER flink
