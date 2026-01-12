FROM confluentinc/cp-kafka-connect:7.8.2

USER root

RUN mkdir -p /connect-plugins/snowflake

# Snowflake Kafka Connector JAR (3.4.0)
RUN curl -L -o /connect-plugins/snowflake/snowflake-kafka-connector-3.4.0.jar   https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/3.4.0/snowflake-kafka-connector-3.4.0.jar

RUN chmod -R 755 /connect-plugins

USER appuser
