# check=skip=SecretsUsedInArgOrEnv
# (False positive: it flags CONNECT_KEY_CONVERTER. No secrets are baked into this image.)
#
# Kafka Connect worker with the Snowflake Kafka connector (v4, high-performance Snowpipe Streaming).
# Used by both docker/docker-compose.yml and the Minikube manifests in kubernetes/.
#
# The connector ships a Rust-based native library, so the runtime image must be glibc-based
# (Confluent's UBI image), not Alpine. cp-kafka-connect 7.9.x runs Kafka Connect 3.9, the API
# version the connector is built against.

ARG SNOWFLAKE_CONNECTOR_VERSION=4.2.0
ARG SNOWFLAKE_CONNECTOR_SHA1=64abccc5d644c453117e46b6c94d05b8708f89a8
ARG BC_FIPS_VERSION=2.1.0
ARG BC_FIPS_SHA1=c8df3d47f9854f3e9ca57e9fc862da18c9381fa9
ARG BCPKIX_FIPS_VERSION=2.1.8
ARG BCPKIX_FIPS_SHA1=38bf2d32f177a6d2c3e7b316a8673ae3f3c18516

# --- Stage 1: download plugin jars from Maven Central and verify their checksums ---
FROM alpine:3.20 AS plugins
ARG SNOWFLAKE_CONNECTOR_VERSION SNOWFLAKE_CONNECTOR_SHA1 BC_FIPS_VERSION BC_FIPS_SHA1 BCPKIX_FIPS_VERSION BCPKIX_FIPS_SHA1
WORKDIR /plugins/snowflake-kafka-connector
RUN set -eu; \
    maven=https://repo1.maven.org/maven2; \
    wget -q "$maven/com/snowflake/snowflake-kafka-connector/${SNOWFLAKE_CONNECTOR_VERSION}/snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.jar"; \
    wget -q "$maven/org/bouncycastle/bc-fips/${BC_FIPS_VERSION}/bc-fips-${BC_FIPS_VERSION}.jar"; \
    wget -q "$maven/org/bouncycastle/bcpkix-fips/${BCPKIX_FIPS_VERSION}/bcpkix-fips-${BCPKIX_FIPS_VERSION}.jar"; \
    printf '%s  %s\n' \
      "$SNOWFLAKE_CONNECTOR_SHA1" "snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.jar" \
      "$BC_FIPS_SHA1" "bc-fips-${BC_FIPS_VERSION}.jar" \
      "$BCPKIX_FIPS_SHA1" "bcpkix-fips-${BCPKIX_FIPS_VERSION}.jar" \
      | sha1sum -c -

# --- Stage 2: Connect worker ---
FROM confluentinc/cp-kafka-connect:7.9.10
COPY --from=plugins /plugins/ /usr/share/confluent-hub-components/

# Worker defaults shared by Compose and Kubernetes. Only CONNECT_BOOTSTRAP_SERVERS and
# CONNECT_REST_ADVERTISED_HOST_NAME differ per environment and are set there.
ENV CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components \
    CONNECT_REST_PORT=8083 \
    CONNECT_GROUP_ID=telematics-connect \
    CONNECT_CONFIG_STORAGE_TOPIC=_connect-configs \
    CONNECT_OFFSET_STORAGE_TOPIC=_connect-offsets \
    CONNECT_STATUS_STORAGE_TOPIC=_connect-status \
    CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
    CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false \
    CONNECT_LOG4J_LOGGERS=org.reflections=ERROR

# Connector configs reference secrets as ${env:VAR}. The worker resolves them from its own
# environment at task start, so credentials never pass through the REST API and are never
# stored in the _connect-configs topic.
ENV CONNECT_CONFIG_PROVIDERS=env \
    CONNECT_CONFIG_PROVIDERS_ENV_CLASS=org.apache.kafka.common.config.provider.EnvVarConfigProvider

# The connector's Snowpipe Streaming SDK buffers off-heap, so keep the heap to ~50% of the
# container memory limit (2 GiB in Compose and Kubernetes).
ENV KAFKA_HEAP_OPTS="-Xms512m -Xmx1g"
