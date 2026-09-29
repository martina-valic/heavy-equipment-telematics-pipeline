# Heavy Equipment Telematics Platform - Architecture & Module Breakdown

## System Overview
A production-grade, end-to-end data platform designed to ingest, synchronize, and transform real-time IoT telematics and legacy PostgreSQL Change Data Capture (CDC) data into a unified Snowflake data warehouse using Apache Kafka, Kafka Connect, and dbt.

---

## Module 1: Real-Time IoT Telemetry Stream (`01_telemetry_simulator`)
* **Equipment Simulator**: Python-based telemetry simulation engine generating continuous, real-time machine metric payloads (e.g., engine temperature, vibration levels, fluid pressures, GPS coordinates).
* **Event Broker**: Apache Kafka topics configured to handle high-throughput streaming of IoT JSON payloads with strict data contracts for schema enforcement.
* **CI/CD**: GitHub Actions workflows that run unit tests, data contract checks, and Docker Compose validation on every push, extended as each later module is added.

## Module 2: Real-Time Ingestion & Streaming (`02_streaming_ingestion`)
* **Kafka Connect Infrastructure**: Kafka (KRaft mode), Kafka Connect, and Kafka UI, containerized with Docker Compose for local development and deployed to a Kubernetes (Minikube) cluster.
* **Snowflake Sink Connector**: Snowflake Connector for Kafka v4 (high-performance Snowpipe Streaming) with idempotent multi-connector registration through the REST API. Secrets are hydrated from environment variables on the worker and never stored in connector configs.
* **Data Routing**: Streams the telemetry topic and its dead-letter topic into Snowflake Bronze landing tables within a 120-second ingestion SLA.

## Module 3: Legacy Database Migration & Change Data Capture (`03_cdc_migration`)
* **PostgreSQL Source (`03_cdc_migration`)**: Local transactional PostgreSQL (SQL) database (`telematics_legacy`) managing core equipment metadata and historical hour-meter logs.
* **Debezium CDC Pipeline**: Configured and registered the Debezium PostgreSQL connector using `pgoutput` logical decoding to stream real-time row changes (`INSERT`, `UPDATE`, `DELETE`) into Kafka topics.

## Module 4: Modern Data Warehouse & Medallion Transformation (`04_data_warehouse`)
* **Bronze Tier**: Landing zone for raw, unparsed JSON payloads originating from both IoT streams and Debezium CDC logs.
* **Silver Tier**: dbt models parsing, casting, cleaning, and structuring raw payloads into typed relational schemas.
* **Gold Tier**: Aggregated business logic, operational summaries, and equipment health metrics modeled using Kimball Methodologies and Star Schema design optimized for reporting.
* **Testing & Quality Assurance**: Comprehensive dbt test suites validating schema integrity, uniqueness, and relational constraints.

## Module 5: Dashboard Analytics & Monitoring (`05_dashboard_analytics`)
* **Grafana Integration**: Connected Grafana visualization dashboards directly to Snowflake data warehouse models (Silver/Gold layers).
* **Operational Monitoring**: Real-time tracking of machine health metrics, equipment status, and pipeline execution.