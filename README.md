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
* **PostgreSQL Source**: Transactional PostgreSQL 16 database (`telematics_legacy`) managing core equipment metadata and historical hour-meter logs, seeded from the Module 1 fleet, with least-privilege replication and application roles. A simulated legacy application keeps it changing.
* **Debezium CDC Pipeline**: Debezium 3.7 PostgreSQL connector using `pgoutput` logical decoding. It migrates the existing rows with an initial snapshot, then streams real-time row changes (`INSERT`, `UPDATE`, `DELETE`) with full before and after images into versioned Kafka topics, governed by a data contract.
* **Bronze Landing**: The Snowflake sink lands the raw Debezium envelopes in Bronze CDC tables within the same 120-second SLA, measured from the Postgres commit. It runs on Docker Compose and Minikube and is exercised end to end in CI.

## Module 4: Modern Data Warehouse & Medallion Transformation (`04_data_warehouse`)
* **Bronze Tier**: Landing zone for raw, unparsed JSON payloads originating from both IoT streams and Debezium CDC logs, declared as dbt sources with freshness checks.
* **Silver Tier**: dbt models that parse, type and deduplicate the raw payloads into relational tables. Incremental merges run on the Bronze load time. Every telemetry reading is checked against a signal catalog generated from the simulator config, and anomalies are flagged rather than dropped. SCD Type 2 equipment history and current-state hour-meter logs are rebuilt from the Debezium change log.
* **Gold Tier**: Kimball star schema with an SCD2 equipment dimension joined as of each event's time, signal and date dimensions, hourly equipment, sensor and data-quality facts, and an equipment health mart. Every Gold model has an enforced dbt contract.
* **Testing & Quality Assurance**: dbt tests for keys, relationships and SCD2 integrity, warn-level anomaly-rate thresholds, dbt unit tests, and a Gold-to-Silver reconciliation check.
* **Orchestration**: dbt Core runs every 5 minutes as a Kubernetes CronJob (Minikube), on demand through Docker Compose, with its own least-privilege Snowflake role and key-pair service user.

## Module 5: Dashboard Analytics & Monitoring (`05_dashboard_analytics`)
* **Grafana Integration**: Connected Grafana visualization dashboards directly to Snowflake data warehouse models (Silver/Gold layers).
* **Operational Monitoring**: Real-time tracking of machine health metrics, equipment status, and pipeline execution.