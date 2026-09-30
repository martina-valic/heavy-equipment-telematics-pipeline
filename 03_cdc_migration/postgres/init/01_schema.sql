-- Module 3: legacy fleet database (telematics_legacy), schema "fleet".
--
-- Runs automatically on first start of the Postgres container (docker-entrypoint-initdb.d), as the
-- superuser from POSTGRES_USER. Idempotent, so it is also safe to re-apply by hand:
--   docker compose -f docker/docker-compose.yml exec postgres \
--     psql -U telematics_admin -d telematics_legacy -f /docker-entrypoint-initdb.d/01_schema.sql

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS fleet;
COMMENT ON SCHEMA fleet IS 'Legacy fleet management system: equipment master data and hour-meter logs';

-- ---------------------------------------------------------------------------
-- Equipment master data. equipment_id and equipment_serial_number match the telemetry fleet
-- (01_telemetry_simulator/equipment_config.py), so Silver can join CDC and telemetry.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet.equipment (
    equipment_id            integer      PRIMARY KEY,
    equipment_serial_number varchar(20)  NOT NULL UNIQUE,
    type_name               varchar(40)  NOT NULL,
    category                varchar(10)  NOT NULL CHECK (category IN ('BEV', 'Diesel')),
    manufacturer            varchar(40)  NOT NULL,
    model                   varchar(60)  NOT NULL,
    commissioned_on         date         NOT NULL,
    assigned_site           varchar(40)  NOT NULL,
    status                  varchar(20)  NOT NULL DEFAULT 'ACTIVE'
                            CHECK (status IN ('ACTIVE', 'STANDBY', 'MAINTENANCE', 'DECOMMISSIONED')),
    service_interval_hours  integer      NOT NULL CHECK (service_interval_hours > 0),
    created_at              timestamptz  NOT NULL DEFAULT now(),
    updated_at              timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE fleet.equipment IS 'One row per machine. Machines are decommissioned (status), never deleted';

-- ---------------------------------------------------------------------------
-- Hour-meter readings logged by operators, shift reports and service visits. Clerical errors are
-- corrected in place (UPDATE) or voided (DELETE), which is why this table needs CDC rather than
-- an append-only extract.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet.hour_meter_logs (
    log_id        bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    equipment_id  integer       NOT NULL REFERENCES fleet.equipment (equipment_id),
    reading_at    timestamptz   NOT NULL,
    hour_meter    numeric(9, 1) NOT NULL CHECK (hour_meter >= 0),
    source        varchar(20)   NOT NULL CHECK (source IN ('SHIFT_REPORT', 'OPERATOR_LOG', 'SERVICE_VISIT')),
    recorded_by   varchar(40)   NOT NULL,
    created_at    timestamptz   NOT NULL DEFAULT now(),
    updated_at    timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (equipment_id, reading_at)
);
COMMENT ON TABLE fleet.hour_meter_logs IS 'Cumulative engine/motor hours per machine over time';

CREATE INDEX IF NOT EXISTS hour_meter_logs_equipment_reading_idx
    ON fleet.hour_meter_logs (equipment_id, reading_at DESC);

-- Keep updated_at honest for every write path, including manual fixes in psql.
CREATE OR REPLACE FUNCTION fleet.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER equipment_touch_updated_at
    BEFORE UPDATE ON fleet.equipment
    FOR EACH ROW EXECUTE FUNCTION fleet.touch_updated_at();

CREATE OR REPLACE TRIGGER hour_meter_logs_touch_updated_at
    BEFORE UPDATE ON fleet.hour_meter_logs
    FOR EACH ROW EXECUTE FUNCTION fleet.touch_updated_at();

-- ---------------------------------------------------------------------------
-- Logical replication
-- ---------------------------------------------------------------------------
-- FULL: UPDATE and DELETE events carry the complete previous row ("before"), not just the key.
-- Silver uses it to build history and to know what a deleted row contained.
ALTER TABLE fleet.equipment       REPLICA IDENTITY FULL;
ALTER TABLE fleet.hour_meter_logs REPLICA IDENTITY FULL;

-- Created here rather than by Debezium (publication.autocreate.mode = disabled), so the CDC user
-- needs no table ownership. Only the tables listed here are streamed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'telematics_cdc_pub') THEN
        CREATE PUBLICATION telematics_cdc_pub FOR TABLE fleet.equipment, fleet.hour_meter_logs;
    END IF;
END;
$$;
