"""Module 4 unit tests: the dbt project, its signal catalog, the Snowflake setup, and the dbt image,
Compose service and CronJob stay consistent with each other. No Snowflake needed; the dbt data and
unit tests themselves run with `python 04_data_warehouse/run_dbt.py build`."""

import csv
import io
import re

import pytest
import yaml

from conftest import REPO_ROOT
from equipment_config import EQUIPMENT_FLEET, SIGNAL_REGISTRY
from generate_signal_catalog import CATALOG_PATH, catalog_rows, render

DBT_DIR = REPO_ROOT / "04_data_warehouse"
MODELS_DIR = DBT_DIR / "models"
SETUP_SQL = (DBT_DIR / "snowflake" / "setup.sql").read_text(encoding="utf-8")
PROFILES = (DBT_DIR / "profiles.yml").read_text(encoding="utf-8")
REQUIREMENTS = (DBT_DIR / "requirements.txt").read_text(encoding="utf-8")
DOCKERFILE = (DBT_DIR / "Dockerfile").read_text(encoding="utf-8")
COMPOSE = yaml.safe_load((REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8"))
CRONJOB = yaml.safe_load((REPO_ROOT / "kubernetes" / "dbt-cronjob.yaml").read_text(encoding="utf-8"))
KUSTOMIZATION = yaml.safe_load((REPO_ROOT / "kubernetes" / "kustomization.yaml").read_text(encoding="utf-8"))
DEPLOY_SH = (REPO_ROOT / "kubernetes" / "deploy.sh").read_text(encoding="utf-8")
ENV_EXAMPLE = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
CATALOG = list(csv.DictReader(io.StringIO(CATALOG_PATH.read_text(encoding="utf-8"))))


def model_properties() -> dict[str, dict]:
    """Every model entry in the project's YAML files, by name, with the layer folder it lives in."""
    models = {}
    for path in MODELS_DIR.rglob("*.yml"):
        for model in yaml.safe_load(path.read_text(encoding="utf-8")).get("models", []):
            models[model["name"]] = {**model, "layer": path.parent.name}
    return models


def deploy_secret_pattern(secret: str) -> re.Pattern:
    match = re.search(rf"^secret_from_env {secret} '([^']+)'", DEPLOY_SH, re.MULTILINE)
    assert match, f"deploy.sh does not create Secret {secret}"
    return re.compile(match.group(1))


def cron_container() -> dict:
    return CRONJOB["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]


# --- Signal catalog seed ---


def test_committed_signal_catalog_is_up_to_date():
    committed = CATALOG_PATH.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == render(catalog_rows()), (
        "signal_catalog.csv is stale: run python 04_data_warehouse/generate_signal_catalog.py"
    )


def test_catalog_lists_every_signal_of_every_fleet_type():
    for type_name in {machine["type_name"] for machine in EQUIPMENT_FLEET}:
        expected = {*SIGNAL_REGISTRY["UNIVERSAL_CORE"], *SIGNAL_REGISTRY[type_name]}
        assert {row["tag"] for row in CATALOG if row["type_name"] == type_name} == expected, type_name


def test_catalog_ranges_are_well_formed():
    for row in CATALOG:
        if row["unit"] == "CODE":
            assert row["allowed_values"] and not row["min_expected"], row["tag"]
        elif row["is_cumulative"] == "true":
            assert row["min_expected"] == "0" and not row["max_expected"], row["tag"]
        else:
            assert float(row["min_expected"]) <= float(row["max_expected"]), row["tag"]


def test_pressure_drop_anomalies_are_always_out_of_range():
    # PRESSURE_DROP sets a PSI reading to between -50 and 0, so no PSI signal may expect negatives.
    assert all(float(row["min_expected"]) >= 0 for row in CATALOG if row["unit"] == "PSI")


def test_temperature_spikes_are_out_of_range_except_on_exhaust_gas():
    # TEMPERATURE_SPIKE sets a DEGC reading to 250-999. Exhaust gas legitimately reaches 750 in
    # FAULT, so a spike there is only caught above that; every other temperature is always caught.
    for row in CATALOG:
        if row["unit"] == "DEGC" and row["signal_name"] != "Exhaust_Gas_Temp":
            assert float(row["max_expected"]) < 250, row["tag"]


# --- dbt project ---


def test_every_model_is_documented():
    documented = model_properties()
    for sql in MODELS_DIR.rglob("*.sql"):
        assert sql.stem in documented, f"{sql.relative_to(DBT_DIR)} has no YAML entry"
        assert documented[sql.stem].get("description"), f"{sql.stem} has no description"


def test_gold_models_enforce_a_typed_contract():
    # Contracts are enforced project-wide for gold/ in dbt_project.yml; each column needs a type.
    project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
    assert project["models"]["telematics"]["gold"]["+contract"] == {"enforced": True}
    gold = [model for model in model_properties().values() if model["layer"] == "gold"]
    assert gold
    for model in gold:
        untyped = [col["name"] for col in model["columns"] if not col.get("data_type")]
        assert not untyped, f"{model['name']}: columns without data_type: {untyped}"


@pytest.mark.parametrize("model", ["telemetry_events", "telemetry_readings"])
def test_telemetry_silver_is_incremental_first_delivery_wins(model):
    sql = (MODELS_DIR / "silver" / f"{model}.sql").read_text(encoding="utf-8")
    assert "materialized='incremental'" in sql
    assert "incremental_watermark('ingested_at')" in sql
    # Updating only the key makes a match a no-op, so a later delivery never overwrites the first.
    key = re.search(r"unique_key='(\w+)'", sql).group(1)
    assert f"merge_update_columns=['{key}']" in sql


# --- Snowflake setup and credentials ---


def test_transform_role_reads_bronze_and_writes_only_silver_and_gold():
    grants = [
        (privs.strip(), target.strip())
        for privs, target in re.findall(r"^GRANT (.+?) ON (.+?) TO ROLE TELEMATICS_TRANSFORM_ROLE;", SETUP_SQL, re.MULTILINE)
    ]
    bronze = [(privs, target) for privs, target in grants if "BRONZE" in target]
    assert bronze and all(privs in ("USAGE", "SELECT") for privs, _ in bronze)
    writable = {target.split(".")[-1] for privs, target in grants if "CREATE" in privs}
    assert writable == {"SILVER", "GOLD"}
    assert not re.search(r"OWNERSHIP|INSERT|UPDATE|DELETE|TRUNCATE", "\n".join(p for p, _ in grants))


def test_setup_sql_is_a_template_without_a_key():
    assert "SET RSA_PUBLIC_KEY = '<RSA_PUBLIC_KEY>'" in SETUP_SQL
    assert "BEGIN" not in SETUP_SQL


def test_profile_reads_every_setting_from_the_environment():
    output = yaml.safe_load(PROFILES)["telematics"]["outputs"]["prod"]
    for key in ("account", "user", "role", "private_key", "warehouse", "database"):
        assert "env_var(" in output[key], key
    for var in re.findall(r"env_var\('(\w+)'", PROFILES):
        assert re.search(rf"^{var}=", ENV_EXAMPLE, re.MULTILINE), f"{var} is not documented in .env.example"


def test_dbt_key_renders_into_setup_sql():
    import generate_dbt_keypair

    rendered = generate_dbt_keypair.render_setup_sql("MIIBIjANBgkq")
    assert "SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq'" in rendered
    assert rendered.startswith("-- GENERATED")
    assert generate_dbt_keypair.PRIVATE_KEY_PATH.parent.name == "secrets"


# --- Image, Compose and Kubernetes ---


def test_dbt_image_tag_matches_pinned_versions_everywhere():
    core = re.search(r"^dbt-core==([\d.]+)$", REQUIREMENTS, re.MULTILINE).group(1)
    adapter = re.search(r"^dbt-snowflake==([\d.]+)$", REQUIREMENTS, re.MULTILINE).group(1)
    tag = f"telematics/dbt:core{core}-sf{adapter}"
    assert COMPOSE["services"]["dbt"]["image"] == tag
    assert cron_container()["image"] == tag
    assert f"DBT_IMAGE={tag}" in DEPLOY_SH


def test_compose_dbt_service_only_runs_on_demand():
    service = COMPOSE["services"]["dbt"]
    assert service["profiles"] == ["dbt"]
    assert service["build"] == "../04_data_warehouse"


def test_cronjob_builds_every_5_minutes_without_overlap():
    spec = CRONJOB["spec"]
    assert spec["schedule"] == "*/5 * * * *"
    assert spec["concurrencyPolicy"] == "Forbid"
    assert "dbt-cronjob.yaml" in KUSTOMIZATION["resources"]


def test_scheduled_build_skips_unit_tests():
    args = ["build", "--exclude-resource-type", "unit_test"]
    assert cron_container()["args"] == args
    assert f'CMD {str(args).replace(chr(39), chr(34))}' in DOCKERFILE


def test_dbt_and_connector_never_share_credentials():
    assert [ref["secretRef"]["name"] for ref in cron_container()["envFrom"]] == ["dbt-snowflake-credentials"]
    dbt_secret = deploy_secret_pattern("dbt-snowflake-credentials")
    connector_secret = deploy_secret_pattern("snowflake-credentials")
    assert dbt_secret.match("DBT_SNOWFLAKE_PRIVATE_KEY=x") and dbt_secret.match("SNOWFLAKE_ACCOUNT=x")
    assert not dbt_secret.match("SNOWFLAKE_PRIVATE_KEY=x")
    assert not connector_secret.match("DBT_SNOWFLAKE_PRIVATE_KEY=x")
