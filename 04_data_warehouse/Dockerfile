# dbt Core with the Snowflake adapter and the telematics project (Module 4). Used by the "dbt"
# service in docker/docker-compose.yml and the dbt-build CronJob in kubernetes/.
#
# No credentials are baked in: profiles.yml reads everything from environment variables, which come
# from .env (Compose) or the dbt-snowflake-credentials Secret (Kubernetes).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DO_NOT_TRACK=1 \
    DBT_PROFILES_DIR=/app \
    DBT_PROJECT_DIR=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# dbt writes dbt_packages/, target/ and logs/ next to the project, so the non-root user owns /app.
RUN useradd --create-home --uid 10001 dbt && chown dbt:dbt /app
COPY --chown=dbt:dbt . .
USER 10001

# Packages are installed at build time, pinned by package-lock.yml, so runs need no network
# access beyond Snowflake.
RUN dbt deps

ENTRYPOINT ["dbt"]
# The scheduled run: models, seeds and data tests. Unit tests run in development, not every 5 minutes.
CMD ["build", "--exclude-resource-type", "unit_test"]
