#!/bin/bash
# Deploys Kafka (KRaft), the legacy Postgres database, Kafka Connect with the Snowflake sink and
# the Debezium source, and Kafka UI to Minikube, then registers the connectors. Idempotent: safe
# to run any number of times.
#
# Usage (from the repo root, in Git Bash on Windows or any POSIX shell):
#   minikube start --driver=docker --cpus=4 --memory=6g   # first time only
#   bash kubernetes/deploy.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE=telematics
CONNECT_IMAGE=telematics/kafka-connect:sf4.2.0-dbz3.7.0
ENV_FILE="$REPO_ROOT/.env"
SECRET_ENV="$REPO_ROOT/secrets/.k8s-secret.env"

if [[ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]]; then PYTHON="$REPO_ROOT/.venv/Scripts/python.exe"
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then PYTHON="$REPO_ROOT/.venv/bin/python"
else PYTHON=python3; fi

step() { echo; echo "==> $*"; }

# secret_from_env NAME REGEX: creates or updates Secret NAME from the .env lines matching REGEX.
# The filtered copy lives in the gitignored secrets/ folder (not a /dev/fd pipe, which kubectl.exe
# on Windows cannot read) and is removed straight after, or on exit if anything fails.
secret_from_env() {
  grep -E "$2" "$ENV_FILE" | tr -d '\r' > "$SECRET_ENV"
  kubectl -n "$NAMESPACE" create secret generic "$1" \
    --from-env-file="$SECRET_ENV" --dry-run=client -o yaml | kubectl apply -f -
  rm -f "$SECRET_ENV"
}

step "Checking prerequisites"
minikube status --format '{{.Host}} {{.APIServer}}' | grep -q "Running Running" \
  || { echo "Minikube is not running. Start it with: minikube start --driver=docker --cpus=4 --memory=6g"; exit 1; }
[[ -f "$ENV_FILE" ]] || { echo "Missing .env. Copy .env.example and run generate_keypair.py first."; exit 1; }
grep -q '^SNOWFLAKE_PRIVATE_KEY=.\+' "$ENV_FILE" \
  || { echo "SNOWFLAKE_PRIVATE_KEY is empty. Run: python 02_streaming_ingestion/snowflake/generate_keypair.py"; exit 1; }
grep -q '^POSTGRES_CDC_PASSWORD=.\+' "$ENV_FILE" \
  || { echo "Postgres passwords are not set. Run: python 03_cdc_migration/postgres/configure_env.py"; exit 1; }
kubectl config use-context minikube > /dev/null

step "Building $CONNECT_IMAGE inside Minikube (cached after the first build)"
minikube image build -t "$CONNECT_IMAGE" "$REPO_ROOT/02_streaming_ingestion/connect"

step "Applying namespace, script ConfigMaps and Secrets"
kubectl apply -f "$REPO_ROOT/kubernetes/namespace.yaml"
kubectl -n "$NAMESPACE" create configmap kafka-topics-script \
  --from-file=create_topics.sh="$REPO_ROOT/kafka/create_topics.sh" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create configmap postgres-init-scripts \
  --from-file="$REPO_ROOT/03_cdc_migration/postgres/init" \
  --dry-run=client -o yaml | kubectl apply -f -
trap 'rm -f "$SECRET_ENV"' EXIT
mkdir -p "$REPO_ROOT/secrets"
# Each pod gets only the keys it needs.
secret_from_env snowflake-credentials '^SNOWFLAKE_[A-Z_]+='
secret_from_env postgres-credentials '^POSTGRES_(DB|USER|PASSWORD|CDC_USER|CDC_PASSWORD|APP_USER|APP_PASSWORD)='
secret_from_env postgres-cdc-credentials '^POSTGRES_(DB|CDC_USER|CDC_PASSWORD)='

step "Applying manifests"
# Jobs are immutable; delete the finished one so the topic script runs again.
kubectl -n "$NAMESPACE" delete job kafka-init --ignore-not-found
kubectl apply -k "$REPO_ROOT/kubernetes"
# Pick up a changed Secret or a rebuilt image (same tag) on re-deploys.
kubectl -n "$NAMESPACE" rollout restart deployment/kafka-connect

step "Waiting for Kafka, topics, Postgres, Kafka Connect and Kafka UI"
kubectl -n "$NAMESPACE" rollout status statefulset/kafka --timeout=300s
kubectl -n "$NAMESPACE" wait --for=condition=complete job/kafka-init --timeout=300s
kubectl -n "$NAMESPACE" rollout status statefulset/postgres --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/kafka-connect --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/kafka-ui --timeout=300s

step "Registering connectors through a temporary port-forward"
kubectl -n "$NAMESPACE" port-forward svc/kafka-connect 18083:8083 > /dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; rm -f "$SECRET_ENV"' EXIT
"$PYTHON" "$REPO_ROOT/02_streaming_ingestion/register_connectors.py" --connect-url http://localhost:18083

cat <<EOF

Deployed. To use the cluster from this machine (run each in its own terminal):
  kubectl -n $NAMESPACE port-forward svc/kafka 9092:9092            # simulator -> Kafka
  kubectl -n $NAMESPACE port-forward svc/postgres 5432:5432         # legacy_activity.py -> Postgres
  kubectl -n $NAMESPACE port-forward svc/kafka-connect 8083:8083    # Connect REST API
  kubectl -n $NAMESPACE port-forward svc/kafka-ui 8080:8080         # http://localhost:8080
Stop the Docker Compose stack first; both use ports 9092, 5432, 8083 and 8080.
EOF
