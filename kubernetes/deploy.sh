#!/bin/bash
# Deploys Kafka (KRaft), Kafka Connect with the Snowflake sink, and Kafka UI to Minikube, then
# registers the connectors. Idempotent: safe to run any number of times.
#
# Usage (from the repo root, in Git Bash on Windows or any POSIX shell):
#   minikube start --driver=docker --cpus=4 --memory=6g   # first time only
#   bash kubernetes/deploy.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE=telematics
CONNECT_IMAGE=telematics/kafka-connect-snowflake:4.2.0
ENV_FILE="$REPO_ROOT/.env"

if [[ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]]; then PYTHON="$REPO_ROOT/.venv/Scripts/python.exe"
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then PYTHON="$REPO_ROOT/.venv/bin/python"
else PYTHON=python3; fi

step() { echo; echo "==> $*"; }

step "Checking prerequisites"
minikube status --format '{{.Host}} {{.APIServer}}' | grep -q "Running Running" \
  || { echo "Minikube is not running. Start it with: minikube start --driver=docker --cpus=4 --memory=6g"; exit 1; }
[[ -f "$ENV_FILE" ]] || { echo "Missing .env. Copy .env.example and run generate_keypair.py first."; exit 1; }
grep -q '^SNOWFLAKE_PRIVATE_KEY=.\+' "$ENV_FILE" \
  || { echo "SNOWFLAKE_PRIVATE_KEY is empty. Run: python 02_streaming_ingestion/snowflake/generate_keypair.py"; exit 1; }
kubectl config use-context minikube > /dev/null

step "Building $CONNECT_IMAGE inside Minikube (cached after the first build)"
minikube image build -t "$CONNECT_IMAGE" "$REPO_ROOT/02_streaming_ingestion/connect"

step "Applying namespace, topic script ConfigMap and Snowflake Secret"
kubectl apply -f "$REPO_ROOT/kubernetes/namespace.yaml"
kubectl -n "$NAMESPACE" create configmap kafka-topics-script \
  --from-file=create_topics.sh="$REPO_ROOT/kafka/create_topics.sh" \
  --dry-run=client -o yaml | kubectl apply -f -
# Only SNOWFLAKE_* keys go into the Secret. The filtered copy lives in the gitignored secrets/
# folder (not a /dev/fd pipe, which kubectl.exe on Windows cannot read) and is removed on exit.
SECRET_ENV="$REPO_ROOT/secrets/.k8s-snowflake.env"
trap 'rm -f "$SECRET_ENV"' EXIT
mkdir -p "$REPO_ROOT/secrets"
grep -E '^SNOWFLAKE_[A-Z_]+=' "$ENV_FILE" | tr -d '\r' > "$SECRET_ENV"
kubectl -n "$NAMESPACE" create secret generic snowflake-credentials \
  --from-env-file="$SECRET_ENV" --dry-run=client -o yaml | kubectl apply -f -
rm -f "$SECRET_ENV"

step "Applying manifests"
# Jobs are immutable; delete the finished one so the topic script runs again.
kubectl -n "$NAMESPACE" delete job kafka-init --ignore-not-found
kubectl apply -k "$REPO_ROOT/kubernetes"
# Pick up a changed Secret or a rebuilt image (same tag) on re-deploys.
kubectl -n "$NAMESPACE" rollout restart deployment/kafka-connect

step "Waiting for Kafka, topics, Kafka Connect and Kafka UI"
kubectl -n "$NAMESPACE" rollout status statefulset/kafka --timeout=300s
kubectl -n "$NAMESPACE" wait --for=condition=complete job/kafka-init --timeout=300s
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
  kubectl -n $NAMESPACE port-forward svc/kafka-connect 8083:8083    # Connect REST API
  kubectl -n $NAMESPACE port-forward svc/kafka-ui 8080:8080         # http://localhost:8080
Stop the Docker Compose stack first; both use ports 9092, 8083 and 8080.
EOF
