#!/usr/bin/env bash
# =============================================================================
# scripts/rebuild-all.sh
# Purpose: Deep clean and rebuild the entire CDC pipeline (Automated)
# Usage:   bash scripts/rebuild-all.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${PROJECT_ROOT}"

echo "🔥 [1/7] Deep cleaning: stopping all services and removing volumes..."
docker compose down -v --remove-orphans

echo "🚀 [2/7] Starting infrastructure services (Zookeeper, Kafka, MySQL, Postgres)..."
docker compose up -d zookeeper kafka mysql warehouse kafka-ui

# Wait for healthy
echo "⌛ Waiting for MySQL and Kafka to be healthy..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' cdc_mysql 2>/dev/null)" = "healthy" ]; do printf "."; sleep 5; done
until [ "$(docker inspect --format='{{.State.Health.Status}}' cdc_kafka 2>/dev/null)" = "healthy" ]; do printf "."; sleep 5; done
echo " Infrastructure is ready!"

echo "🔧 [3/7] Starting Kafka Connect (Debezium)..."
docker compose up -d kafka-connect
echo "⌛ Waiting for Kafka Connect API..."
until curl -sf "http://localhost:8083/connectors" > /dev/null 2>&1; do printf "."; sleep 5; done
echo " Kafka Connect is ready!"

echo "🤖 [4/7] Registering Debezium MySQL Connector..."
bash debezium/register-connector.sh

echo "🌊 [5/7] Starting Apache NiFi..."
docker compose up -d nifi
echo "⌛ Waiting for NiFi container to start..."
until [ "$(docker inspect -f '{{.State.Running}}' cdc_nifi 2>/dev/null)" = "true" ]; do printf "."; sleep 3; done

echo "📦 [6/7] Installing PostgreSQL JDBC driver into NiFi..."
JDBC_JAR="postgresql-42.7.3.jar"
if [ ! -f "${JDBC_JAR}" ]; then
    curl -fsSL -o "${JDBC_JAR}" "https://jdbc.postgresql.org/download/${JDBC_JAR}"
fi
docker cp "${JDBC_JAR}" cdc_nifi:/opt/nifi/nifi-current/lib/
# Need to restart NiFi to pick up the JAR if it wasn't there
docker restart cdc_nifi

echo "⌛ Waiting for NiFi API to become available (this takes a minute)..."
until curl -sf "http://localhost:8081/nifi-api/process-groups/root" > /dev/null 2>&1; do printf "."; sleep 10; done
echo " NiFi API is online!"

echo "⚙️ [7/7] Automating NiFi Flow Deployment..."
if command -v python.exe &> /dev/null; then
    python.exe build_nifi_flow.py
else
    python build_nifi_flow.py
fi

echo ""
echo "✅ SUCCESS: The entire pipeline is rebuilt and running!"
echo "------------------------------------------------------------"
echo " NiFi URL:       http://localhost:8081/nifi"
echo " Kafka UI:       http://localhost:8090"
echo " DW Table:       docker exec cdc_warehouse psql -U dw_user -d datawarehouse -c 'SELECT * FROM dw_users;'"
echo "------------------------------------------------------------"
