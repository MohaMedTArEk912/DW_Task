#!/usr/bin/env bash
# =============================================================================
# scripts/live-input.sh
# Purpose: Insert a new user record into the source MySQL database to trigger CDC.
# Usage:   bash scripts/live-input.sh [NAME] [EMAIL]
# =============================================================================

set -e

# Default values if no arguments provided
NAME=${1:-"Live User $((RANDOM % 1000))"}
EMAIL=${2:-"live.user.$((RANDOM % 10000))@example.com"}

echo "📥 [1/2] Inserting data into MySQL (testdb.users)..."
echo "    - Name:  $NAME"
echo "    - Email: $EMAIL"

# Use the Docker exec command
docker exec -i cdc_mysql mysql -u cdc_user -pcdc_password testdb -e \
    "INSERT INTO users (name, email) VALUES ('$NAME', '$EMAIL');"

echo "✅ [2/2] Insert complete."
echo "⌛ Waiting 5 seconds for CDC pipeline to process..."
sleep 5

echo "📊 Current state of Data Warehouse (dw_users):"
docker exec cdc_warehouse psql -U dw_user -d datawarehouse -c \
    "SELECT id, name, email, cdc_operation, TO_TIMESTAMP(cdc_timestamp/1000) AS event_time FROM dw_users ORDER BY dw_id DESC LIMIT 5;"

echo "------------------------------------------------------------"
echo "💡 You can run this script again with: bash scripts/live-input.sh \"My Name\" \"my@email.com\""
echo "------------------------------------------------------------"
