#!/usr/bin/env bash
# =============================================================================
# scripts/simple-test.sh
# Purpose: Simple end-to-end CDC pipeline test WITHOUT requiring NiFi
#   1. Run INSERT / UPDATE / DELETE in MySQL
#   2. Confirm Kafka received the CDC events
#   3. Directly insert a sample CDC event into PostgreSQL DW (simulates NiFi)
#   4. Verify the DW has correct data and views work
# Usage:   bash scripts/simple-test.sh
# =============================================================================

set -euo pipefail

PASS=0
FAIL=0

pass() { echo "  ✅ PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "============================================================"
echo " Simple CDC Pipeline Test (No NiFi Required)"
echo "============================================================"

# ── Test 1: MySQL connectivity ──────────────────────────────────
echo ""
echo "[Test 1] MySQL connectivity..."
RESULT=$(docker exec cdc_mysql mysql -u cdc_user -pcdc_password testdb \
    -se "SELECT COUNT(*) FROM users;" 2>/dev/null)
if [[ "$RESULT" =~ ^[0-9]+$ ]] && [ "$RESULT" -ge 3 ]; then
    pass "MySQL reachable, users table has $RESULT rows (≥3 seed rows)"
else
    fail "MySQL unreachable or users table missing (got: '$RESULT')"
fi

# ── Test 2: Insert, Update, Delete in MySQL ─────────────────────
echo ""
echo "[Test 2] MySQL INSERT / UPDATE / DELETE..."
docker exec cdc_mysql mysql -u cdc_user -pcdc_password testdb 2>/dev/null -e "
    INSERT INTO users (name, email) VALUES ('Simple Test User', 'simpletest@test.com')
        ON DUPLICATE KEY UPDATE name='Simple Test User';
    UPDATE users SET name='Simple Test User (v2)' WHERE email='simpletest@test.com';
" >/dev/null
UPDATED=$(docker exec cdc_mysql mysql -u cdc_user -pcdc_password testdb \
    -se "SELECT name FROM users WHERE email='simpletest@test.com';" 2>/dev/null)
if [ "$UPDATED" = "Simple Test User (v2)" ]; then
    pass "INSERT + UPDATE verified in MySQL"
else
    fail "Expected 'Simple Test User (v2)', got: '$UPDATED'"
fi

# ── Test 3: Kafka topic exists ──────────────────────────────────
echo ""
echo "[Test 3] Kafka topic cdc.testdb.users exists..."
TOPICS=$(docker exec cdc_kafka kafka-topics \
    --bootstrap-server kafka:9092 --list 2>/dev/null)
if echo "$TOPICS" | grep -q "cdc.testdb.users"; then
    pass "Topic cdc.testdb.users exists"
else
    fail "Topic cdc.testdb.users NOT found"
fi

# ── Test 4: Kafka has CDC messages ─────────────────────────────
echo ""
echo "[Test 4] Kafka cdc.testdb.users has messages..."
sleep 3  # allow Debezium to write the UPDATE event
MSG_COUNT=$(docker exec cdc_kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic cdc.testdb.users \
    --from-beginning \
    --timeout-ms 5000 \
    2>/dev/null | wc -l || true)
if [ "$MSG_COUNT" -ge 4 ]; then
    pass "Kafka has $MSG_COUNT messages (≥4 expected)"
else
    fail "Kafka has only $MSG_COUNT messages (expected ≥4)"
fi

# ── Test 5: Sample CDC message is valid JSON ─────────────────────
echo ""
echo "[Test 5] Kafka messages are valid JSON..."
SAMPLE=$(docker exec cdc_kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic cdc.testdb.users \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 \
    2>/dev/null | head -1 || true)
if echo "$SAMPLE" | python3 -m json.tool >/dev/null 2>&1; then
    pass "Messages are valid JSON"
    echo "     Sample: $(echo "$SAMPLE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'op={d.get(\"__op\",\"?\")} id={d.get(\"id\",\"?\")} name={d.get(\"name\",\"?\")}')" 2>/dev/null || echo "$SAMPLE")"
else
    fail "Message is not valid JSON: $SAMPLE"
fi

# ── Test 6: PostgreSQL DW connectivity ─────────────────────────
echo ""
echo "[Test 6] PostgreSQL DW connectivity..."
DW_STATUS=$(docker exec -i cdc_warehouse psql \
    -U dw_user -d datawarehouse \
    -tc "SELECT 'ok';" 2>/dev/null | tr -d ' \n')
if [ "$DW_STATUS" = "ok" ]; then
    pass "PostgreSQL DW reachable"
else
    fail "PostgreSQL DW unreachable"
fi

# ── Test 7: DW schema (dw_users table + views) ─────────────────
echo ""
echo "[Test 7] DW schema — dw_users table and views exist..."
TABLE_CHECK=$(docker exec -i cdc_warehouse psql \
    -U dw_user -d datawarehouse \
    -tc "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('dw_users') AND table_schema='public';" \
    2>/dev/null | tr -d ' \n')
VIEW_CHECK=$(docker exec -i cdc_warehouse psql \
    -U dw_user -d datawarehouse \
    -tc "SELECT COUNT(*) FROM information_schema.views WHERE table_name IN ('dw_users_latest','dw_users_active') AND table_schema='public';" \
    2>/dev/null | tr -d ' \n')
if [ "$TABLE_CHECK" = "1" ] && [ "$VIEW_CHECK" = "2" ]; then
    pass "dw_users table + dw_users_latest + dw_users_active views all exist"
else
    fail "Missing DW objects (tables=$TABLE_CHECK expected=1, views=$VIEW_CHECK expected=2)"
fi

# ── Test 8: Write a CDC event directly into DW, verify views ───
echo ""
echo "[Test 8] Direct DW write + view verification (simulates NiFi)..."
docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse -q 2>/dev/null << 'EOSQL'
  -- clean up any previous simple-test rows
  DELETE FROM dw_users WHERE email = 'simpletest@test.com';

  -- simulate what NiFi would insert for a 'create' event
  INSERT INTO dw_users (id, name, email, created_at, updated_at, cdc_operation, cdc_timestamp)
  VALUES (999, 'Simple Test User', 'simpletest@test.com',
          NOW(), NOW(), 'c', EXTRACT(EPOCH FROM NOW()) * 1000);

  -- simulate NiFi inserting an 'update' event
  INSERT INTO dw_users (id, name, email, created_at, updated_at, cdc_operation, cdc_timestamp)
  VALUES (999, 'Simple Test User (v2)', 'simpletest@test.com',
          NOW(), NOW(), 'u', EXTRACT(EPOCH FROM NOW()) * 1000 + 1000);
EOSQL

ACTIVE_NAME=$(docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse \
    -tc "SELECT name FROM dw_users_active WHERE id=999;" 2>/dev/null | tr -d ' \n')
LATEST_OP=$(docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse \
    -tc "SELECT cdc_operation FROM dw_users_latest WHERE id=999;" 2>/dev/null | tr -d ' \n')
if [ "$ACTIVE_NAME" = "SimpleTestUser(v2)" ] || [ "$ACTIVE_NAME" = "Simple Test User (v2)" ]; then
    pass "dw_users_active shows latest name: '$ACTIVE_NAME'"
else
    fail "Expected 'Simple Test User (v2)' in dw_users_active, got: '$ACTIVE_NAME'"
fi
if [ "$LATEST_OP" = "u" ]; then
    pass "dw_users_latest shows cdc_operation='u' (update)"
else
    fail "Expected cdc_operation='u', got: '$LATEST_OP'"
fi

# ── Test 9: Debezium connector status ──────────────────────────
echo ""
echo "[Test 9] Debezium connector is RUNNING..."
CONNECTOR_STATE=$(curl -s http://localhost:8083/connectors/mysql-cdc-connector/status \
    2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
TASK_STATE=$(curl -s http://localhost:8083/connectors/mysql-cdc-connector/status \
    2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tasks'][0]['state'])" 2>/dev/null || echo "UNKNOWN")
if [ "$CONNECTOR_STATE" = "RUNNING" ] && [ "$TASK_STATE" = "RUNNING" ]; then
    pass "Connector=RUNNING, Task=RUNNING"
else
    fail "Connector=$CONNECTOR_STATE, Task=$TASK_STATE (both should be RUNNING)"
fi

# ── Test 10: Delete propagation ─────────────────────────────────
echo ""
echo "[Test 10] Delete propagation in DW..."
docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse -q 2>/dev/null << 'EOSQL'
  INSERT INTO dw_users (id, name, email, created_at, updated_at, cdc_operation, cdc_timestamp)
  VALUES (999, 'Simple Test User (v2)', 'simpletest@test.com',
          NOW(), NOW(), 'd', EXTRACT(EPOCH FROM NOW()) * 1000 + 2000);
EOSQL
ACTIVE_COUNT=$(docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse \
    -tc "SELECT COUNT(*) FROM dw_users_active WHERE id=999;" 2>/dev/null | tr -d ' \n')
if [ "$ACTIVE_COUNT" = "0" ]; then
    pass "Deleted user excluded from dw_users_active view"
else
    fail "Expected 0 active rows for deleted user, got: $ACTIVE_COUNT"
fi

# ── Cleanup test data ───────────────────────────────────────────
docker exec -i cdc_warehouse psql -U dw_user -d datawarehouse -q \
    -c "DELETE FROM dw_users WHERE email='simpletest@test.com';" 2>/dev/null
docker exec cdc_mysql mysql -u cdc_user -pcdc_password testdb 2>/dev/null \
    -e "DELETE FROM users WHERE email='simpletest@test.com';" >/dev/null

# ── Summary ─────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " Results: $PASS passed, $FAIL failed"
echo "============================================================"

if [ "$FAIL" -gt 0 ]; then
    echo " ❌ Some tests FAILED — see above for details."
    exit 1
else
    echo " ✅ All tests PASSED!"
    exit 0
fi
