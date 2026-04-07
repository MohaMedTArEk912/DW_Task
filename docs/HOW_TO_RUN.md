# 🚀 Running the CDC Pipeline (Automated)

This guide documents how to run, manage, and test the MySQL → Debezium → Kafka → NiFi → Data Warehouse pipeline using the provided automation scripts.

---

## 🏗️ 1 - Fresh Start (Highly Recommended)

If you want to blow away all data and rebuild the entire environment from scratch **fully automated**:

```bash
# This cleans everything (even volumes), restarts Docker, 
# registers Debezium, and builds the NiFi flow automatically. 
bash scripts/rebuild-all.sh
```

> **Note:** NiFi takes about 60–90 seconds to fully start its internal API before the automation can build the flow. The script will wait for you.

---

## 🛠️ 2 - Manual Controls

If you've already started the containers and just want to re-run specific parts:

### Register / Reset Debezium
To (re-)register the MySQL CDC connector with Debezium:
```bash
bash debezium/register-connector.sh
```

### Build / Reset NiFi Flow
To (re-)deploy the NiFi flow (this expects NiFi to be running at `http://localhost:8081/nifi`):
```bash
python build_nifi_flow.py
```

### Clear NiFi Canvas
To completely wipe the NiFi canvas (deletes all processors and connections):
```bash
python clear_nifi.py
```

---

## 🧪 3 - Testing the Pipeline

### Manual Test Queries
Run a pre-defined set of INSERT / UPDATE / DELETE queries on the source MySQL:
```bash
docker exec -i cdc_mysql mysql -u cdc_user -pcdc_password testdb < mysql/test-queries.sql
```

### Live User Input
To simulate a "live" interactive input:
```bash
# Adds a random user
bash scripts/live-input.sh

# Or add a specific user
bash scripts/live-input.sh "Jane Doe" "jane@example.com"
```

### To verify the Data Warehouse content:

```bash
docker exec cdc_warehouse psql -U dw_user -d datawarehouse -c "SELECT id, name, email, cdc_operation, TO_TIMESTAMP(cdc_timestamp/1000) AS event_time FROM dw_users ORDER BY dw_id DESC LIMIT 10;"
```
---

## 🔍 4 - Verification Tools

| Purpose | Command |
|---|---|
| **Data Warehouse** | `docker exec cdc_warehouse psql -U dw_user -d datawarehouse -c "SELECT * FROM dw_users DESC LIMIT 10;"` |
| **Kafka Messages** | `bash kafka/consume-messages.sh` |
| **Connector Status**| `curl http://localhost:8083/connectors/mysql-cdc-connector/status` |
| **NiFi Logs** | `docker logs cdc_nifi --tail=100 -f` |

---

## 🧹 5 - Stopping & Cleaning

| Action | Command |
|---|---|
| **Stop but keep data** | `docker compose stop` |
| **Stop and remove containers**| `docker compose down` |
| **Stop and wipe all data volumes** (Clean Slate) | `docker compose down -v` |

---

## 🌍 Accessing Web UIs

- **Apache NiFi:** [http://localhost:8081/nifi](http://localhost:8081/nifi) (Wait for it! Takes ~1 min to boot)
- **Kafka UI:** [http://localhost:8090](http://localhost:8090)
- **Debezium API:** [http://localhost:8083/connectors](http://localhost:8083/connectors)

### For MySQL:
    
```bash
docker exec -it cdc_mysql mysql -u cdc_user -pcdc_password testdb
```

```bash
docker exec cdc_mysql mysql -u cdc_user -pcdc_password testdb -e "SELECT * FROM users;"
```

```bash
INSERT INTO users (name, email) VALUES ('Jane Doe', 'jane@example.com');
```

### For PostgreSQL: 

```bash
docker exec cdc_warehouse psql -U dw_user -d datawarehouse -c "SELECT * FROM dw_users;"
```
