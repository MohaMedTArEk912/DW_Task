import requests
import json
import time

BASE_URL = "http://localhost:8081/nifi-api"

def get(path):
    r = requests.get(f"{BASE_URL}/{path}")
    r.raise_for_status()
    return r.json()

def post(path, body):
    r = requests.post(f"{BASE_URL}/{path}", json=body)
    try:
        r.raise_for_status()
    except Exception as e:
        print("POST Error:", r.text)
        raise e
    return r.json()

def put(path, body):
    r = requests.put(f"{BASE_URL}/{path}", json=body)
    try:
        r.raise_for_status()
    except Exception as e:
        print("PUT Error:", r.text)
        raise e
    return r.json()

root_id = get("flow/process-groups/root")['processGroupFlow']['id']

X = 0
Y = 0
def create_processor(name, type_name):
    global Y
    types = get("flow/processor-types")['processorTypes']
    bundle, cls_name = None, None
    for t in types:
        if type_name in t['type']:
            bundle = t['bundle']
            cls_name = t['type']
            break
    if not bundle:
        raise Exception(f"Processor type '{type_name}' not found!")
    body = {
        "revision": {"version": 0},
        "disconnectedNodeAcknowledged": False,
        "component": {
            "type": cls_name,
            "bundle": bundle,
            "name": name,
            "position": {"x": X, "y": Y}
        }
    }
    Y += 200
    res = post(f"process-groups/{root_id}/processors", body)
    return res['component']['id'], res['revision']['version']

def update_processor(proc_id, version, properties, auto_terminate=[]):
    body = {
        "revision": {"version": version},
        "component": {
            "id": proc_id,
            "config": {
                "properties": properties,
                "autoTerminatedRelationships": auto_terminate
            }
        }
    }
    res = put(f"processors/{proc_id}", body)
    return res['revision']['version']

def create_controller_service(name, type_name):
    types = get("flow/controller-service-types")['controllerServiceTypes']
    bundle, cls_name = None, None
    for t in types:
        if type_name in t['type']:
            bundle = t['bundle']
            cls_name = t['type']
            break
    if not bundle:
        raise Exception(f"Service type '{type_name}' not found!")
    body = {
        "revision": {"version": 0},
        "component": {
            "type": cls_name,
            "bundle": bundle,
            "name": name
        }
    }
    res = post(f"process-groups/{root_id}/controller-services", body)
    return res['component']['id'], res['revision']['version']

def update_service(cs_id, version, properties):
    body = {
        "revision": {"version": version},
        "component": {
            "id": cs_id,
            "properties": properties
        }
    }
    res = put(f"controller-services/{cs_id}", body)
    return res['revision']['version']

def enable_service(cs_id, version):
    body = {
        "revision": {"version": version},
        "state": "ENABLED"
    }
    res = put(f"controller-services/{cs_id}/run-status", body)
    return res['revision']['version']

def create_connection(source_id, target_id, relationships):
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": source_id, "groupId": root_id, "type": "PROCESSOR"},
            "destination": {"id": target_id, "groupId": root_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships
        }
    }
    return post(f"process-groups/{root_id}/connections", body)

def start_processor(proc_id):
    proc = get(f"processors/{proc_id}")
    version = proc['revision']['version']
    body = {"revision": {"version": version}, "state": "RUNNING"}
    put(f"processors/{proc_id}/run-status", body)

print("Building NiFi flow (v2 - with SplitJson)...")

# ── Controller Services ──
print("- Creating controller services...")
dbcp_id, dbcp_v = create_controller_service("DBCPConnectionPool", "DBCPConnectionPool")
dbcp_v = update_service(dbcp_id, dbcp_v, {
    "Database Connection URL": "jdbc:postgresql://warehouse:5432/datawarehouse",
    "Database Driver Class Name": "org.postgresql.Driver",
    "Database Driver Location(s)": "/opt/nifi/nifi-current/lib/postgresql-42.7.3.jar",
    "Database User": "dw_user",
    "Password": "dw_password"
})
enable_service(dbcp_id, dbcp_v)

json_reader_id, json_reader_v = create_controller_service("JsonTreeReader", "JsonTreeReader")
enable_service(json_reader_id, json_reader_v)

json_writer_id, json_writer_v = create_controller_service("JsonRecordSetWriter", "JsonRecordSetWriter")
enable_service(json_writer_id, json_writer_v)

# ── Processors ──
print("- Creating processors...")
consume_id, consume_v = create_processor("ConsumeKafkaRecord", "ConsumeKafkaRecord")
split_id, split_v = create_processor("SplitJson", "SplitJson")
eval_id, eval_v = create_processor("EvaluateJsonPath", "EvaluateJsonPath")
route_id, route_v = create_processor("RouteOnAttribute", "RouteOnAttribute")
put_insert_id, put_insert_v = create_processor("PutSQL_Insert", "PutSQL")
put_update_id, put_update_v = create_processor("PutSQL_Update", "PutSQL")
put_delete_id, put_delete_v = create_processor("PutSQL_Delete", "PutSQL")
log_id, log_v = create_processor("LogAttribute", "LogAttribute")

# ── Configure ──
print("- Configuring ConsumeKafkaRecord...")
update_processor(consume_id, consume_v, {
    "bootstrap.servers": "kafka:9092",
    "topic": "cdc.testdb.users",
    "group.id": "nifi-cdc-consumer-v4",
    "auto.offset.reset": "earliest",
    "record-reader": json_reader_id,
    "record-writer": json_writer_id,
    "honor-transactions": "false"
})

print("- Configuring SplitJson...")
update_processor(split_id, split_v, {
    "JsonPath Expression": "$.*"
}, auto_terminate=["failure", "original"])

print("- Configuring EvaluateJsonPath...")
update_processor(eval_id, eval_v, {
    "Destination": "flowfile-attribute",
    "Return Type": "auto-detect",
    "cdc_id": "$.id",
    "cdc_name": "$.name",
    "cdc_email": "$.email",
    "cdc_created_at": "$.created_at",
    "cdc_updated_at": "$.updated_at",
    "cdc_operation": "$.__op",
    "cdc_timestamp": "$.__ts_ms"
})

print("- Configuring RouteOnAttribute...")
body = {
    "revision": {"version": route_v},
    "component": {
        "id": route_id,
        "config": {
            "properties": {
                "Routing Strategy": "Route to Property name",
                "insert": "${cdc_operation:equals('c')}",
                "update": "${cdc_operation:equals('u')}",
                "delete": "${cdc_operation:equals('d')}",
                "snapshot": "${cdc_operation:equals('r')}"
            }
        }
    }
}
res = put(f"processors/{route_id}", body)

sql_insert = """INSERT INTO dw_users (id, name, email, created_at, updated_at, cdc_operation, cdc_timestamp)
VALUES (
    ${cdc_id},
    '${cdc_name}',
    '${cdc_email}',
    NULLIF('${cdc_created_at}', '')::TIMESTAMP,
    NULLIF('${cdc_updated_at}', '')::TIMESTAMP,
    '${cdc_operation}',
    ${cdc_timestamp}
)"""

sql_delete = """INSERT INTO dw_users (id, name, email, created_at, updated_at, cdc_operation, cdc_timestamp)
VALUES (
    ${cdc_id},
    NULLIF('${cdc_name}', ''),
    NULLIF('${cdc_email}', ''),
    NULL,
    NULL,
    'd',
    ${cdc_timestamp}
)"""

print("- Configuring PutSQL processors...")
update_processor(put_insert_id, put_insert_v, {
    "JDBC Connection Pool": dbcp_id,
    "putsql-sql-statement": sql_insert
}, auto_terminate=["success"])

update_processor(put_update_id, put_update_v, {
    "JDBC Connection Pool": dbcp_id,
    "putsql-sql-statement": sql_insert
}, auto_terminate=["success"])

update_processor(put_delete_id, put_delete_v, {
    "JDBC Connection Pool": dbcp_id,
    "putsql-sql-statement": sql_delete
}, auto_terminate=["success"])

print("- Configuring LogAttribute...")
update_processor(log_id, log_v, {
    "Log Level": "warn",
    "attributes-to-log-regex": ".*",
    "Log Payload": "true"
}, auto_terminate=["success"])

# ── Connections ──
print("- Creating connections...")
# ConsumeKafkaRecord -> SplitJson
create_connection(consume_id, split_id, ["success"])
create_connection(consume_id, log_id, ["parse.failure"])

# SplitJson -> EvaluateJsonPath
create_connection(split_id, eval_id, ["split"])

# EvaluateJsonPath -> RouteOnAttribute
create_connection(eval_id, route_id, ["matched", "unmatched"])
create_connection(eval_id, log_id, ["failure"])

# RouteOnAttribute -> PutSQL
create_connection(route_id, put_insert_id, ["insert", "snapshot"])
create_connection(route_id, put_update_id, ["update"])
create_connection(route_id, put_delete_id, ["delete"])
create_connection(route_id, log_id, ["unmatched"])

# PutSQL failures -> LogAttribute
create_connection(put_insert_id, log_id, ["failure", "retry"])
create_connection(put_update_id, log_id, ["failure", "retry"])
create_connection(put_delete_id, log_id, ["failure", "retry"])

# ── Start ──
print("- Starting all processors...")
start_processor(log_id)
start_processor(put_insert_id)
start_processor(put_update_id)
start_processor(put_delete_id)
start_processor(route_id)
start_processor(eval_id)
start_processor(split_id)
start_processor(consume_id)

print("\n✅ NiFi flow deployed and started successfully!")
