import requests
import json
import time

BASE_URL = "http://localhost:8081/nifi-api"

def get(path):
    res = requests.get(f"{BASE_URL}/{path}")
    res.raise_for_status()
    return res.json()

def put(path, body):
    res = requests.put(f"{BASE_URL}/{path}", json=body)
    res.raise_for_status()
    return res.json()

def delete(path):
    requests.delete(f"{BASE_URL}/{path}").raise_for_status()

# stop all processors
root_id = get("flow/process-groups/root")['processGroupFlow']['id']
procs = get(f"process-groups/{root_id}/processors")['processors']
for p in procs:
    body = {"revision": p['revision'], "state": "STOPPED"}
    put(f"processors/{p['id']}/run-status", body)

time.sleep(1)

# delete connections
conns = get(f"process-groups/{root_id}/connections")['connections']
for c in conns:
    delete(f"connections/{c['id']}?version={c['revision']['version']}")

# delete processors
for p in get(f"process-groups/{root_id}/processors")['processors']:
    delete(f"processors/{p['id']}?version={p['revision']['version']}")

# delete controller services (disable then delete)
css = get(f"flow/process-groups/{root_id}/controller-services")['controllerServices']
for cs in css:
    body = {"revision": cs['revision'], "state": "DISABLED"}
    put(f"controller-services/{cs['id']}/run-status", body)
time.sleep(1)
css = get(f"flow/process-groups/{root_id}/controller-services")['controllerServices']
for cs in css:
    delete(f"controller-services/{cs['id']}?version={cs['revision']['version']}")

print("Canvas cleared")
