#!/usr/bin/env python3
"""Check and start cluster if needed"""

import sys
import os
import json
import requests
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

# Databricks API
host = config['databricks']['host']
token = config['databricks']['token']
lineage_cluster_id = config['databricks'].get('lineage_cluster_id')
sandbox_cluster_id = config['databricks']['cluster_id']

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

print("Checking cluster status...")
print("=" * 80)

for name, cluster_id in [('Lineage', lineage_cluster_id), ('Sandbox', sandbox_cluster_id)]:
    if not cluster_id:
        continue

    response = requests.get(f"{host}/api/2.0/clusters/get?cluster_id={cluster_id}", headers=headers)

    if response.status_code == 200:
        cluster = response.json()
        state = cluster.get('state', 'UNKNOWN')
        cluster_name = cluster.get('cluster_name', 'N/A')

        print(f"\n{name} Cluster: {cluster_name}")
        print(f"  ID: {cluster_id}")
        print(f"  State: {state}")

        if state == 'TERMINATED':
            print(f"  Action: Starting cluster...")
            start_response = requests.post(
                f"{host}/api/2.0/clusters/start",
                headers=headers,
                json={'cluster_id': cluster_id}
            )

            if start_response.status_code == 200:
                print(f"  Status: Cluster start initiated")
                print(f"  Note: Wait 3-5 minutes for cluster to be ready")
            else:
                print(f"  Error: Failed to start - {start_response.text}")
        elif state == 'RUNNING':
            print(f"  Status: Ready to use")
        elif state in ['PENDING', 'RESTARTING']:
            print(f"  Status: Starting... (wait a few minutes)")
    else:
        print(f"\n{name} Cluster: ERROR")
        print(f"  {response.status_code} - {response.text}")

print("\n" + "=" * 80)
print("Use the RUNNING cluster for processing")
