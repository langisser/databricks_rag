#!/usr/bin/env python3
"""Find dtx-dp-lineage cluster ID"""

import sys
import os
import json
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

# Databricks API
host = config['databricks']['host']
token = config['databricks']['token']

# List clusters
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

response = requests.get(f"{host}/api/2.0/clusters/list", headers=headers)

if response.status_code == 200:
    clusters = response.json().get('clusters', [])

    print(f"Found {len(clusters)} clusters:\n")
    print(f"{'Cluster Name':<40} {'Cluster ID':<30} {'State':<15}")
    print("-" * 90)

    for cluster in clusters:
        name = cluster.get('cluster_name', 'N/A')
        cluster_id = cluster.get('cluster_id', 'N/A')
        state = cluster.get('state', 'N/A')

        print(f"{name:<40} {cluster_id:<30} {state:<15}")

        # Check for dtx-dp-lineage
        if 'dtx-dp-lineage' in name.lower() or 'lineage' in name.lower():
            print(f"\n>>> FOUND dtx-dp-lineage cluster: {cluster_id}")

            # Update config
            config['databricks']['lineage_cluster_id'] = cluster_id
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            print(f">>> Updated config.json with lineage_cluster_id")
else:
    print(f"Error: {response.status_code} - {response.text}")
