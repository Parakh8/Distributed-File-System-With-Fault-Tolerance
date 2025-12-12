import os

# Network Constants
MASTER_HOST = 'localhost'
MASTER_PORT = 5000
NODE_PORTS_START = 6000
NUM_NODES = 3  # Default number of nodes to start for demo

# DFS Constants
BLOCK_SIZE = 1024 * 1024  # 1 MB chunk size
REPLICATION_FACTOR = 2    # Number of replicas per chunk
HEARTBEAT_INTERVAL = 2    # Seconds
NODE_TIMEOUT = 6          # Seconds (3 missed heartbeats)

# Storage Paths
STORAGE_ROOT = "dfs_storage"
if not os.path.exists(STORAGE_ROOT):
    os.makedirs(STORAGE_ROOT)
