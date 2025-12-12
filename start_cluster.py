import subprocess
import time
import sys
import os

def main():
    print("Starting Distributed File System Cluster...")
    
    # 1. Start Master
    print("Launching Master...")
    # Use cmd /k to keep window open if it crashes
    master_cmd = f"start cmd /k {sys.executable} master.py" 
    subprocess.run(master_cmd, shell=True)
    time.sleep(2) # Wait for master to be ready
    
    # 2. Start Nodes
    num_nodes = 3
    start_port = 6001
    
    for i in range(num_nodes):
        node_id = f"node_{i+1}"
        port = start_port + i
        print(f"Launching Node {node_id} on port {port}...")
        node_cmd = f"start cmd /k {sys.executable} node.py {node_id} {port}"
        subprocess.run(node_cmd, shell=True)
        
    print("\nCluster is running!")
    print("You can now run 'python client_app.py' to start the GUI.")
    print("Press Ctrl+C to stop the cluster (or close the windows manually).")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping cluster...")
        master_proc.terminate()
        for p in nodes:
            p.terminate()

if __name__ == "__main__":
    main()
