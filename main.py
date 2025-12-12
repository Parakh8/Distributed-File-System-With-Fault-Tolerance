import sys
import subprocess
import time
import os
import tkinter as tk
from config import *
from cleanup_ports import kill_processes_on_ports
from client_app import DFSGUI
import master
import node

def run_master_cli():
    # Wrapper to run master
    master.start_master()

def run_node_cli():
    # Wrapper to run node, args already in sys.argv
    node.start_node()

def start_backend_frozen():
    """Start backend processes assuming we are a frozen executable."""
    print("Starting Distributed File System Backend (Frozen/Unified)...")
    
    # 1. Cleanup
    try:
        cleanup_ports_list = [MASTER_PORT] + [NODE_PORTS_START + i for i in range(10)]
        kill_processes_on_ports(cleanup_ports_list)
    except Exception as e:
        print(f"Cleanup warning: {e}")

    # Ensure logs dir
    if not os.path.exists("logs"):
        os.makedirs("logs")

    CREATE_NO_WINDOW = 0x08000000

    # 2. Start Master
    print("Launching Master Node (Background)...")
    master_log = open("logs/master.log", "w")
    # Launch main.exe with 'master' argument
    subprocess.Popen(
        [sys.executable, "master"], 
        creationflags=CREATE_NO_WINDOW,
        stdout=master_log, 
        stderr=master_log
    )
    time.sleep(1)

    # 3. Start Nodes
    num_nodes = NUM_NODES
    start_port = NODE_PORTS_START
    
    print(f"Launching {num_nodes} Storage Nodes (Background)...")
    for i in range(num_nodes):
        node_id = f"node_{i+1}"
        port = start_port + i + 1 
        
        log_file = open(f"logs/{node_id}.log", "w")
        # Launch main.exe with 'node' argument
        subprocess.Popen(
            [sys.executable, "node", node_id, str(port)], 
            creationflags=CREATE_NO_WINDOW,
            stdout=log_file,
            stderr=log_file
        )

    print("Backend running in background. Logs are in /logs folder.")

def start_backend_script():
    # Same as before but calling python main.py <arg>
    print("Starting Distributed File System Backend (Script Mode)...")
    try:
        cleanup_ports_list = [MASTER_PORT] + [NODE_PORTS_START + i for i in range(10)]
        kill_processes_on_ports(cleanup_ports_list)
    except Exception as e: # Handle argument mismatches in cleanup
         try:
             kill_processes_on_ports()
         except: pass

    if not os.path.exists("logs"): os.makedirs("logs")
    CREATE_NO_WINDOW = 0x08000000

    master_log = open("logs/master.log", "w")
    subprocess.Popen(
        [sys.executable, "main.py", "master"], 
        creationflags=CREATE_NO_WINDOW,
        stdout=master_log, 
        stderr=master_log
    )
    time.sleep(1)

    for i in range(NUM_NODES):
        node_id = f"node_{i+1}"
        port = NODE_PORTS_START + i + 1
        log_file = open(f"logs/{node_id}.log", "w")
        subprocess.Popen(
            [sys.executable, "main.py", "node", node_id, str(port)], 
            creationflags=CREATE_NO_WINDOW,
            stdout=log_file,
            stderr=log_file
        )

def main_gui():
    print("Initializing DFS System...")
    
    # Detect if we are running as a script or frozen
    if getattr(sys, 'frozen', False):
        start_backend_frozen()
    else:
        start_backend_script()

    print("Launching GUI...")
    root = tk.Tk()
    app = DFSGUI(root)
    
    def on_closing():
        print("Shutting down system...")
        try:
            cleanup_ports_list = [MASTER_PORT] + [NODE_PORTS_START + i for i in range(10)]
            kill_processes_on_ports(cleanup_ports_list)
        except:
             kill_processes_on_ports()
        root.destroy()
        sys.exit(0)

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'master':
            run_master_cli()
        elif sys.argv[1] == 'node':
            # Remove the 'node' arg so node.py sees [script, id, port]
            # sys.argv is [main.py, node, id, port]
            # node.py expects [script, id, port]
            sys.argv.pop(1) 
            run_node_cli()
        else:
            main_gui()
    else:
        main_gui()
