import psutil
import sys

PORTS_TO_CHECK = [5000, 6001, 6002, 6003, 6004, 6005]

def kill_processes_on_ports():
    print("Checking for zombie processes on DFS ports...")
    killed_count = 0
    
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            for conn in proc.connections(kind='inet'):
                if conn.laddr.port in PORTS_TO_CHECK:
                    print(f"Killing {proc.name()} (PID: {proc.pid}) on port {conn.laddr.port}")
                    proc.kill()
                    killed_count += 1
                    break
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
            
    if killed_count == 0:
        print("No conflicting processes found.")
    else:
        print(f"Killed {killed_count} processes.")

if __name__ == "__main__":
    kill_processes_on_ports()
