import socket
import threading
import time
import os
import psutil
import logging
import sys
from config import *
from utils import send_json, receive_json, recv_all, calculate_checksum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Node-%(process)d - %(levelname)s - %(message)s')

class NodeServer:
    def __init__(self, node_id, port, master_host=MASTER_HOST, master_port=MASTER_PORT):
        self.node_id = node_id
        self.port = port
        self.master_host = master_host
        self.master_port = master_port
        self.storage_path = os.path.join(STORAGE_ROOT, f"node_{node_id}")
        self.running = True
        
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
            
        logging.info(f"Node {self.node_id} initialized. Storage: {self.storage_path}")

    def start(self):
        """Start the node server (heartbeat and command listener)."""
        # Start heartbeat thread
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        
        # Start TCP listener
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(('0.0.0.0', self.port))
        server_sock.listen(5)
        logging.info(f"Node {self.node_id} listening on port {self.port}")
        
        try:
            while self.running:
                client_sock, addr = server_sock.accept()
                threading.Thread(target=self.handle_client, args=(client_sock,), daemon=True).start()
        except Exception as e:
            logging.error(f"Node {self.node_id} server error: {e}")
        finally:
            server_sock.close()

    def heartbeat_loop(self):
        """Periodically send heartbeat with stats to Master."""
        while self.running:
            try:
                stats = self.get_stats()
                message = {
                    'type': 'HEARTBEAT',
                    'node_id': self.node_id,
                    'port': self.port,
                    'stats': stats
                }
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.master_host, self.master_port))
                    send_json(sock, message)
                    
            except ConnectionRefusedError:
                logging.warning(f"Node {self.node_id} could not connect to Master at {self.master_host}:{self.master_port}")
            except Exception as e:
                logging.error(f"Heartbeat error: {e}")
                
            time.sleep(HEARTBEAT_INTERVAL)

    def get_stats(self):
        """Gather system metrics using psutil."""
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(self.storage_path)
        
        # For demo purposes on localhost: Add slight jitter so nodes look distinct
        import random
        jitter_cpu = random.uniform(-1.5, 1.5)
        jitter_mem = random.uniform(-0.5, 0.5)
        
        return {
            'cpu': max(0, round(psutil.cpu_percent() + jitter_cpu, 1)),
            'ram_percent': max(0, round(mem.percent + jitter_mem, 1)),
            'ram_used': mem.used,
            'disk_percent': disk.percent,
            'disk_free': disk.free
        }

    def handle_client(self, client_sock):
        """Handle incoming commands from Master or Client."""
        try:
            command = receive_json(client_sock)
            if not command:
                return

            cmd_type = command.get('type')
            
            if cmd_type == 'STORE_CHUNK':
                self.handle_store_chunk(client_sock, command)
            elif cmd_type == 'RETRIEVE_CHUNK':
                self.handle_retrieve_chunk(client_sock, command)
            elif cmd_type == 'DELETE_CHUNK':
                self.handle_delete_chunk(client_sock, command)
            else:
                logging.warning(f"Unknown command: {cmd_type}")
                
        except Exception as e:
            logging.error(f"Error handling client: {e}")
        finally:
            client_sock.close()

    def handle_store_chunk(self, sock, command):
        """
        Receive chunk data and save to disk.
        Protocol:
        1. Receive JSON command (already done) containing chunk_id and data_size.
        2. Receive raw bytes.
        3. Save to file.
        4. Send ack.
        """
        chunk_id = command['chunk_id']
        size = command['size']
        
        data = recv_all(sock, size)
        if not data:
            logging.error("Failed to receive chunk data")
            return

        filepath = os.path.join(self.storage_path, chunk_id)
        with open(filepath, 'wb') as f:
            f.write(data)
            
        checksum = calculate_checksum(data)
        logging.info(f"Stored chunk {chunk_id}, size {size}, checksum {checksum[:8]}...")
        
        send_json(sock, {'status': 'OK', 'checksum': checksum})

    def handle_retrieve_chunk(self, sock, command):
        """
        Read chunk from disk and send back.
        """
        chunk_id = command['chunk_id']
        filepath = os.path.join(self.storage_path, chunk_id)
        
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                data = f.read()
            send_json(sock, {'status': 'OK', 'size': len(data)})
            sock.sendall(data)
            logging.info(f"Served chunk {chunk_id}")
        else:
            send_json(sock, {'status': 'ERROR', 'message': 'Chunk not found'})

    def handle_delete_chunk(self, sock, command):
        chunk_id = command['chunk_id']
        filepath = os.path.join(self.storage_path, chunk_id)
        
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info(f"Deleted chunk {chunk_id}")
            send_json(sock, {'status': 'OK'})
        else:
             # Even if not found, we consider delete successful (idempotent)
            send_json(sock, {'status': 'OK', 'message': 'Chunk not found'})

def start_node():
    if len(sys.argv) < 3:
        print("Usage: python node.py <node_id> <port>")
        sys.exit(1)
        
    node_id = sys.argv[1]
    port = int(sys.argv[2])
    
    node = NodeServer(node_id, port)
    node.start()

if __name__ == "__main__":
    start_node()
