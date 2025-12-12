import socket
import threading
import time
import json
import logging
import random
import uuid
from config import *
from utils import send_json, receive_json, recv_all

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

class MasterService:
    def __init__(self, host=MASTER_HOST, port=MASTER_PORT):
        self.host = host
        self.port = port
        self.running = True
        self.metadata_file = "dfs_metadata.json"
        
        # Registry
        # node_id -> {address: (ip, port), last_heartbeat: timestamp, status: 'ONLINE', stats: {}}
        self.nodes = {} 
        
        # Files
        # filename -> {size: int, chunks: [chunk_id_1, ...]}
        self.files = {}
        
        # Chunk locations
        # chunk_id -> [node_id_1, node_id_2]
        self.chunk_locations = {}
        
        self.lock = threading.RLock() # Thread safety for registries
        self.load_metadata()

    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    self.files = data.get('files', {})
                    self.chunk_locations = data.get('chunk_locations', {})
                logging.info(f"Loaded metadata: {len(self.files)} files.")
            except Exception as e:
                logging.error(f"Failed to load metadata: {e}")

    def save_metadata(self):
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump({
                    'files': self.files,
                    'chunk_locations': self.chunk_locations
                }, f)
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")

    def start(self):
        # Start Failure Detector
        threading.Thread(target=self.failure_detector_loop, daemon=True).start()
        
        # Start TCP Server
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('0.0.0.0', self.port))
        server_sock.listen(10)
        logging.info(f"Master listening on {self.port}")
        
        try:
            while self.running:
                client_sock, addr = server_sock.accept()
                threading.Thread(target=self.handle_client, args=(client_sock,), daemon=True).start()
        except Exception as e:
            logging.error(f"Master server error: {e}")
        finally:
            server_sock.close()

    def failure_detector_loop(self):
        """Monitor heartbeats and trigger replication if node fails."""
        while self.running:
            time.sleep(1)
            with self.lock:
                now = time.time()
                for node_id, info in list(self.nodes.items()):
                    if info['status'] == 'ONLINE':
                        if now - info['last_heartbeat'] > NODE_TIMEOUT:
                            logging.warning(f"Node {node_id} TIMED OUT! Marking OFFLINE.")
                            info['status'] = 'OFFLINE'
                            threading.Thread(target=self.handle_node_failure, args=(node_id,), daemon=True).start()

    def handle_node_failure(self, failed_node_id):
        """Identify lost chunks and replicate them."""
        logging.info(f"Starting replication for failed node {failed_node_id}")
        chunks_to_replicate = []
        
        with self.lock:
            # Find all chunks that were on this node
            for chunk_id, locations in self.chunk_locations.items():
                if failed_node_id in locations:
                    locations.remove(failed_node_id)
                    chunks_to_replicate.append(chunk_id)
        
        for chunk_id in chunks_to_replicate:
            self.replicate_chunk(chunk_id)

    def replicate_chunk(self, chunk_id):
        """
        Copy chunk from a healthy replica to a new node.
        """
        # 1. Find a source node
        source_node_id = None
        current_locations = []
        with self.lock:
            current_locations = self.chunk_locations.get(chunk_id, [])
            for node_id in current_locations:
                if self.nodes.get(node_id, {}).get('status') == 'ONLINE':
                    source_node_id = node_id
                    break
        
        if not source_node_id:
            logging.error(f"DATA LOSS WARNING: No healthy replicas for chunk {chunk_id}")
            return

        # 2. Find a destination node (not already holding the chunk)
        dest_node_id = None
        with self.lock:
            online_nodes = [nid for nid, info in self.nodes.items() if info['status'] == 'ONLINE']
            candidates = [nid for nid in online_nodes if nid not in current_locations]
            if candidates:
                dest_node_id = random.choice(candidates)
        
        if not dest_node_id:
            logging.warning(f"Cannot replicate chunk {chunk_id}: No available destination nodes.")
            return

        logging.info(f"Replicating chunk {chunk_id} from {source_node_id} to {dest_node_id}")

        # 3. Perform transfer via Master (Source -> Master -> Dest)
        try:
            # Fetch from Source
            source_info = self.nodes[source_node_id]
            data = None
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(source_info['address'])
                send_json(sock, {'type': 'RETRIEVE_CHUNK', 'chunk_id': chunk_id})
                resp = receive_json(sock)
                if resp and resp['status'] == 'OK':
                    data = recv_all(sock, resp['size'])
            
            if data:
                # Push to Dest
                dest_info = self.nodes[dest_node_id]
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(dest_info['address'])
                    send_json(sock, {'type': 'STORE_CHUNK', 'chunk_id': chunk_id, 'size': len(data)})
                    sock.sendall(data)
                    ack = receive_json(sock)
                    if ack and ack['status'] == 'OK':
                        with self.lock:
                            if chunk_id in self.chunk_locations:
                                self.chunk_locations[chunk_id].append(dest_node_id)
                            logging.info(f"Replication successful for {chunk_id}")
        except Exception as e:
            logging.error(f"Replication failed for {chunk_id}: {e}")

    def handle_client(self, sock):
        try:
            request = receive_json(sock)
            if not request:
                return
            
            req_type = request.get('type')
            
            if req_type == 'HEARTBEAT':
                self.handle_heartbeat(request)
            elif req_type == 'GET_STATS':
                 with self.lock:
                    send_json(sock, {'status': 'OK', 'nodes': self.nodes})
            elif req_type == 'UPLOAD_INIT':
                self.handle_upload_init(sock, request)
            elif req_type == 'UPLOAD_SUCCESS':
                self.handle_upload_success(request)
            elif req_type == 'DOWNLOAD_REQ':
                self.handle_download_req(sock, request)
            elif req_type == 'LIST_FILES':
                with self.lock:
                     # Calculate total size correctly
                    file_list = []
                    for fname, meta in self.files.items():
                        # Count available replicas for health status
                         file_list.append({
                             'filename': fname, 
                             'size': meta['size'],
                             'status': 'Available' # Simplified
                         })
                    send_json(sock, {'status': 'OK', 'files': file_list})
            elif req_type == 'DELETE_FILE':
                self.handle_delete_file(sock, request)
            else:
                send_json(sock, {'status': 'ERROR', 'message': 'Unknown command'})
                
        except Exception as e:
            logging.error(f"Client handler error: {e}")
        finally:
            sock.close()

    def handle_delete_file(self, sock, request):
        filename = request['filename']
        logging.info(f"Received delete request for {filename}")
        
        chunks_to_delete = []
        with self.lock:
            if filename not in self.files:
                send_json(sock, {'status': 'ERROR', 'message': 'File not found'})
                return
            
            # Get chunks to delete
            chunk_ids = self.files[filename]['chunks']
            for cid in chunk_ids:
                if cid in self.chunk_locations:
                    chunks_to_delete.append({
                        'chunk_id': cid, 
                        'nodes': list(self.chunk_locations[cid]) # Copy list
                    })
                    del self.chunk_locations[cid]
            
            # Remove file metadata
            del self.files[filename]
            self.save_metadata()
            
        # Notify nodes to delete chunks (best effort, no lock needed here)
        # We spawned a thread for this to not block the client response too long?
        # Actually client waits for confirmation. Let's do it inline but fast.
        
        threading.Thread(target=self._cleanup_chunks, args=(chunks_to_delete,), daemon=True).start()
        
        send_json(sock, {'status': 'OK'})
        logging.info(f"File {filename} deleted.")

    def _cleanup_chunks(self, chunks_list):
        for item in chunks_list:
            cid = item['chunk_id']
            for node_id in item['nodes']:
                try:
                    with self.lock:
                        if node_id not in self.nodes: continue
                        node_info = self.nodes[node_id]
                        
                    if node_info['status'] == 'ONLINE':
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ns:
                            ns.connect(node_info['address'])
                            send_json(ns, {'type': 'DELETE_CHUNK', 'chunk_id': cid})
                            receive_json(ns) # Wait for ack
                except Exception as e:
                    logging.warning(f"Failed to cleanup chunk {cid} on {node_id}: {e}")

    def handle_heartbeat(self, request):
        node_id = request['node_id']
        port = request['port']
        stats = request['stats']
        
        with self.lock:
            # If new node or updating existing
            self.nodes[node_id] = {
                'address': ('localhost', port), # Assuming localhost for this demo
                'last_heartbeat': time.time(),
                'status': 'ONLINE',
                'stats': stats
            }

    def handle_upload_init(self, sock, request):
        """
        Client asks to upload file.
        Returns: [ {chunk_id, [node_ips]} ]
        """
        filename = request['filename']
        filesize = request['filesize']
        
        num_chunks = (filesize + BLOCK_SIZE - 1) // BLOCK_SIZE
        chunks_plan = []
        
        with self.lock:
            online_nodes = [nid for nid, info in self.nodes.items() if info['status'] == 'ONLINE']
            
            if len(online_nodes) < 1:
                send_json(sock, {'status': 'ERROR', 'message': 'No online nodes'})
                return

            for i in range(num_chunks):
                chunk_id = f"{filename}_chunk_{i}_{uuid.uuid4().hex[:8]}"
                # Choose replicas
                # Round robin or random. Let's do random for simplicity but ensure distinct
                replicas = []
                available = list(online_nodes)
                count = min(REPLICATION_FACTOR, len(available))
                replicas = random.sample(available, count)
                
                # Format for client: list of (ip, port)
                replica_addrs = [self.nodes[nid]['address'] for nid in replicas]
                
                chunks_plan.append({
                    'chunk_id': chunk_id,
                    'nodes': replica_addrs
                })
        
        send_json(sock, {'status': 'OK', 'chunks': chunks_plan})

    def handle_upload_success(self, request):
        """
        Client confirms upload. Commit metadata.
        Client sends chunks_placed: [{chunk_id, nodes: [[ip, port], ...]}]
        Master resolves [ip, port] to node_ids.
        """
        filename = request['filename']
        filesize = request['filesize']
        
        with self.lock:
            chunk_ids = []
            for item in request['chunks_placed']:
                c_id = item['chunk_id']
                chunk_ids.append(c_id)
                
                # Resolve addresses to Node IDs
                resolved_node_ids = []
                for addr_list in item['nodes']:
                    addr_tuple = tuple(addr_list)
                    # Find node_id for this address
                    for nid, info in self.nodes.items():
                        if info['address'] == addr_tuple:
                            resolved_node_ids.append(nid)
                            break
                
                self.chunk_locations[c_id] = resolved_node_ids
            
            self.files[filename] = {
                'size': filesize,
                'chunks': chunk_ids
            }
            self.save_metadata()
        logging.info(f"File {filename} uploaded successfully.")

    def handle_download_req(self, sock, request):
        filename = request['filename']
        with self.lock:
            if filename not in self.files:
                send_json(sock, {'status': 'ERROR', 'message': 'File not found'})
                return
            
            file_meta = self.files[filename]
            plan = []
            for chunk_id in file_meta['chunks']:
                locs = self.chunk_locations.get(chunk_id, [])
                # Filter for online nodes
                alive_locs = [nid for nid in locs if self.nodes.get(nid, {}).get('status') == 'ONLINE']
                
                if not alive_locs:
                     send_json(sock, {'status': 'ERROR', 'message': 'Data unavailable'})
                     return
                     
                plan.append({
                    'chunk_id': chunk_id,
                    'nodes': [self.nodes[nid]['address'] for nid in alive_locs]
                })
                
            send_json(sock, {'status': 'OK', 'filesize': file_meta['size'], 'chunks': plan})

def start_master():
    master = MasterService()
    master.start()

if __name__ == "__main__":
    start_master()
