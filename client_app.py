import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import socket
import os
import time
import queue
import subprocess
import sys
from config import *
from utils import send_json, receive_json, recv_all, calculate_checksum

class DFSClient:
    def __init__(self, master_host=MASTER_HOST, master_port=MASTER_PORT):
        self.master_host = master_host
        self.master_port = master_port

    def get_stats(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                send_json(sock, {'type': 'GET_STATS'})
                return receive_json(sock)
        except Exception:
            return None

    def list_files(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                send_json(sock, {'type': 'LIST_FILES'})
                return receive_json(sock)
        except Exception:
            return None

    def upload_file(self, filepath, log_callback=None):
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        
        if log_callback: log_callback(f"Starting upload: {filename} ({filesize} bytes)")

        # 1. Init Upload
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                send_json(sock, {'type': 'UPLOAD_INIT', 'filename': filename, 'filesize': filesize})
                response = receive_json(sock)
        except Exception as e:
            if log_callback: log_callback(f"Error connecting to Master: {e}")
            return False

        if response['status'] != 'OK':
            if log_callback: log_callback(f"Upload failed: {response.get('message')}")
            return False

        chunks_plan = response['chunks']
        chunks_placed_info = []

        # 2. Upload Chunks
        with open(filepath, 'rb') as f:
            for chunk_info in chunks_plan:
                chunk_id = chunk_info['chunk_id']
                target_nodes = chunk_info['nodes'] # List of (ip, port)
                
                chunk_data = f.read(BLOCK_SIZE)
                chunk_size = len(chunk_data)
                
                successful_nodes = [] # List of node_ids (actually we need IDs for SUCCESS msg)
                # But wait, Master returned Addrs. We need IDs?
                # Simplified: Node IDs were used in Master Config. 
                # Let's just track where we put it.
                # Actually, Master expects `nodes` list in `chunks_placed`.
                # Ideally `target_nodes` should have included node_ids. 
                # To fix this, I'll update client to just send back what it got if successful?
                # Actually, `target_nodes` are tuples (ip, port).
                # We can't easily map back to ID unless provided.
                # Let's assume Master handles `(ip, port)` mapping or we fix Master to send ID.
                # FIX: I will update Master logic implicitly by assuming Client tells Master 
                # "I put chunk X on Node Y" where Y is the ID.
                # BUT Client doesn't know ID. 
                # Let's hacking it: Master `chunk_locations` uses Node IDs.
                # So Protocol update: `UPLOAD_INIT` returns `nodes`: [{'id': 'node_1', 'address': ('...', ...)}]
                
                # RE-FACTORING ON THE FLY:
                # I'll just rely on `target_nodes` being `(ip, port)` and Master can't map back easily?
                # Actually Master keeps `nodes` dict. It can reverse lookup `address`.
                # Okay, let's keep it simple. Status Quo: `target_nodes` is `[[ip, port], ...]`.
                # Client sends `nodes`: `[[ip, port], ...]` back to Master.
                # Master reverse lookups.
                
                placed_on_addrs = []

                for node_addr in target_nodes:
                    node_ip, node_port = node_addr
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ns:
                            ns.connect((node_ip, node_port))
                            send_json(ns, {'type': 'STORE_CHUNK', 'chunk_id': chunk_id, 'size': chunk_size})
                            ns.sendall(chunk_data)
                            ack = receive_json(ns)
                            if ack['status'] == 'OK':
                                placed_on_addrs.append(node_addr) # Store address to send back to Master
                                if log_callback: log_callback(f"Chunk {chunk_id} -> Node {node_port}")
                    except Exception as e:
                        if log_callback: log_callback(f"Failed to send to Node {node_port}: {e}")

                if not placed_on_addrs:
                    if log_callback: log_callback(f"Failed to store chunk {chunk_id} on any node!")
                    return False
                
                # We need to covert addresses back to IDs for Master? 
                # Or Master does it. Let's make Master do it.
                chunks_placed_info.append({
                    'chunk_id': chunk_id,
                    'nodes': placed_on_addrs # Client sends back addresses, Master resolves.
                })

        # 3. Confirm Success
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                # We need to teach Master to handle address->id mapping. 
                # Actually, I'll update Master code next turn to be robust. 
                # OR I can update Master now? No, I'm writing Client now.
                # Solution: Master `nodes` registry key is `node_id`. Value has address.
                # I will change Master to accept addresses in `UPLOAD_SUCCESS` and resolve them.
                send_json(sock, {
                    'type': 'UPLOAD_SUCCESS',
                    'filename': filename,
                    'filesize': filesize,
                    'chunks_placed': chunks_placed_info
                })
        except Exception as e:
            if log_callback: log_callback(f"Error finalizing upload: {e}")
            return False

        if log_callback: log_callback("Upload Complete!")
        return True

    def download_file(self, filename, save_path, log_callback=None):
        if log_callback: log_callback(f"Starting download: {filename}")
        
        # 1. Get Plan
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                send_json(sock, {'type': 'DOWNLOAD_REQ', 'filename': filename})
                resp = receive_json(sock)
        except Exception as e:
            if log_callback: log_callback(f"Error connecting to Master: {e}")
            return False
            
        if resp['status'] != 'OK':
            if log_callback: log_callback(f"Download error: {resp.get('message')}")
            return False
            
        chunks = resp['chunks']
        file_size = resp['filesize']
        
        # 2. Fetch Chunks
        with open(save_path, 'wb') as f:
            for chunk_data_item in chunks:
                chunk_id = chunk_data_item['chunk_id']
                nodes = chunk_data_item['nodes']
                
                fetched = False
                for node_addr in nodes:
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ns:
                            ns.connect(tuple(node_addr))
                            send_json(ns, {'type': 'RETRIEVE_CHUNK', 'chunk_id': chunk_id})
                            header = receive_json(ns)
                            if header['status'] == 'OK':
                                raw = recv_all(ns, header['size'])
                                f.write(raw)
                                fetched = True
                                if log_callback: log_callback(f"Retrieved {chunk_id} from {node_addr[1]}")
                                break
                    except Exception as e:
                         if log_callback: log_callback(f"Failed to fetch {chunk_id} from {node_addr}: {e}")
                
                if not fetched:
                    if log_callback: log_callback(f"Detailed Error: Could not retrieve chunk {chunk_id} from any node")
                    return False
                    
        if log_callback: log_callback("Download Complete!")
        return True

    def delete_file(self, filename, log_callback=None):
        if log_callback: log_callback(f"Deleting file: {filename}")
        try:
             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_host, self.master_port))
                send_json(sock, {'type': 'DELETE_FILE', 'filename': filename})
                resp = receive_json(sock)
                if resp['status'] == 'OK':
                    if log_callback: log_callback("File deleted successfully.")
                    return True
                else:
                    if log_callback: log_callback(f"Deletion failed: {resp.get('message')}")
                    return False
        except Exception as e:
            if log_callback: log_callback(f"Deletion error: {e}")
            return False


class DFSGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("DFS Task Manager")
        self.root.geometry("1100x750")
        
        # --- Modern Light Theme Setup ---
        self.colors = {
            'bg': '#F0F2F5',      # Light Grey background
            'fg': '#333333',      # Dark text
            'panel': '#FFFFFF',   # White panels
            'accent': '#0078D4',  # Fluent Blue
            'success': '#107C10', # Green
            'error': '#D13438',   # Red
            'warning': '#D83B01', # Orange
            'select': '#E1DFDD'   # Selection color
        }
        
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure common styles
        style.configure(".", 
            background=self.colors['bg'], 
            foreground=self.colors['fg'], 
            font=("Segoe UI", 10),
            borderwidth=0
        )
        style.configure("TFrame", background=self.colors['bg'])
        style.configure("TLabelframe", background=self.colors['bg'], foreground=self.colors['fg'], borderwidth=1, relief="flat")
        style.configure("TLabelframe.Label", background=self.colors['bg'], foreground=self.colors['accent'], font=("Segoe UI", 11, "bold"))
        
        # Button Styling
        style.configure("TButton", 
            background=self.colors['panel'], 
            foreground=self.colors['fg'], 
            borderwidth=1, 
            focuscolor=self.colors['accent'],
            padding=(12, 6),
            font=("Segoe UI", 10, "bold")
        )
        style.map("TButton", 
            background=[('active', self.colors['accent']), ('pressed', self.colors['accent'])],
            foreground=[('active', 'white')]
        )
        
        # Notebook (Tabs)
        style.configure("TNotebook", background=self.colors['bg'], borderwidth=0)
        style.configure("TNotebook.Tab", 
            background=self.colors['select'], 
            foreground=self.colors['fg'], 
            padding=(20, 10),
            font=("Segoe UI", 10)
        )
        style.map("TNotebook.Tab", 
            background=[('selected', self.colors['panel'])],
            foreground=[('selected', self.colors['accent'])]
        )
        
        # Treeview (Lists)
        style.configure("Treeview", 
            background=self.colors['panel'],
            foreground=self.colors['fg'], 
            fieldbackground=self.colors['panel'],
            borderwidth=0,
            rowheight=30,
            font=("Segoe UI", 10)
        )
        style.configure("Treeview.Heading", 
            background=self.colors['select'], 
            foreground=self.colors['fg'],
            font=("Segoe UI", 10, "bold"),
            padding=(5, 5)
        )
        style.map("Treeview", 
            background=[('selected', self.colors['accent'])],
            foreground=[('selected', 'white')]
        )
        
        # -------------------------------
        
        self.root.configure(bg=self.colors['bg'])
        
        self.client = DFSClient()
        self.log_queue = queue.Queue()
        self.local_cwd = os.path.expanduser("~")
        
        # Change default to Downloads if available
        downloads = os.path.join(self.local_cwd, "Downloads")
        if os.path.isdir(downloads):
            self.local_cwd = downloads
        
        self.setup_ui()
        self.start_monitoring()
        self.process_logs()

    def setup_ui(self):
        # Header Status
        top_bar = ttk.Frame(self.root)
        top_bar.pack(fill="x", padx=20, pady=15)
        
        ttk.Label(top_bar, text="Distributed File System", font=("Segoe UI", 18, "bold"), foreground=self.colors['accent']).pack(side="left")
        
        self.status_var = tk.StringVar(value="Connecting...")
        self.status_lbl = ttk.Label(top_bar, textvariable=self.status_var, foreground=self.colors['warning'], font=("Segoe UI", 10, "bold"))
        self.status_lbl.pack(side="right")

        # Tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Tab 1: Performance (Nodes)
        self.perf_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.perf_frame, text="Performance")
        self.setup_performance_tab()
        
        # Tab 2: File Explorer
        self.files_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.files_frame, text="File Explorer")
        self.setup_files_tab()
        
        # Bottom: Logs
        log_frame = ttk.LabelFrame(self.root, text="System Events")
        log_frame.pack(fill="x", padx=10, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=6, state='disabled', font=("Consolas", 9),
                                                  bg=self.colors['panel'], fg=self.colors['fg'], insertbackground='white')
        self.log_text.pack(fill="x", padx=5, pady=5)

    def setup_performance_tab(self):
        # Top Control Bar
        perf_controls = ttk.Frame(self.perf_frame)
        perf_controls.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(perf_controls, text="Storage Nodes Status", font=("Segoe UI", 12)).pack(side="left")
        ttk.Button(perf_controls, text="+ Add Node", command=self.add_dynamic_node).pack(side="right")

        # Node Grid
        columns = ("ID", "Status", "Port", "CPU", "RAM", "Disk")
        self.node_tree = ttk.Treeview(self.perf_frame, columns=columns, show="headings")
        
        for col in columns:
            self.node_tree.heading(col, text=col)
            self.node_tree.column(col, width=100, anchor="center")
        
        # Configure Tags for Colors
        self.node_tree.tag_configure('online', foreground=self.colors['success'])
        self.node_tree.tag_configure('offline', foreground=self.colors['error'])
            
        self.node_tree.pack(fill="both", expand=True, padx=10, pady=10)

    # ... (add_dynamic_node omitted, it is unchanged) ...

    def update_monitor_ui(self, nodes_data):
        for item in self.node_tree.get_children():
            self.node_tree.delete(item)
        for nid, info in nodes_data.items():
            stats = info.get('stats', {})
            status = info['status']
            
            # Use tags based on status
            tag = 'online' if status == 'ONLINE' else 'offline'
            
            self.node_tree.insert("", "end", values=(
                nid, status, info['address'][1],
                f"{stats.get('cpu', 0)}%",
                f"{stats.get('ram_percent', 0)}%",
                f"{stats.get('disk_percent', 0)}%"
            ), tags=(tag,))

    def add_dynamic_node(self):
        # Determine next available ID/Port
        # Only works for local simulation
        try:
            current_nodes = self.node_tree.get_children()
            count = len(current_nodes)
            
            # Simple heuristic: node_N+1
            next_idx = count + 1
            node_id = f"node_{next_idx}"
            port = NODE_PORTS_START + next_idx # e.g. 6000 + 4
            
            # Check if port is taken? 
            # Ideally we check used ports, but for demo simple increment is okay
            # unless previous nodes were killed.
            # Better: find max port currently in use
            used_ports = []
            max_id_num = 0
            for item in current_nodes:
                vals = self.node_tree.item(item)['values']
                # vals = (id, status, port, ...)
                p = int(vals[2])
                used_ports.append(p)
                
                nid = vals[0]
                if nid.startswith("node_"):
                    try:
                        n = int(nid.split("_")[1])
                        if n > max_id_num: max_id_num = n
                    except: pass
            
            next_idx = max_id_num + 1
            node_id = f"node_{next_idx}"
            port = NODE_PORTS_START + next_idx 
            # Ensure port uniqueness simplisticly
            while port in used_ports:
                port += 1

            # Launch
            CREATE_NO_WINDOW = 0x08000000
            log_file = open(f"logs/{node_id}.log", "w")
            
            cmd = []
            if getattr(sys, 'frozen', False):
                # We are running as exe
                cmd = [sys.executable, "node", node_id, str(port)]
            else:
                # We are running as script, call main.py node ...
                cmd = [sys.executable, "main.py", "node", node_id, str(port)]
                
            subprocess.Popen(
                cmd,
                creationflags=CREATE_NO_WINDOW,
                stdout=log_file,
                stderr=log_file
            )
            self.log(f"Launched new node: {node_id} on port {port}")
            messagebox.showinfo("Success", f"Started {node_id} on port {port}. It will appear shortly.")
            
        except Exception as e:
             self.log(f"Failed to add node: {e}")
             messagebox.showerror("Error", str(e))


    def setup_files_tab(self):
        paned = ttk.PanedWindow(self.files_frame, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Left: Local Files
        left_frame = ttk.LabelFrame(paned, text="Local System")
        paned.add(left_frame, weight=1)
        
        # Navigation Bar
        nav_frame = ttk.Frame(left_frame)
        nav_frame.pack(fill="x", padx=5, pady=5)
        
        self.path_var = tk.StringVar(value=self.local_cwd)
        
        # Helper to find common paths including OneDrive
        user_home = os.path.expanduser("~")
        onedrive = os.path.join(user_home, "OneDrive")
        
        potential_paths = [user_home]
        
        for name in ["Downloads", "Documents", "Desktop"]:
            # Standard path
            p1 = os.path.join(user_home, name)
            if os.path.isdir(p1): potential_paths.append(p1)
            
            # OneDrive path (common on Windows)
            if os.path.isdir(onedrive):
                p2 = os.path.join(onedrive, name)
                if os.path.isdir(p2) and p2 not in potential_paths:
                    potential_paths.append(p2)

        self.dir_combo = ttk.Combobox(nav_frame, textvariable=self.path_var)
        self.dir_combo['values'] = potential_paths
        self.dir_combo.bind('<<ComboboxSelected>>', self.change_dir)
        self.dir_combo.bind('<Return>', self.change_dir)
        self.dir_combo.pack(side="left", fill="x", expand=True)
        
        ttk.Button(nav_frame, text="Browse", command=self.browse_dir).pack(side="right", padx=(5,0))

        self.local_tree = ttk.Treeview(left_frame, columns=("Size",), show="tree headings")
        self.local_tree.heading("#0", text="Filename")
        self.local_tree.heading("Size", text="Size")
        self.local_tree.pack(fill="both", expand=True, padx=5, pady=5)
        
        local_btn_frame = ttk.Frame(left_frame)
        local_btn_frame.pack(fill="x", padx=5, pady=5)
        ttk.Button(local_btn_frame, text="Refresh", command=self.refresh_local_files).pack(side="left")
        ttk.Button(local_btn_frame, text="Upload ->", command=self.upload_selected).pack(side="right")
        
        # Right: DFS Files
        right_frame = ttk.LabelFrame(paned, text="Distributed Storage")
        paned.add(right_frame, weight=1)
        
        self.dfs_tree = ttk.Treeview(right_frame, columns=("Filename", "Size", "Status"), show="headings")
        self.dfs_tree.heading("Filename", text="Filename")
        self.dfs_tree.heading("Size", text="Size")
        self.dfs_tree.heading("Status", text="Replication")
        self.dfs_tree.pack(fill="both", expand=True, padx=5, pady=5)
        
        dfs_btn_frame = ttk.Frame(right_frame)
        dfs_btn_frame.pack(fill="x", padx=5, pady=5)
        ttk.Button(dfs_btn_frame, text="Download", command=self.download_selected).pack(side="left")
        ttk.Button(dfs_btn_frame, text="Delete", command=self.delete_selected).pack(side="left", padx=5)
        ttk.Button(dfs_btn_frame, text="Refresh DFS", command=self.refresh_dfs_files).pack(side="right")
        
        self.refresh_local_files()

    def browse_dir(self):
        d = filedialog.askdirectory(initialdir=self.local_cwd)
        if d:
            self.local_cwd = d
            self.path_var.set(d)
            self.refresh_local_files()

    def change_dir(self, event=None):
        path = self.path_var.get()
        if os.path.isdir(path):
            self.local_cwd = path
            self.refresh_local_files()
        else:
            self.log(f"Invalid directory: {path}")

    def refresh_local_files(self):
        for i in self.local_tree.get_children():
            self.local_tree.delete(i)
            
        try:
            for item in os.listdir(self.local_cwd):
                path = os.path.join(self.local_cwd, item)
                if os.path.isfile(path):
                    size = f"{os.path.getsize(path)/1024:.1f} KB"
                    self.local_tree.insert("", "end", text=item, values=(size,))
        except Exception as e:
            self.log(f"Error reading local files: {e}")

    def refresh_dfs_files(self):
        threading.Thread(target=self._refresh_dfs_thread, daemon=True).start()

    def _refresh_dfs_thread(self):
        resp = self.client.list_files()
        if resp and 'files' in resp:
            self.root.after(0, self.update_dfs_list, resp['files'])

    def update_dfs_list(self, files):
        for item in self.dfs_tree.get_children():
            self.dfs_tree.delete(item)
        for f in files:
            self.dfs_tree.insert("", "end", values=(f['filename'], f['size'], f['status']))

    # ... Continuing logic ...
    # I realized I made a mistake in the previous setup_files_tab snippet above. 
    # I need to ensure DFS tree has Filename column.

    def upload_selected(self):
        sel = self.local_tree.selection()
        if not sel: return
        filename = self.local_tree.item(sel[0])['text']
        filepath = os.path.join(self.local_cwd, filename) # Correctly join with current nav dir
        threading.Thread(target=self.client.upload_file, args=(filepath, self.log), daemon=True).start()

    def download_selected(self):
        sel = self.dfs_tree.selection()
        if not sel: return
        filename = self.dfs_tree.item(sel[0])['values'][0] # Assuming Col 0 is Filename now
        save_path = filedialog.asksaveasfilename(initialfile=filename)
        if save_path:
             threading.Thread(target=self.client.download_file, args=(filename, save_path, self.log), daemon=True).start()

    def delete_selected(self):
        sel = self.dfs_tree.selection()
        if not sel: return
        filename = self.dfs_tree.item(sel[0])['values'][0]
        if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete '{filename}'?"):
            def _delete_task():
                if self.client.delete_file(filename, self.log):
                    self.refresh_dfs_files()
            threading.Thread(target=_delete_task, daemon=True).start()

    def log(self, msg):
        self.log_queue.put(msg)

    def process_logs(self):
        while not self.log_queue.empty():
            msg = self.log_queue.get()
            self.log_text.config(state='normal')
            self.log_text.insert('end', f"[{time.strftime('%H:%M:%S')}] {msg}\n")
            self.log_text.see('end')
            self.log_text.config(state='disabled')
        self.root.after(100, self.process_logs)

    def start_monitoring(self):
        threading.Thread(target=self.poll_stats, daemon=True).start()

    def poll_stats(self):
        while True:
            try:
                resp = self.client.get_stats()
                if resp and 'nodes' in resp:
                    self.root.after(0, self.update_monitor_ui, resp['nodes'])
                    self.root.after(0, lambda: self.status_var.set("CONNECTED"))
                    self.root.after(0, lambda: self.status_lbl.config(foreground=self.colors['success']))
                else:
                    self.root.after(0, lambda: self.status_var.set("DISCONNECTED"))
                    self.root.after(0, lambda: self.status_lbl.config(foreground=self.colors['error']))
            except Exception:
                 self.root.after(0, lambda: self.status_var.set("CONNECTION ERROR"))
                 self.root.after(0, lambda: self.status_lbl.config(foreground=self.colors['error']))
            time.sleep(1)

    def update_monitor_ui(self, nodes_data):
        for item in self.node_tree.get_children():
            self.node_tree.delete(item)
        for nid, info in nodes_data.items():
            stats = info.get('stats', {})
            status = info['status']
            
            tag = 'online' if status == 'ONLINE' else 'offline'
            
            self.node_tree.insert("", "end", values=(
                nid, status, info['address'][1],
                f"{stats.get('cpu', 0)}%",
                f"{stats.get('ram_percent', 0)}%",
                f"{stats.get('disk_percent', 0)}%"
            ), tags=(tag,))

if __name__ == "__main__":
    root = tk.Tk()
    app = DFSGUI(root)
    root.mainloop()
