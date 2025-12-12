import json
import hashlib
import struct
import socket

def send_json(sock, data):
    """
    Send a JSON object over a socket with length prefix.
    """
    json_str = json.dumps(data)
    data_bytes = json_str.encode('utf-8')
    # Send length of the message first (4 bytes, big endian)
    sock.sendall(struct.pack('>I', len(data_bytes)))
    sock.sendall(data_bytes)

def receive_json(sock):
    """
    Receive a JSON object from a socket.
    """
    # Read length prefix
    len_bytes = recv_all(sock, 4)
    if not len_bytes:
        return None
    msg_len = struct.unpack('>I', len_bytes)[0]
    # Read message body
    msg_bytes = recv_all(sock, msg_len)
    if not msg_bytes:
        return None
    return json.loads(msg_bytes.decode('utf-8'))

def recv_all(sock, n):
    """
    Helper to receive exactly n bytes.
    """
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def calculate_checksum(data):
    """
    Calculate SHA-256 checksum of bytes data.
    """
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()

def calculate_file_checksum(filepath):
    """
    Calculate SHA-256 checksum of a file.
    """
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    return sha256.hexdigest()
