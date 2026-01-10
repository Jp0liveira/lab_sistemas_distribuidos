import socket
import json
import hashlib
import time


def send_query(host, port, query, query_type='READ'):
    message = {
        'type': 'QUERY',
        'node_id': 'test_client',
        'timestamp': time.time(),
        'data': {'query': query, 'type': query_type}
    }
    checksum_data = message.copy()
    message['checksum'] = hashlib.md5(
        json.dumps(checksum_data, sort_keys=True).encode()
    ).hexdigest()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(json.dumps(message).encode())

    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk or len(chunk) < 4096:
            data += chunk
            break
        data += chunk

    sock.close()
    return json.loads(data.decode())


# Teste SELECT
result = send_query('localhost', 5001, 'SELECT * FROM users;', 'READ')
print(json.dumps(result, indent=2))

# Teste INSERT
result = send_query('localhost', 5001,
                    "INSERT INTO users (name, email) VALUES ('Docker Test', 'docker@test.com');",
                    'WRITE')
print(json.dumps(result, indent=2))