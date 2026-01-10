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


# Teste 1: SELECT simples
print("=" * 60)
print("TESTE 1: SELECT * FROM users")
print("=" * 60)
result = send_query('localhost', 5001, 'SELECT * FROM users;', 'READ')

if result['data']['status'] == 'success':
    print(f"✓ Sucesso no nó {result['data']['node_id']}")
    rows = result['data']['result']['rows']
    print(f"\nEncontrados {len(rows)} usuários:\n")
    for row in rows:
        print(f"  ID: {row['id']} | Nome: {row['name']} | Email: {row['email']}")
else:
    print(f"✗ Erro: {result['data']['message']}")

# Teste 2: INSERT novo usuário (com timestamp único)
print("\n" + "=" * 60)
print("TESTE 2: INSERT novo usuário")
print("=" * 60)

timestamp = int(time.time())
email = f"usuario_{timestamp}@test.com"
query = f"INSERT INTO users (name, email) VALUES ('Usuario Teste {timestamp}', '{email}');"

result = send_query('localhost', 5001, query, 'WRITE')

if result['data']['status'] == 'success':
    print(f"✓ INSERT executado no nó {result['data']['node_id']}")
    print(f"  Linhas afetadas: {result['data']['result']['affected_rows']}")

    # Aguarda replicação
    print("\nAguardando replicação (2s)...")
    time.sleep(2)

    # Verifica nos outros nós
    print("\nVerificando replicação nos outros nós:")
    for port in [5002, 5003]:
        check_query = f"SELECT * FROM users WHERE email = '{email}';"
        result = send_query('localhost', port, check_query, 'READ')

        if result['data']['status'] == 'success':
            rows = result['data']['result']['rows']
            if rows:
                print(f"  ✓ Nó na porta {port}: Dado replicado!")
            else:
                print(f"  ✗ Nó na porta {port}: Dado NÃO encontrado")
else:
    print(f"✗ Erro: {result['data']['message']}")

# Teste 3: SELECT COUNT
print("\n" + "=" * 60)
print("TESTE 3: COUNT total de usuários")
print("=" * 60)

result = send_query('localhost', 5002, 'SELECT COUNT(*) as total FROM users;', 'READ')

if result['data']['status'] == 'success':
    total = result['data']['result']['rows'][0]['total']
    print(f"✓ Total de usuários: {total}")
else:
    print(f"✗ Erro: {result['data']['message']}")

print("\n" + "=" * 60)
print("TESTES CONCLUÍDOS!")
print("=" * 60)