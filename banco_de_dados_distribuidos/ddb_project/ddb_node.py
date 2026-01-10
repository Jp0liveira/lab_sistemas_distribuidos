#!/usr/bin/env python3
"""
Distributed Database Node
Middleware para Banco de Dados Distribuído com MySQL
VERSÃO CORRIGIDA - Heartbeat com IPs corretos
"""

import socket
import threading
import json
import hashlib
import time
import mysql.connector
from datetime import datetime
from enum import Enum
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DDB-Node')


class MessageType(Enum):
    HEARTBEAT = "HEARTBEAT"
    QUERY = "QUERY"
    REPLICATE = "REPLICATE"
    ELECTION = "ELECTION"
    COORDINATOR = "COORDINATOR"
    ACK = "ACK"
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    NODE_INFO = "NODE_INFO"


class DDBNode:

    def __init__(self, node_id, host, port, db_config):
        self.node_id = node_id
        self.host = host  # 0.0.0.0 para bind
        self.port = port
        self.db_config = db_config

        # IP real para comunicação entre nós (dentro da rede Docker)
        self.actual_host = '172.20.0.' + str(10 + self.node_id)

        self.is_coordinator = False
        self.coordinator_id = None
        self.nodes = {}
        # Registra si mesmo com IP real
        self.nodes[node_id] = (self.actual_host, 5000, time.time())

        self.db_connection = None
        self.connect_db()

        self.server_socket = None
        self.running = False

        self.queries_processed = 0
        self.queries_log = []

        self.nodes_lock = threading.Lock()
        self.db_lock = threading.Lock()

    def connect_db(self):
        """Conecta ao MySQL local com retry"""
        max_retries = 10
        retry_delay = 3

        for attempt in range(max_retries):
            try:
                self.db_connection = mysql.connector.connect(**self.db_config)
                logger.info(f"Conectado ao MySQL: {self.db_config['database']}")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Tentativa {attempt + 1}/{max_retries} falhou. Aguardando {retry_delay}s... Erro: {e}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Erro ao conectar ao MySQL após {max_retries} tentativas: {e}")
                    raise

    def calculate_checksum(self, data):
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def verify_checksum(self, message):
        received_checksum = message.get('checksum')
        message_copy = message.copy()
        message_copy.pop('checksum', None)
        calculated = self.calculate_checksum(message_copy)
        return received_checksum == calculated

    def create_message(self, msg_type, data):
        message = {
            'type': msg_type.value,
            'node_id': self.node_id,
            'timestamp': time.time(),
            'data': data
        }
        message['checksum'] = self.calculate_checksum(message)
        return message

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)

        logger.info(f"Nó {self.node_id} iniciado em {self.host}:{self.port} (IP real: {self.actual_host})")

        threading.Thread(target=self.heartbeat_sender, daemon=True).start()
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()

        def delayed_election():
            time.sleep(10)
            self.start_election()

        threading.Thread(target=delayed_election, daemon=True).start()

        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    logger.error(f"Erro ao aceitar conexão: {e}")

    def handle_client(self, client_socket, address):
        try:
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break

            if not data:
                return

            message = json.loads(data.decode())

            if not self.verify_checksum(message):
                logger.warning("Checksum inválido")
                response = self.create_message(MessageType.ACK, {
                    'status': 'error',
                    'message': 'Checksum inválido'
                })
                client_socket.send(json.dumps(response).encode())
                return

            response = self.process_message(message)
            client_socket.send(json.dumps(response).encode())

        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
        finally:
            client_socket.close()

    def process_message(self, message):
        msg_type = MessageType(message['type'])
        sender_id = message['node_id']
        data = message['data']

        logger.info(f"Recebido {msg_type.value} de {sender_id}")

        if msg_type == MessageType.HEARTBEAT:
            return self.handle_heartbeat(sender_id, data)
        elif msg_type == MessageType.QUERY:
            return self.handle_query(data)
        elif msg_type == MessageType.REPLICATE:
            return self.handle_replicate(data)
        elif msg_type == MessageType.ELECTION:
            return self.handle_election(sender_id)
        elif msg_type == MessageType.COORDINATOR:
            return self.handle_coordinator_announcement(sender_id)
        elif msg_type == MessageType.NODE_INFO:
            return self.handle_node_info(sender_id, data)

        return self.create_message(MessageType.ACK, {'status': 'unknown_type'})

    def handle_heartbeat(self, sender_id, data):
        with self.nodes_lock:
            if sender_id not in self.nodes:
                self.nodes[sender_id] = (data['host'], data['port'], time.time())
                logger.info(f"Novo nó descoberto via HEARTBEAT: {sender_id}")
            else:
                host, port, _ = self.nodes[sender_id]
                self.nodes[sender_id] = (host, port, time.time())

        return self.create_message(MessageType.ACK, {'status': 'ok'})

    def handle_query(self, data):
        query = data['query']
        query_type = data.get('type', 'READ')

        logger.info(f"Executando query [{query_type}]: {query[:50]}...")

        try:
            with self.db_lock:
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(query)

                if query_type == 'WRITE':
                    self.db_connection.commit()
                    result = {'affected_rows': cursor.rowcount}
                    self.broadcast_replicate(query)
                else:
                    rows = cursor.fetchall()
                    for row in rows:
                        for key, value in row.items():
                            if isinstance(value, datetime):
                                row[key] = value.isoformat()
                    result = {'rows': rows}

                cursor.close()

                self.queries_processed += 1
                self.queries_log.append({
                    'timestamp': datetime.now().isoformat(),
                    'query': query,
                    'type': query_type,
                    'node': self.node_id
                })

                return self.create_message(MessageType.ACK, {
                    'status': 'success',
                    'result': result,
                    'node_id': self.node_id
                })

        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            return self.create_message(MessageType.ACK, {
                'status': 'error',
                'message': str(e),
                'node_id': self.node_id
            })

    def handle_replicate(self, data):
        query = data['query']
        logger.info(f"Replicando query: {query[:50]}...")

        try:
            with self.db_lock:
                cursor = self.db_connection.cursor()
                cursor.execute(query)
                self.db_connection.commit()
                cursor.close()

            logger.info("Replicação concluída com sucesso!")
            return self.create_message(MessageType.ACK, {'status': 'replicated'})

        except Exception as e:
            logger.error(f"Erro na replicação: {e}")
            return self.create_message(MessageType.ACK, {
                'status': 'error',
                'message': str(e)
            })

    def broadcast_replicate(self, query):
        logger.info(f"Iniciando broadcast de replicação para query: {query[:50]}...")

        message = self.create_message(MessageType.REPLICATE, {'query': query})

        with self.nodes_lock:
            nodes_copy = list(self.nodes.items())

        logger.info(f"Enviando replicação para {len(nodes_copy)-1} nós")

        for node_id, (host, port, _) in nodes_copy:
            if node_id == self.node_id:
                continue

            logger.info(f"Enviando replicação para nó {node_id} ({host}:{port})")

            threading.Thread(
                target=self.send_message,
                args=(host, port, message),
                daemon=True
            ).start()

    def send_message(self, host, port, message):
        try:
            logger.info(f"Conectando em {host}:{port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.send(json.dumps(message).encode())

            response = sock.recv(4096)
            sock.close()

            result = json.loads(response.decode())
            logger.info(f"Resposta de {host}:{port}: {result['data']['status']}")

            return result

        except Exception as e:
            logger.error(f"Erro ao enviar para {host}:{port} - {e}")
            return None

    def heartbeat_sender(self):
        while self.running:
            message = self.create_message(MessageType.HEARTBEAT, {
                'host': self.actual_host,  # USA IP REAL!
                'port': 5000
            })

            with self.nodes_lock:
                nodes_copy = list(self.nodes.items())

            for node_id, (host, port, _) in nodes_copy:
                if node_id == self.node_id:
                    continue

                threading.Thread(
                    target=self.send_message,
                    args=(host, port, message),
                    daemon=True
                ).start()

            time.sleep(5)

    def heartbeat_monitor(self):
        """Monitora nós inativos"""
        while self.running:
            time.sleep(10)
            current_time = time.time()

            dead_nodes = []
            coordinator_failed = False

            with self.nodes_lock:
                for node_id, (host, port, last_seen) in self.nodes.items():
                    if node_id == self.node_id:
                        continue

                    if current_time - last_seen > 15:
                        dead_nodes.append(node_id)
                        if node_id == self.coordinator_id:
                            coordinator_failed = True

            # FORA DO LOCK - remove nós mortos
            if dead_nodes:
                with self.nodes_lock:
                    for node_id in dead_nodes:
                        if node_id in self.nodes:
                            logger.warning(f"Nó {node_id} está inativo - removendo")
                            del self.nodes[node_id]

            if coordinator_failed:
                logger.warning("Coordenador falhou - iniciando eleição")
                self.start_election()

    def start_election(self):
        """Inicia processo de eleição (Algoritmo Bully)"""
        try:
            logger.info(f"=== INICIANDO ELEIÇÃO === Nó {self.node_id}")

            # Pega lista de nós uma vez só
            with self.nodes_lock:
                higher_nodes = [nid for nid in self.nodes.keys() if nid > self.node_id]
                nodes_copy = dict(self.nodes)  # Copia para não precisar de lock depois

            logger.info(f"Nós com ID maior que {self.node_id}: {higher_nodes}")

            if not higher_nodes:
                logger.info(f"Nenhum nó maior - Nó {self.node_id} se tornará coordenador")
                self.become_coordinator()
                return

            logger.info(f"Enviando ELECTION para {len(higher_nodes)} nós")
            message = self.create_message(MessageType.ELECTION, {})
            responses = 0

            for node_id in higher_nodes:
                if node_id not in nodes_copy:
                    continue

                host, port, _ = nodes_copy[node_id]
                logger.info(f"Enviando ELECTION para nó {node_id}")
                response = self.send_message(host, port, message)

                if response:
                    responses += 1
                    logger.info(f"✓ Nó {node_id} respondeu")
                else:
                    logger.warning(f"✗ Nó {node_id} NÃO respondeu")

            logger.info(f"Total de respostas: {responses}/{len(higher_nodes)}")

            if responses == 0:
                logger.info(f"Nenhuma resposta - Nó {self.node_id} se tornará coordenador")
                self.become_coordinator()
            else:
                logger.info(f"Aguardando coordenador...")

        except Exception as e:
            logger.error(f"ERRO na eleição: {e}", exc_info=True)
            self.become_coordinator()

    def become_coordinator(self):
        self.is_coordinator = True
        self.coordinator_id = self.node_id
        logger.info(f"Nó {self.node_id} é o novo COORDENADOR")

        message = self.create_message(MessageType.COORDINATOR, {})

        with self.nodes_lock:
            nodes_copy = list(self.nodes.items())

        for node_id, (host, port, _) in nodes_copy:
            if node_id == self.node_id:
                continue
            self.send_message(host, port, message)

    def handle_election(self, sender_id):
        if sender_id < self.node_id:
            threading.Thread(target=self.start_election, daemon=True).start()

        return self.create_message(MessageType.ACK, {'status': 'ok'})

    def handle_coordinator_announcement(self, sender_id):
        self.coordinator_id = sender_id
        self.is_coordinator = False
        logger.info(f"Novo coordenador: {sender_id}")

        return self.create_message(MessageType.ACK, {'status': 'ok'})

    def handle_node_info(self, sender_id, data):
        with self.nodes_lock:
            if sender_id not in self.nodes:
                self.nodes[sender_id] = (data['host'], data['port'], time.time())
                logger.info(f"Novo nó registrado via NODE_INFO: {sender_id}")
            else:
                self.nodes[sender_id] = (data['host'], data['port'], time.time())
                logger.info(f"Nó {sender_id} atualizado via NODE_INFO")

        return self.create_message(MessageType.ACK, {
            'status': 'ok',
            'nodes': {
                str(nid): {'host': h, 'port': p}
                for nid, (h, p, _) in self.nodes.items()
            }
        })

    def register_with_node(self, known_host, known_port):
        logger.info(f"Registrando com nó em {known_host}:{known_port}...")

        message = self.create_message(MessageType.NODE_INFO, {
            'host': self.actual_host,  # USA IP REAL!
            'port': 5000
        })

        response = self.send_message(known_host, known_port, message)

        if response and response['data'].get('nodes'):
            with self.nodes_lock:
                for node_id_str, info in response['data']['nodes'].items():
                    node_id = int(node_id_str)
                    if node_id not in self.nodes:
                        self.nodes[node_id] = (info['host'], info['port'], time.time())
                        logger.info(f"Descoberto nó {node_id} via NODE_INFO")

            logger.info(f"Registro completo! Total de {len(self.nodes)} nós conhecidos")
        else:
            logger.warning("Falha ao registrar")

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        if self.db_connection:
            self.db_connection.close()
        logger.info("Nó finalizado")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Uso: python ddb_node.py <node_id> <port> <db_name> [known_host:known_port]")
        sys.exit(1)

    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    db_name = sys.argv[3]

    mysql_host = os.environ.get('DB_HOST', f'mysql{node_id}')

    db_config = {
        'host': mysql_host,
        'user': 'ddb_user',
        'password': 'ddb_password',
        'database': db_name
    }

    node = DDBNode(node_id, '0.0.0.0', port, db_config)

    if len(sys.argv) >= 5:
        known = sys.argv[4].split(':')
        node.register_with_node(known[0], int(known[1]))

    try:
        node.start()
    except KeyboardInterrupt:
        logger.info("Encerrando...")
        node.stop()