#!/usr/bin/env python3
"""
Nó do Banco de Dados Distribuído (DDB) - VERSÃO SQLITE
Sistema baseado em sockets TCP com SQLite backend
VANTAGEM: Não precisa de MySQL instalado ou permissões de admin
"""

import socket
import threading
import json
import time
import sqlite3
import hashlib
import sys
import os
from datetime import datetime

class DDBNodeSQLite:
    def __init__(self, node_id, port, db_path, peers=None):
        """
        Inicializa um nó do DDB com SQLite
        
        Args:
            node_id: ID único do nó (inteiro)
            port: Porta para escutar conexões
            db_path: Caminho para o arquivo SQLite
            peers: Lista de tuplas (node_id, host, port) dos outros nós
        """
        self.node_id = node_id
        self.port = port
        self.host = '0.0.0.0'
        self.db_path = db_path
        self.peers = peers if peers else []
        
        # Estado do coordenador
        self.is_coordinator = False
        self.coordinator_id = None
        
        # Socket servidor
        self.server_socket = None
        self.running = False
        
        # Lock para operações críticas
        self.lock = threading.Lock()
        
        # Estatísticas
        self.queries_processed = 0
        self.last_heartbeat = time.time()
        
        # Inicializa banco SQLite
        self.init_database()
        
    def init_database(self):
        """Inicializa o banco SQLite e cria tabelas"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Cria tabela de exemplo
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL,
                    email TEXT UNIQUE,
                    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insere dados iniciais apenas no nó 1
            if self.node_id == 1:
                cursor.execute('''
                    INSERT OR IGNORE INTO usuarios (id, nome, email) VALUES 
                    (1, 'Admin Sistema', 'admin@ddb.local'),
                    (2, 'Usuario Teste', 'teste@ddb.local')
                ''')
            
            conn.commit()
            conn.close()
            
            print(f"[Nó {self.node_id}] Banco SQLite inicializado: {self.db_path}")
            
        except Exception as e:
            print(f"[Nó {self.node_id}] ERRO ao inicializar SQLite: {e}")
            sys.exit(1)
    
    def get_connection(self):
        """Retorna uma nova conexão SQLite (thread-safe)"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Para retornar dicionários
        return conn
    
    def calculate_checksum(self, data):
        """Calcula checksum MD5 dos dados"""
        return hashlib.md5(data.encode('utf-8')).hexdigest()
    
    def verify_checksum(self, data, checksum):
        """Verifica integridade dos dados"""
        return self.calculate_checksum(data) == checksum
    
    def start(self):
        """Inicia o servidor do nó"""
        self.running = True
        
        # Cria socket servidor
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print(f"\n{'='*60}")
        print(f"[Nó {self.node_id}] DDB Node SQLite iniciado na porta {self.port}")
        print(f"[Nó {self.node_id}] Database: {self.db_path}")
        print(f"{'='*60}\n")
        
        # Thread para aceitar conexões
        accept_thread = threading.Thread(target=self.accept_connections, daemon=True)
        accept_thread.start()
        
        # Thread para heartbeat
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Inicia eleição de coordenador
        time.sleep(2)  # Aguarda outros nós iniciarem
        self.start_election()
        
        # Mantém programa rodando
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[Nó {self.node_id}] Encerrando...")
            self.stop()
    
    def accept_connections(self):
        """Loop para aceitar conexões de clientes e outros nós"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                print(f"[Nó {self.node_id}] Conexão recebida de {address}")
                
                # Thread para processar requisição
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
            except Exception as e:
                if self.running:
                    print(f"[Nó {self.node_id}] Erro ao aceitar conexão: {e}")
    
    def handle_client(self, client_socket, address):
        """Processa requisição de um cliente ou outro nó"""
        try:
            # Recebe dados
            data = client_socket.recv(8192).decode('utf-8')
            
            if not data:
                return
            
            # Parse da mensagem JSON
            message = json.loads(data)
            msg_type = message.get('type')
            
            print(f"\n[Nó {self.node_id}] Mensagem recebida: {msg_type}")
            
            # Processa diferentes tipos de mensagem
            if msg_type == 'QUERY':
                response = self.handle_query(message)
            elif msg_type == 'REPLICATE':
                response = self.handle_replication(message)
            elif msg_type == 'ELECTION':
                response = self.handle_election(message)
            elif msg_type == 'COORDINATOR':
                response = self.handle_coordinator_announcement(message)
            elif msg_type == 'HEARTBEAT':
                response = self.handle_heartbeat(message)
            elif msg_type == 'STATUS':
                response = self.get_status()
            else:
                response = {'status': 'error', 'message': 'Tipo de mensagem desconhecido'}
            
            # Envia resposta
            response_data = json.dumps(response)
            client_socket.sendall(response_data.encode('utf-8'))
            
        except json.JSONDecodeError as e:
            print(f"[Nó {self.node_id}] Erro ao decodificar JSON: {e}")
            error_response = json.dumps({'status': 'error', 'message': 'JSON inválido'})
            client_socket.sendall(error_response.encode('utf-8'))
        except Exception as e:
            print(f"[Nó {self.node_id}] Erro ao processar requisição: {e}")
            error_response = json.dumps({'status': 'error', 'message': str(e)})
            client_socket.sendall(error_response.encode('utf-8'))
        finally:
            client_socket.close()
    
    def handle_query(self, message):
        """Processa uma query SQL (garantindo ACID)"""
        query = message.get('query', '')
        checksum = message.get('checksum', '')
        
        # Verifica integridade
        if not self.verify_checksum(query, checksum):
            return {
                'status': 'error',
                'message': 'Checksum inválido - dados corrompidos'
            }
        
        print(f"[Nó {self.node_id}] Executando query: {query[:100]}...")
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Inicia transação (ACID)
            conn.execute("BEGIN TRANSACTION")
            
            # Executa query
            cursor.execute(query)
            
            # Verifica se é query de modificação
            is_modification = query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE'))
            
            if is_modification:
                # Commit da transação local
                conn.commit()
                rows_affected = cursor.rowcount
                
                # Replica para outros nós (broadcast)
                replication_result = self.replicate_to_peers(query)
                
                result = {
                    'status': 'success',
                    'message': f'Query executada com sucesso no nó {self.node_id}',
                    'rows_affected': rows_affected,
                    'node_id': self.node_id,
                    'replicated': replication_result
                }
            else:
                # Query de leitura (SELECT)
                rows = [dict(row) for row in cursor.fetchall()]
                result = {
                    'status': 'success',
                    'data': rows,
                    'row_count': len(rows),
                    'node_id': self.node_id
                }
            
            cursor.close()
            conn.close()
            self.queries_processed += 1
            
            return result
            
        except sqlite3.Error as err:
            # Rollback em caso de erro (ACID)
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            print(f"[Nó {self.node_id}] Erro SQLite: {err}")
            return {
                'status': 'error',
                'message': f'Erro SQLite: {err}',
                'node_id': self.node_id
            }
        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            print(f"[Nó {self.node_id}] Erro: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'node_id': self.node_id
            }
    
    def replicate_to_peers(self, query):
        """Replica uma query para todos os outros nós (broadcast)"""
        print(f"[Nó {self.node_id}] Replicando para {len(self.peers)} peers...")
        
        success_count = 0
        failed_peers = []
        
        for peer_id, peer_host, peer_port in self.peers:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.settimeout(5)
                peer_socket.connect((peer_host, peer_port))
                
                checksum = self.calculate_checksum(query)
                message = {
                    'type': 'REPLICATE',
                    'query': query,
                    'checksum': checksum,
                    'origin_node': self.node_id
                }
                
                peer_socket.sendall(json.dumps(message).encode('utf-8'))
                response = peer_socket.recv(4096).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'success':
                    success_count += 1
                    print(f"[Nó {self.node_id}] Replicação OK para nó {peer_id}")
                else:
                    failed_peers.append(peer_id)
                
                peer_socket.close()
                
            except Exception as e:
                print(f"[Nó {self.node_id}] Erro ao replicar para nó {peer_id}: {e}")
                failed_peers.append(peer_id)
        
        return {
            'success_count': success_count,
            'total_peers': len(self.peers),
            'failed_peers': failed_peers
        }
    
    def handle_replication(self, message):
        """Recebe e executa uma query replicada de outro nó"""
        query = message.get('query', '')
        checksum = message.get('checksum', '')
        origin_node = message.get('origin_node')
        
        print(f"[Nó {self.node_id}] Replicação recebida do nó {origin_node}")
        
        if not self.verify_checksum(query, checksum):
            return {'status': 'error', 'message': 'Checksum inválido na replicação'}
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            conn.execute("BEGIN TRANSACTION")
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"[Nó {self.node_id}] Replicação executada com sucesso")
            
            return {
                'status': 'success',
                'message': 'Replicação executada',
                'node_id': self.node_id
            }
        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            print(f"[Nó {self.node_id}] Erro na replicação: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def start_election(self):
        """Inicia processo de eleição (Algoritmo Bully)"""
        print(f"\n[Nó {self.node_id}] Iniciando eleição...")
        
        higher_nodes = [peer for peer in self.peers if peer[0] > self.node_id]
        
        if not higher_nodes:
            self.become_coordinator()
            return
        
        responses = 0
        for peer_id, peer_host, peer_port in higher_nodes:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.settimeout(2)
                peer_socket.connect((peer_host, peer_port))
                
                message = {'type': 'ELECTION', 'from_node': self.node_id}
                peer_socket.sendall(json.dumps(message).encode('utf-8'))
                response = peer_socket.recv(1024).decode('utf-8')
                
                if response:
                    responses += 1
                
                peer_socket.close()
            except:
                pass
        
        if responses == 0:
            self.become_coordinator()
    
    def handle_election(self, message):
        """Responde a uma mensagem de eleição"""
        from_node = message.get('from_node')
        print(f"[Nó {self.node_id}] Eleição recebida do nó {from_node}")
        
        threading.Thread(target=self.start_election, daemon=True).start()
        return {'status': 'ok', 'node_id': self.node_id}
    
    def become_coordinator(self):
        """Este nó se torna o coordenador"""
        with self.lock:
            self.is_coordinator = True
            self.coordinator_id = self.node_id
        
        print(f"\n{'*'*60}")
        print(f"[Nó {self.node_id}] AGORA SOU O COORDENADOR!")
        print(f"{'*'*60}\n")
        
        self.announce_coordinator()
    
    def announce_coordinator(self):
        """Anuncia que este nó é o novo coordenador"""
        for peer_id, peer_host, peer_port in self.peers:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.settimeout(2)
                peer_socket.connect((peer_host, peer_port))
                
                message = {'type': 'COORDINATOR', 'coordinator_id': self.node_id}
                peer_socket.sendall(json.dumps(message).encode('utf-8'))
                peer_socket.close()
            except:
                pass
    
    def handle_coordinator_announcement(self, message):
        """Recebe anúncio de novo coordenador"""
        coordinator_id = message.get('coordinator_id')
        
        with self.lock:
            self.coordinator_id = coordinator_id
            self.is_coordinator = False
        
        print(f"[Nó {self.node_id}] Novo coordenador: Nó {coordinator_id}")
        return {'status': 'ok'}
    
    def heartbeat_loop(self):
        """Envia heartbeat periodicamente"""
        while self.running:
            time.sleep(5)
            self.last_heartbeat = time.time()
            
            status = "COORDENADOR" if self.is_coordinator else "ATIVO"
            print(f"[Nó {self.node_id}] Heartbeat - Status: {status} - Queries: {self.queries_processed}")
    
    def handle_heartbeat(self, message):
        """Responde a um heartbeat"""
        return {
            'status': 'alive',
            'node_id': self.node_id,
            'is_coordinator': self.is_coordinator,
            'timestamp': time.time()
        }
    
    def get_status(self):
        """Retorna status detalhado do nó"""
        return {
            'node_id': self.node_id,
            'port': self.port,
            'is_coordinator': self.is_coordinator,
            'coordinator_id': self.coordinator_id,
            'queries_processed': self.queries_processed,
            'peers_count': len(self.peers),
            'uptime': time.time() - self.last_heartbeat,
            'database': 'SQLite',
            'db_file': self.db_path
        }
    
    def stop(self):
        """Para o servidor"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


def main():
    """Função principal para iniciar um nó"""
    if len(sys.argv) < 2:
        print("Uso: python ddb_node_sqlite.py <node_id>")
        print("Exemplo: python ddb_node_sqlite.py 1")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    
    # Diretório para os bancos SQLite
    db_dir = os.path.expanduser("~/ddb_databases")
    os.makedirs(db_dir, exist_ok=True)
    
    # Configuração dos nós
    nodes_config = {
        1: {
            'port': 5001,
            'db_path': os.path.join(db_dir, 'ddb_node1.db')
        },
        2: {
            'port': 5002,
            'db_path': os.path.join(db_dir, 'ddb_node2.db')
        },
        3: {
            'port': 5003,
            'db_path': os.path.join(db_dir, 'ddb_node3.db')
        }
    }
    
    if node_id not in nodes_config:
        print(f"Erro: Configuração não encontrada para nó {node_id}")
        sys.exit(1)
    
    config = nodes_config[node_id]
    
    # Lista de peers - CONFIGURADO PARA SUAS MÁQUINAS
    if node_id == 1:
        peers = [
            (2, '10.16.0.109', 5002),   # Máquina 2
            (3, '10.16.0.254', 5003)   # Máquina 3
        ]
    elif node_id == 2:
        peers = [
            (1, '10.16.1.233', 5001),   # Máquina 1
            (3, '110.16.0.254', 5003)   # Máquina 3
        ]
    elif node_id == 3:
        peers = [
            (1, '10.16.1.233', 5001),   # Máquina 1
            (2, '10.16.0.109', 5002)    # Máquina 2
        ]
    else:
        peers = []
    
    # Cria e inicia o nó
    node = DDBNodeSQLite(
        node_id=node_id,
        port=config['port'],
        db_path=config['db_path'],
        peers=peers
    )
    
    node.start()


if __name__ == '__main__':
    main()
