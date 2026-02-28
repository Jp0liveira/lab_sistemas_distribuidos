#!/usr/bin/env python3
"""
Script de Testes Automatizados para o DDB
Testa funcionalidades principais do sistema
"""

import socket
import json
import time
import hashlib
import sys

class DDBTester:
    def __init__(self):
        self.nodes = [
            {'id': 1, 'host': 'localhost', 'port': 5001},
            {'id': 2, 'host': 'localhost', 'port': 5002},
            {'id': 3, 'host': 'localhost', 'port': 5003}
        ]
        self.passed = 0
        self.failed = 0
    
    def calculate_checksum(self, data):
        return hashlib.md5(data.encode('utf-8')).hexdigest()
    
    def send_message(self, node, message, timeout=5):
        """Envia mensagem para um nó e retorna resposta"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((node['host'], node['port']))
            
            s.sendall(json.dumps(message).encode('utf-8'))
            response = s.recv(8192).decode('utf-8')
            
            s.close()
            return json.loads(response)
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def print_header(self, text):
        print(f"\n{'='*70}")
        print(f"  {text}")
        print(f"{'='*70}\n")
    
    def print_test(self, name, passed, details=""):
        status = "✓ PASSOU" if passed else "✗ FALHOU"
        color = "\033[92m" if passed else "\033[91m"
        reset = "\033[0m"
        
        print(f"{color}{status}{reset} - {name}")
        if details:
            print(f"        {details}")
        
        if passed:
            self.passed += 1
        else:
            self.failed += 1
    
    def test_connectivity(self):
        """Teste 1: Conectividade dos Nós"""
        self.print_header("TESTE 1: Conectividade dos Nós")
        
        for node in self.nodes:
            try:
                message = {'type': 'STATUS'}
                response = self.send_message(node, message, timeout=3)
                
                if response.get('status') != 'error':
                    self.print_test(
                        f"Nó {node['id']} está acessível",
                        True,
                        f"Porta {node['port']} respondendo"
                    )
                else:
                    self.print_test(
                        f"Nó {node['id']} está acessível",
                        False,
                        f"Erro: {response.get('message')}"
                    )
            except Exception as e:
                self.print_test(
                    f"Nó {node['id']} está acessível",
                    False,
                    f"Exceção: {e}"
                )
    
    def test_query_execution(self):
        """Teste 2: Execução de Queries"""
        self.print_header("TESTE 2: Execução de Queries")
        
        # Teste SELECT
        query = "SELECT * FROM usuarios LIMIT 1;"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        node = self.nodes[0]
        response = self.send_message(node, message)
        
        self.print_test(
            "Query SELECT executada",
            response.get('status') == 'success',
            f"Nó {response.get('node_id', '?')} respondeu"
        )
        
        # Teste INSERT
        query = f"INSERT INTO usuarios (nome, email) VALUES ('Teste Auto', 'auto_{int(time.time())}@test.com');"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        response = self.send_message(node, message)
        
        self.print_test(
            "Query INSERT executada",
            response.get('status') == 'success',
            f"Linhas afetadas: {response.get('rows_affected', 0)}"
        )
    
    def test_replication(self):
        """Teste 3: Replicação entre Nós"""
        self.print_header("TESTE 3: Replicação entre Nós")
        
        # Insere em um nó
        timestamp = int(time.time())
        query = f"INSERT INTO usuarios (nome, email) VALUES ('Teste Replicação', 'repl_{timestamp}@test.com');"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        node1 = self.nodes[0]
        response = self.send_message(node1, message)
        
        if response.get('status') != 'success':
            self.print_test("Inserção inicial", False, "Falha ao inserir")
            return
        
        self.print_test("Inserção inicial", True, f"Inserido no nó {node1['id']}")
        
        # Aguarda replicação
        time.sleep(2)
        
        # Verifica em outros nós
        query_check = f"SELECT * FROM usuarios WHERE email = 'repl_{timestamp}@test.com';"
        checksum = self.calculate_checksum(query_check)
        
        for node in self.nodes[1:]:  # Pula o primeiro (já testado)
            message = {
                'type': 'QUERY',
                'query': query_check,
                'checksum': checksum
            }
            
            response = self.send_message(node, message)
            
            if response.get('status') == 'success':
                data = response.get('data', [])
                found = len(data) > 0
                self.print_test(
                    f"Replicação para nó {node['id']}",
                    found,
                    f"{'Registro encontrado' if found else 'Registro NÃO encontrado'}"
                )
            else:
                self.print_test(
                    f"Replicação para nó {node['id']}",
                    False,
                    "Erro ao verificar"
                )
    
    def test_checksum_integrity(self):
        """Teste 4: Verificação de Integridade (Checksum)"""
        self.print_header("TESTE 4: Verificação de Integridade")
        
        # Query válida com checksum correto
        query = "SELECT 1;"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        node = self.nodes[0]
        response = self.send_message(node, message)
        
        self.print_test(
            "Checksum válido aceito",
            response.get('status') == 'success',
            "Query com checksum correto executada"
        )
        
        # Query com checksum inválido
        message_invalid = {
            'type': 'QUERY',
            'query': query,
            'checksum': 'invalid_checksum_12345'
        }
        
        response = self.send_message(node, message_invalid)
        
        self.print_test(
            "Checksum inválido rejeitado",
            response.get('status') == 'error' and 'checksum' in response.get('message', '').lower(),
            "Query com checksum inválido foi rejeitada"
        )
    
    def test_coordinator_status(self):
        """Teste 5: Status do Coordenador"""
        self.print_header("TESTE 5: Status do Coordenador")
        
        coordinator_found = False
        coordinator_id = None
        
        for node in self.nodes:
            message = {'type': 'STATUS'}
            response = self.send_message(node, message)
            
            if response.get('is_coordinator'):
                coordinator_found = True
                coordinator_id = response.get('node_id')
                self.print_test(
                    f"Nó {node['id']} é coordenador",
                    True,
                    "Coordenador identificado"
                )
            else:
                print(f"        Nó {node['id']}: Nó comum")
        
        self.print_test(
            "Sistema tem um coordenador",
            coordinator_found,
            f"Coordenador: Nó {coordinator_id}" if coordinator_id else "Nenhum coordenador encontrado"
        )
    
    def test_load_distribution(self):
        """Teste 6: Distribuição de Carga"""
        self.print_header("TESTE 6: Distribuição de Carga")
        
        # Envia várias queries e verifica estatísticas
        initial_stats = {}
        
        # Coleta estatísticas iniciais
        for node in self.nodes:
            message = {'type': 'STATUS'}
            response = self.send_message(node, message)
            initial_stats[node['id']] = response.get('queries_processed', 0)
        
        # Envia múltiplas queries
        queries_sent = 10
        for i in range(queries_sent):
            query = f"SELECT {i};"
            checksum = self.calculate_checksum(query)
            message = {
                'type': 'QUERY',
                'query': query,
                'checksum': checksum
            }
            
            # Alterna entre nós
            node = self.nodes[i % len(self.nodes)]
            self.send_message(node, message)
            time.sleep(0.1)
        
        # Verifica estatísticas finais
        final_stats = {}
        for node in self.nodes:
            message = {'type': 'STATUS'}
            response = self.send_message(node, message)
            final_stats[node['id']] = response.get('queries_processed', 0)
        
        # Calcula diferença
        all_processed = True
        for node_id in initial_stats:
            diff = final_stats.get(node_id, 0) - initial_stats.get(node_id, 0)
            print(f"        Nó {node_id}: {diff} queries processadas")
            if diff == 0:
                all_processed = False
        
        self.print_test(
            "Queries distribuídas entre nós",
            all_processed,
            f"{queries_sent} queries enviadas"
        )
    
    def test_acid_properties(self):
        """Teste 7: Propriedades ACID (Transações)"""
        self.print_header("TESTE 7: Propriedades ACID")
        
        # Teste de rollback com query inválida
        query = "INSERT INTO tabela_inexistente (campo) VALUES ('teste');"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        node = self.nodes[0]
        response = self.send_message(node, message)
        
        self.print_test(
            "Rollback em query inválida",
            response.get('status') == 'error',
            "Erro detectado e transação não commitada"
        )
        
        # Teste de transação bem-sucedida
        query = "SELECT COUNT(*) as total FROM usuarios;"
        checksum = self.calculate_checksum(query)
        message = {
            'type': 'QUERY',
            'query': query,
            'checksum': checksum
        }
        
        response = self.send_message(node, message)
        
        self.print_test(
            "Commit em query válida",
            response.get('status') == 'success',
            "Query executada e commitada com sucesso"
        )
    
    def run_all_tests(self):
        """Executa todos os testes"""
        print("\n" + "="*70)
        print("  SISTEMA DE TESTES AUTOMATIZADOS - DDB")
        print("="*70)
        print(f"\n  Data/Hora: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Nós configurados: {len(self.nodes)}")
        print()
        
        # Executa testes
        self.test_connectivity()
        self.test_query_execution()
        self.test_replication()
        self.test_checksum_integrity()
        self.test_coordinator_status()
        self.test_load_distribution()
        self.test_acid_properties()
        
        # Relatório final
        self.print_header("RELATÓRIO FINAL")
        
        total = self.passed + self.failed
        percentage = (self.passed / total * 100) if total > 0 else 0
        
        print(f"  Total de testes: {total}")
        print(f"  Testes passados: {self.passed} ({percentage:.1f}%)")
        print(f"  Testes falhados: {self.failed}")
        print()
        
        if self.failed == 0:
            print("  🎉 TODOS OS TESTES PASSARAM! 🎉")
        else:
            print(f"  ⚠️  {self.failed} teste(s) falharam. Verifique os logs.")
        
        print("\n" + "="*70 + "\n")
        
        return self.failed == 0


if __name__ == '__main__':
    print("\nAguarde os nós iniciarem completamente...")
    print("(Certifique-se de que todos os 3 nós estão rodando)\n")
    time.sleep(3)
    
    tester = DDBTester()
    success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)
