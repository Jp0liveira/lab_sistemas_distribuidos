"""
Teste de Stress para DDB
Testa capacidade de processamento e replicação do sistema
"""

import socket
import json
import hashlib
import time
import threading
import random
from datetime import datetime
from collections import defaultdict


class StressTest:
    """Teste de carga para DDB"""

    def __init__(self, nodes):
        """
        Args:
            nodes: Lista de tuplas (host, port) dos nós
        """
        self.nodes = nodes
        self.results = defaultdict(list)
        self.errors = []
        self.lock = threading.Lock()

    def create_message(self, msg_type, data):
        """Cria mensagem com checksum"""
        message = {
            'type': msg_type,
            'node_id': 'stress_test',
            'timestamp': time.time(),
            'data': data
        }
        checksum_data = message.copy()
        message['checksum'] = hashlib.md5(
            json.dumps(checksum_data, sort_keys=True).encode()
        ).hexdigest()
        return message

    def send_query(self, host, port, query, query_type='READ'):
        """Envia query para um nó"""
        try:
            start = time.time()

            message = self.create_message('QUERY', {
                'query': query,
                'type': query_type
            })

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((host, port))
            sock.send(json.dumps(message).encode())

            # Recebe resposta
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break

            sock.close()

            elapsed = time.time() - start
            response = json.loads(data.decode())

            return {
                'success': response['data']['status'] == 'success',
                'elapsed': elapsed,
                'node': f"{host}:{port}",
                'response': response
            }

        except Exception as e:
            return {
                'success': False,
                'elapsed': 0,
                'node': f"{host}:{port}",
                'error': str(e)
            }

    def test_single_query(self, query, query_type='READ', node_idx=None):
        """Testa uma query em um nó específico ou aleatório"""
        if node_idx is None:
            node_idx = random.randint(0, len(self.nodes) - 1)

        host, port = self.nodes[node_idx]
        result = self.send_query(host, port, query, query_type)

        with self.lock:
            if result['success']:
                self.results['queries'].append(result)
            else:
                self.errors.append(result)

        return result

    def test_concurrent_reads(self, num_threads=10, queries_per_thread=10):
        """Testa múltiplas leituras concorrentes"""
        print(f"\n{'=' * 60}")
        print(f"TESTE 1: Leituras Concorrentes")
        print(f"Threads: {num_threads} | Queries por thread: {queries_per_thread}")
        print(f"{'=' * 60}\n")

        queries = [
            "SELECT * FROM users;",
            "SELECT * FROM users WHERE id = 1;",
            "SELECT COUNT(*) FROM users;",
            "SELECT name, email FROM users LIMIT 5;"
        ]

        def worker():
            for _ in range(queries_per_thread):
                query = random.choice(queries)
                self.test_single_query(query, 'READ')
                time.sleep(0.01)  # Pequeno delay

        start_time = time.time()
        threads = []

        for _ in range(num_threads):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time

        self.print_results("Leituras Concorrentes", elapsed)

    def test_concurrent_writes(self, num_threads=5, queries_per_thread=5):
        """Testa múltiplas escritas concorrentes"""
        print(f"\n{'=' * 60}")
        print(f"TESTE 2: Escritas Concorrentes (com Replicação)")
        print(f"Threads: {num_threads} | Queries por thread: {queries_per_thread}")
        print(f"{'=' * 60}\n")

        def worker(thread_id):
            for i in range(queries_per_thread):
                # INSERT
                query = f"INSERT INTO users (name, email) VALUES ('User{thread_id}_{i}', 'user{thread_id}_{i}@test.com');"
                result = self.test_single_query(query, 'WRITE')

                if result['success']:
                    print(f"✓ Thread {thread_id}: INSERT executado em {result['elapsed']:.3f}s")
                else:
                    print(f"✗ Thread {thread_id}: Erro - {result.get('error', 'Unknown')}")

                time.sleep(0.1)  # Delay entre escritas

        start_time = time.time()
        threads = []

        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time

        self.print_results("Escritas Concorrentes", elapsed)

    def test_replication_consistency(self):
        """Testa consistência da replicação"""
        print(f"\n{'=' * 60}")
        print(f"TESTE 3: Consistência de Replicação")
        print(f"{'=' * 60}\n")

        # Gera ID único
        test_id = int(time.time() * 1000)
        email = f"consistency_test_{test_id}@test.com"

        # 1. INSERT em um nó
        print("1. Inserindo registro no nó 1...")
        insert_query = f"INSERT INTO users (name, email) VALUES ('Consistency Test', '{email}');"
        result = self.test_single_query(insert_query, 'WRITE', node_idx=0)

        if not result['success']:
            print("✗ Falha ao inserir")
            return

        print(f"✓ Registro inserido em {result['elapsed']:.3f}s")

        # 2. Aguarda replicação
        print("\n2. Aguardando replicação (2 segundos)...")
        time.sleep(2)

        # 3. Verifica em todos os nós
        print("\n3. Verificando em todos os nós...")
        select_query = f"SELECT * FROM users WHERE email = '{email}';"

        found_in_nodes = 0
        for idx, (host, port) in enumerate(self.nodes):
            result = self.send_query(host, port, select_query, 'READ')

            if result['success']:
                rows = result['response']['data']['result'].get('rows', [])
                if rows:
                    found_in_nodes += 1
                    print(f"✓ Nó {idx + 1} ({host}:{port}): Registro encontrado")
                else:
                    print(f"✗ Nó {idx + 1} ({host}:{port}): Registro NÃO encontrado")
            else:
                print(f"✗ Nó {idx + 1} ({host}:{port}): Erro na query")

        print(f"\nResultado: {found_in_nodes}/{len(self.nodes)} nós consistentes")

        if found_in_nodes == len(self.nodes):
            print("✓ REPLICAÇÃO CONSISTENTE!")
        else:
            print("✗ INCONSISTÊNCIA DETECTADA!")

    def test_load_balancing(self, num_queries=50):
        """Testa balanceamento de carga"""
        print(f"\n{'=' * 60}")
        print(f"TESTE 4: Balanceamento de Carga")
        print(f"Queries: {num_queries}")
        print(f"{'=' * 60}\n")

        node_counts = defaultdict(int)
        query = "SELECT * FROM users LIMIT 1;"

        for i in range(num_queries):
            result = self.test_single_query(query, 'READ')
            if result['success']:
                node_counts[result['node']] += 1

            if (i + 1) % 10 == 0:
                print(f"Progresso: {i + 1}/{num_queries} queries")

        print("\nDistribuição de queries por nó:")
        for node, count in sorted(node_counts.items()):
            percentage = (count / num_queries) * 100
            print(f"  {node}: {count} queries ({percentage:.1f}%)")

        # Calcula desvio padrão
        import statistics
        if len(node_counts) > 1:
            std_dev = statistics.stdev(node_counts.values())
            mean = statistics.mean(node_counts.values())
            cv = (std_dev / mean) * 100  # Coeficiente de variação

            print(f"\nMédia: {mean:.1f} queries/nó")
            print(f"Desvio padrão: {std_dev:.1f}")
            print(f"Coeficiente de variação: {cv:.1f}%")

            if cv < 20:
                print("✓ Balanceamento BOM (CV < 20%)")
            elif cv < 40:
                print("⚠ Balanceamento REGULAR (20% < CV < 40%)")
            else:
                print("✗ Balanceamento RUIM (CV > 40%)")

    def test_failure_recovery(self):
        """Testa recuperação de falha"""
        print(f"\n{'=' * 60}")
        print(f"TESTE 5: Recuperação de Falha")
        print(f"{'=' * 60}\n")

        print("Este teste requer intervenção manual:")
        print("1. Durante o teste, mate um dos nós (Ctrl+C)")
        print("2. Aguarde alguns segundos")
        print("3. Reinicie o nó")
        print("\nPressione ENTER quando estiver pronto...")
        input()

        # Envia queries continuamente
        print("\nEnviando queries continuamente (30 segundos)...")
        start = time.time()
        success_count = 0
        error_count = 0

        while time.time() - start < 30:
            result = self.test_single_query("SELECT COUNT(*) FROM users;", 'READ')
            if result['success']:
                success_count += 1
                print(f"✓ Query {success_count + error_count}: OK ({result['node']})")
            else:
                error_count += 1
                print(f"✗ Query {success_count + error_count}: ERRO")

            time.sleep(1)

        print(f"\nResultado:")
        print(f"  Sucessos: {success_count}")
        print(f"  Erros: {error_count}")
        print(f"  Taxa de sucesso: {(success_count / (success_count + error_count)) * 100:.1f}%")

    def print_results(self, test_name, elapsed):
        """Imprime resultados do teste"""
        queries = self.results['queries']
        errors = self.errors

        if not queries and not errors:
            print("\nNenhum resultado registrado.")
            return

        total = len(queries) + len(errors)
        success_rate = (len(queries) / total) * 100 if total > 0 else 0

        print(f"\n{'=' * 60}")
        print(f"RESULTADOS: {test_name}")
        print(f"{'=' * 60}")
        print(f"Tempo total: {elapsed:.2f}s")
        print(f"Total de queries: {total}")
        print(f"Sucessos: {len(queries)} ({success_rate:.1f}%)")
        print(f"Erros: {len(errors)}")

        if queries:
            times = [q['elapsed'] for q in queries]
            print(f"\nTempo de resposta:")
            print(f"  Mínimo: {min(times) * 1000:.1f}ms")
            print(f"  Máximo: {max(times) * 1000:.1f}ms")
            print(f"  Médio: {sum(times) / len(times) * 1000:.1f}ms")
            print(f"  Throughput: {len(queries) / elapsed:.1f} queries/s")

        if errors:
            print(f"\nErros encontrados:")
            for err in errors[:5]:  # Mostra até 5 erros
                print(f"  - {err.get('error', 'Unknown error')}")

        # Limpa resultados para próximo teste
        self.results['queries'].clear()
        self.errors.clear()

    def run_all_tests(self):
        """Executa todos os testes"""
        print("\n" + "=" * 60)
        print(" " * 15 + "TESTE DE STRESS - DDB")
        print("=" * 60)
        print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Nós testados: {len(self.nodes)}")
        for idx, (host, port) in enumerate(self.nodes):
            print(f"  Nó {idx + 1}: {host}:{port}")

        try:
            # Teste 1: Leituras
            self.test_concurrent_reads(num_threads=10, queries_per_thread=10)

            # Teste 2: Escritas
            self.test_concurrent_writes(num_threads=5, queries_per_thread=3)

            # Teste 3: Consistência
            self.test_replication_consistency()

            # Teste 4: Balanceamento
            self.test_load_balancing(num_queries=50)

            # Teste 5: Falha (opcional)
            print("\n" + "=" * 60)
            print("Executar teste de recuperação de falha? (s/n): ", end="")
            if input().lower() == 's':
                self.test_failure_recovery()

            print("\n" + "=" * 60)
            print("TODOS OS TESTES CONCLUÍDOS!")
            print("=" * 60 + "\n")

        except KeyboardInterrupt:
            print("\n\nTestes interrompidos pelo usuário.")
        except Exception as e:
            print(f"\n\nErro durante os testes: {e}")


def main():
    print("=" * 60)
    print(" " * 15 + "TESTE DE STRESS - DDB")
    print("=" * 60)

    # Configuração dos nós
    print("\nConfiguração dos nós:")
    print("Digite os nós no formato HOST:PORT (um por linha)")
    print("Pressione ENTER em linha vazia para finalizar")
    print("Exemplo: 192.168.1.101:5000\n")

    nodes = []
    while True:
        node_input = input(f"Nó {len(nodes) + 1}: ").strip()
        if not node_input:
            break

        try:
            host, port = node_input.split(':')
            port = int(port)
            nodes.append((host, port))
            print(f"✓ Nó adicionado: {host}:{port}")
        except:
            print("✗ Formato inválido. Use HOST:PORT")

    if not nodes:
        print("\nNenhum nó configurado. Usando padrão (localhost:5000)")
        nodes = [('127.0.0.1', 5000)]

    # Executa testes
    tester = StressTest(nodes)
    tester.run_all_tests()


if __name__ == "__main__":
    main()