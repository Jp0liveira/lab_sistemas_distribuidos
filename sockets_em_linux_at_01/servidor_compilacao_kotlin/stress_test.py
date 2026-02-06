#!/usr/bin/env python3
"""
Script de Teste de Stress para Servidor de Compilação Kotlin
Envia múltiplas requisições concorrentes para testar a robustez do servidor
"""

import socket
import threading
import time
from datetime import datetime

# Configurações
SERVER_IP = '127.0.0.1'
SERVER_PORT = 51482
NUM_REQUESTS = 1000  # Número total de requisições
NUM_THREADS = 50     # Número de threads concorrentes

# Código Kotlin de teste (simples e rápido)
CODIGO_TESTE = """fun main() {
    println("Teste de stress - Request executada com sucesso!")
}"""

# Estatísticas globais
stats = {
    'success': 0,
    'errors': 0,
    'timeouts': 0,
    'total_time': 0,
    'lock': threading.Lock()
}

def enviar_requisicao(request_id):
    """
    Envia uma requisição individual ao servidor
    """
    start_time = time.time()
    
    try:
        # Cria socket e conecta
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30)  # Timeout de 30 segundos
        s.connect((SERVER_IP, SERVER_PORT))
        
        # Envia código
        s.sendall(CODIGO_TESTE.encode('utf-8'))
        
        # Recebe resposta
        resposta = s.recv(4096).decode('utf-8')
        
        # Fecha conexão
        s.close()
        
        elapsed_time = time.time() - start_time
        
        # Atualiza estatísticas
        with stats['lock']:
            if "ERRO" not in resposta:
                stats['success'] += 1
            else:
                stats['errors'] += 1
            stats['total_time'] += elapsed_time
        
        print(f"[{request_id:04d}] Sucesso em {elapsed_time:.3f}s")
        
    except socket.timeout:
        with stats['lock']:
            stats['timeouts'] += 1
        print(f"[{request_id:04d}] TIMEOUT")
        
    except Exception as e:
        with stats['lock']:
            stats['errors'] += 1
        print(f"[{request_id:04d}] ERRO: {e}")

def worker(thread_id, requests_per_thread):
    """
    Worker thread que envia múltiplas requisições
    """
    start_req = thread_id * requests_per_thread
    end_req = start_req + requests_per_thread
    
    for req_id in range(start_req, end_req):
        enviar_requisicao(req_id)
        time.sleep(0.01)  # Pequeno delay entre requisições

def main():
    print("=" * 70)
    print("TESTE DE STRESS - SERVIDOR DE COMPILAÇÃO KOTLIN")
    print("=" * 70)
    print(f"Servidor: {SERVER_IP}:{SERVER_PORT}")
    print(f"Total de requisições: {NUM_REQUESTS}")
    print(f"Threads concorrentes: {NUM_THREADS}")
    print(f"Requisições por thread: {NUM_REQUESTS // NUM_THREADS}")
    print("=" * 70)
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    start_time = time.time()
    
    # Cria e inicia threads
    threads = []
    requests_per_thread = NUM_REQUESTS // NUM_THREADS
    
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(i, requests_per_thread))
        threads.append(t)
        t.start()
    
    # Aguarda todas as threads terminarem
    for t in threads:
        t.join()
    
    total_time = time.time() - start_time
    
    # Exibe estatísticas
    print()
    print("=" * 70)
    print("RESULTADOS")
    print("=" * 70)
    print(f"Término: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tempo total: {total_time:.2f} segundos")
    print(f"Requisições bem-sucedidas: {stats['success']}")
    print(f"Erros de compilação/execução: {stats['errors']}")
    print(f"Timeouts: {stats['timeouts']}")
    print(f"Total processado: {stats['success'] + stats['errors'] + stats['timeouts']}")
    print()
    print(f"Taxa de sucesso: {(stats['success']/NUM_REQUESTS)*100:.2f}%")
    print(f"Requisições por segundo: {NUM_REQUESTS/total_time:.2f} req/s")
    
    if stats['success'] > 0:
        avg_time = stats['total_time'] / stats['success']
        print(f"Tempo médio por requisição: {avg_time:.3f} segundos")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
