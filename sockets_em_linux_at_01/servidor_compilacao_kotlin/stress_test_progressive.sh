#!/bin/bash
# Script de Teste de Stress Progressivo
# Testa o servidor com cargas crescentes

echo "=========================================="
echo "TESTE DE STRESS PROGRESSIVO"
echo "=========================================="
echo ""

# Configurações
SERVER_IP="127.0.0.1"
SERVER_PORT=51482

# Código Kotlin de teste
CODIGO_TESTE='fun main() {
    println("Teste OK")
}'

# Função para enviar uma requisição
enviar_requisicao() {
    local id=$1
    echo "$CODIGO_TESTE" | nc $SERVER_IP $SERVER_PORT > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "[$id] Sucesso"
        return 0
    else
        echo "[$id] Falha"
        return 1
    fi
}

# Teste 1: 10 requisições sequenciais
echo "TESTE 1: 10 requisições sequenciais"
echo "------------------------------------"
sucesso=0
falha=0
start_time=$(date +%s)

for i in {1..10}; do
    if enviar_requisicao $i; then
        ((sucesso++))
    else
        ((falha++))
    fi
    sleep 0.1
done

end_time=$(date +%s)
tempo=$((end_time - start_time))
echo "Tempo: ${tempo}s | Sucesso: $sucesso | Falha: $falha"
echo ""

# Teste 2: 50 requisições sequenciais
echo "TESTE 2: 50 requisições sequenciais"
echo "------------------------------------"
sucesso=0
falha=0
start_time=$(date +%s)

for i in {1..50}; do
    if enviar_requisicao $i; then
        ((sucesso++))
    else
        ((falha++))
    fi
    sleep 0.05
done

end_time=$(date +%s)
tempo=$((end_time - start_time))
echo "Tempo: ${tempo}s | Sucesso: $sucesso | Falha: $falha"
echo ""

# Teste 3: 100 requisições com 10 concorrentes
echo "TESTE 3: 100 requisições (10 concorrentes)"
echo "--------------------------------------------"
start_time=$(date +%s)

for i in {1..100}; do
    enviar_requisicao $i &
    
    # Limita a 10 processos em paralelo
    if [ $((i % 10)) -eq 0 ]; then
        wait
    fi
done

wait
end_time=$(date +%s)
tempo=$((end_time - start_time))
echo "Tempo total: ${tempo}s"
echo ""

echo "=========================================="
echo "TESTES CONCLUÍDOS"
echo "=========================================="
