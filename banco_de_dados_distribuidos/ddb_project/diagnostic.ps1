# diagnostic.ps1
Write-Host "`n=========================================="
Write-Host "DIAGNÓSTICO DO SISTEMA DDB"
Write-Host "==========================================`n"

Write-Host "1. Containers rodando:"
docker-compose ps

Write-Host "`n2. Verificando conectividade entre nós:"
docker exec ddb_node1 ping -c 2 172.20.0.12 2>$null
docker exec ddb_node1 ping -c 2 172.20.0.13 2>$null

Write-Host "`n3. Verificando se nós estão escutando:"
docker exec ddb_node1 netstat -tln 2>$null | Select-String "5000"
docker exec ddb_node2 netstat -tln 2>$null | Select-String "5000"
docker exec ddb_node3 netstat -tln 2>$null | Select-String "5000"

Write-Host "`n4. Últimos logs dos nós:"
Write-Host "`nNó 1:"
docker-compose logs --tail=5 ddb-node1

Write-Host "`nNó 2:"
docker-compose logs --tail=5 ddb-node2

Write-Host "`nNó 3:"
docker-compose logs --tail=5 ddb-node3

Write-Host "`n5. Contagem de usuários em cada MySQL:"
$count1 = docker exec mysql1 mysql -u ddb_user -pddb_password ddb_database -se "SELECT COUNT(*) FROM users;" 2>$null
$count2 = docker exec mysql2 mysql -u ddb_user -pddb_password ddb_database -se "SELECT COUNT(*) FROM users;" 2>$null
$count3 = docker exec mysql3 mysql -u ddb_user -pddb_password ddb_database -se "SELECT COUNT(*) FROM users;" 2>$null

Write-Host "  MySQL 1: $count1 usuários"
Write-Host "  MySQL 2: $count2 usuários"
Write-Host "  MySQL 3: $count3 usuários"

Write-Host "`n=========================================="