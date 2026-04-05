# Plano de Testes — Execução de ETL via Admin Panel

## Contexto
O painel admin permite executar pipelines ETL individuais diretamente pela interface web.
A execução ocorre via Docker-in-Docker (socket mount): o container API orquestra containers
ETL efêmeros usando o Docker daemon do host.

## Arquitetura de Execução

```
Frontend (WebSocket) → API Container → Docker Socket → Docker Daemon (host)
  ↓
1. docker compose build etl   (garante que br-acc-etl:latest existe)
2. docker run --rm br-acc-etl (com volumes explícitos via host path)
   -v {host_root}/etl:/workspace/etl:ro
   -v {host_root}/data:/workspace/data
   bash -c "cd /workspace/etl && uv run bracc-etl run --source <id> ..."
3. Logs streamados via WebSocket de volta ao frontend
```

### Por que volumes explícitos com host path?

`docker compose run` resolve `.` relativo ao compose file DENTRO do container API
(`/app/host/`), mas o daemon do Docker precisa do path no HOST. O admin_service
detecta o path HOST via `/proc/mounts` (bind mount de `/app/host`) e passa `-v`
explícitos para `docker run`.

## Pré-requisitos

1. Stack rodando: `docker compose ps` mostra neo4j, api e frontend healthy
2. Neo4j acessível no network `br-acc_default`
3. Imagem ETL construída: `docker images br-acc-etl`
4. `/app/host` montado na API: `cat /proc/mounts | grep app/host`

## Casos de Teste

### TC-01: Pipeline Individual — sanctions (fonte pequena)
**Fonte:** `sanctions`
**Expectativa:** Execução completa em < 2 min, status "success"
**Verificação:**
```bash
# Via WebSocket (frontend)
# Admin → Executar → selecionar "sanctions" → clicar Executar
# Log deve terminar com: {"type":"end","status":"success","exit_code":0}

# Verificar dados no Neo4j
docker exec bracc-neo4j cypher-shell -u neo4j -p <NEO4J_PASSWORD> \
  "MATCH (n:SanctionedEntity) RETURN count(n) as total"
```

### TC-02: Ausência do erro de path
**Verificação negativa:** log NÃO deve conter:
- `No such file or directory`
- `cd: /workspace/etl`
- `exit_code: 1` sem mensagem de erro do ETL

### TC-03: Pipeline cvm
**Fonte:** `cvm`
**Expectativa:** Execução sem erro de path, status "success" ou falha por dados ausentes
**Verificação:**
```bash
# Log deve mostrar progresso do ETL, não erro de shell
# Se dados não baixados: erro vindo do bracc-etl, não do bash
```

### TC-04: Fontes não-implementadas ausentes do dropdown
**Expectativa:** Frontend mostra apenas fontes com `implementation_state = "implemented"`
**Verificação:**
```bash
curl -H "Authorization: Bearer <token>" http://82.25.65.4:8000/api/v1/admin/sources \
  | grep implementation_state
```

### TC-05: Execução concorrente
**Ação:** Disparar 2 pipelines simultaneamente
**Expectativa:** Containers separados, logs independentes
**Verificação:**
```bash
docker ps --filter "name=etl-pipeline" --format "table {{.Names}}\t{{.Status}}"
```

### TC-06: Neo4j indisponível
**Ação:** `docker stop bracc-neo4j` antes de executar
**Expectativa:** ETL falha com erro de conexão, status "failed"
**Verificação:** Log contém mensagem de conexão recusada

## Comandos de Diagnóstico

```bash
# Verificar host path detectado (dentro do container API)
docker exec br-acc-api-1 cat /proc/mounts | grep app/host

# Verificar imagem ETL existe
docker images br-acc-etl

# Verificar conteúdo da imagem baked
docker run --rm br-acc-etl ls -la /workspace/etl/

# Testar ETL manualmente com volumes explícitos (substitua HOST_ROOT)
HOST_ROOT=/opt/br-acc
docker run --rm \
  --network br-acc_default \
  -v ${HOST_ROOT}/etl:/workspace/etl:ro \
  -v ${HOST_ROOT}/data:/workspace/data \
  -e NEO4J_PASSWORD=<senha> \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_USER=neo4j \
  br-acc-etl \
  bash -c "cd /workspace/etl && uv run bracc-etl run --source sanctions \
    --neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j \
    --neo4j-password \"\$NEO4J_PASSWORD\" --neo4j-database neo4j \
    --data-dir /workspace/data --linking-tier full"

# Verificar network
docker network inspect br-acc_default | grep -E "Name|IPv4"

# Ver logs de execução ETL recente
docker logs $(docker ps -lq --filter "name=etl-pipeline") 2>&1 | tail -30
```

## Problemas Conhecidos

| Problema | Status | Solução |
|---|---|---|
| `docker compose run` com path translation DinD | Resolvido | `docker run` com volumes explícitos via host path de `/proc/mounts` |
| `/workspace/etl: No such file` no compose run | Resolvido | Idem acima |
| `--no-deps` causava conflito de nome | N/A | Não usado mais |
| Path resolution no container API | Resolvido | `_get_repo_root()` + `_get_host_repo_root()` |

## Critérios de Aceite

- [ ] TC-01: sanctions executa com status "success"
- [ ] TC-02: nenhum erro de path (`cd: /workspace/etl`)
- [ ] TC-04: fontes não-implementadas ausentes do dropdown
- [ ] TC-06: erro de Neo4j tratado com status "failed" (não crash)
