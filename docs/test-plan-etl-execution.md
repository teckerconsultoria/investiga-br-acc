# Plano de Testes — Execução de ETL via Admin Panel

## Contexto
O painel admin permite executar pipelines ETL individuais ou bootstrap-all diretamente pela interface web. A execução ocorre via Docker-in-Docker (socket mount), onde o container API orquestra a criação de containers ETL efêmeros.

## Arquitetura de Execução

```
Frontend (WebSocket) → API Container → Docker Socket
  ↓
1. docker compose build etl (se imagem não existe)
2. docker run --rm br-acc-etl "uv run bracc-etl run --source <pipeline_id> ..."
3. Logs streamados via WebSocket de volta ao frontend
```

## Pré-requisitos

1. Stack rodando: Neo4j, API, Frontend
2. Neo4j acessível no network `br-acc_default`
3. Imagem ETL construída (`docker compose build etl`)
4. Dados seed carregados no Neo4j

## Casos de Teste

### TC-01: Pipeline Individual — Fonte Simples
**Fonte:** `sanctions` (dados pequenos, já seedados)
**Expectativa:** Execução completa em < 2 min, status "success"
**Verificação:**
```bash
# Via API
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/v1/admin/sources

# Via WebSocket (frontend)
# Selecionar "sanctions" na aba Executar → clicar "Executar"
# Verificar log mostra "success" no final

# Verificar dados no Neo4j
docker exec bracc-neo4j cypher-shell -u neo4j -p changeme \
  "MATCH (n:SanctionedEntity) RETURN count(n) as total"
```

### TC-02: Pipeline Individual — Fonte CVM
**Fonte:** `cvm`
**Expectativa:** Execução completa, sem erros de path (`/workspace/etl: No such file or directory`)
**Verificação:**
- Log não deve conter "No such file or directory"
- Status final deve ser "success"
- Dados devem aparecer no Neo4j: `MATCH (n:CvmProceeding) RETURN count(n)`

### TC-03: Pipeline Individual — Fonte CNPJ
**Fonte:** `cnpj` (core, dados grandes)
**Expectativa:** Execução completa (pode demorar horas)
**Verificação:**
- Log mostra progresso
- Sem erros de conexão com Neo4j
- `MATCH (n:Company) RETURN count(n)` > 0

### TC-04: Pipeline — Fonte sem Implementação
**Fonte:** Qualquer fonte com `implementation_state != "implemented"`
**Expectativa:** Fonte não aparece no dropdown de execução individual
**Verificação:**
- Frontend: dropdown só mostra fontes implementadas
- Se chamado via API: retorna 404

### TC-05: Execução Concorrente
**Ação:** Disparar 2 pipelines simultaneamente
**Expectativa:** Ambos executam independentemente (containers separados com `--name etl-<run_id>`)
**Verificação:**
- `docker ps` mostra 2 containers ETL rodando
- Logs não se misturam

### TC-06: Neo4j Indisponível
**Ação:** Parar Neo4j antes de executar pipeline
**Expectativa:** Pipeline falha com erro claro de conexão
**Verificação:**
- Log contém mensagem de erro de conexão
- Status = "failed" ou "error"

### TC-07: Bootstrap All (via Python script)
**Ação:** Executar bootstrap-all pela aba Config
**Expectativa:** Script `run_bootstrap_all.py` roda sequencialmente
**Verificação:**
- `audit-results/bootstrap-all/latest/summary.json` atualizado
- Contagem de fontes processadas = total no contrato

## Problemas Conhecidos

| Problema | Status | Solução |
|---|---|---|
| `docker compose run` com volumes no Windows | Resolvido | Usar `docker run` direto com imagem baked-in |
| Container Neo4j conflito de nome | Resolvido | Adicionado `--no-deps` (legacy, não mais usado) |
| `/workspace/etl: No such file` | Resolvido | `docker run br-acc-etl` em vez de `docker compose run etl` |
| Path resolution no container API | Resolvido | `_get_repo_root()` fallback para `/app/host` |

## Comandos de Diagnóstico

```bash
# Verificar containers ETL
docker ps -a --filter "name=etl-"

# Ver logs de um container ETL específico
docker logs etl-pipeline-cvm-<hash>

# Verificar network
docker network inspect br-acc_default

# Verificar se Neo4j está acessível do network
docker run --rm --network br-acc_default alpine nc -zv neo4j 7687

# Verificar imagem ETL
docker images br-acc-etl

# Testar ETL manualmente
docker run --rm --network br-acc_default \
  -e NEO4J_PASSWORD=changeme \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_USER=neo4j \
  br-acc-etl \
  bash -lc "cd /workspace/etl && uv run bracc-etl run --source sanctions \
    --neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j \
    --neo4j-password changeme --neo4j-database neo4j \
    --data-dir ../data --linking-tier full"
```

## Critérios de Aceite

- [ ] TC-01: sanctions executa com sucesso
- [ ] TC-02: cvm executa sem erro de path
- [ ] TC-04: fontes não-implementadas não aparecem no dropdown
- [ ] TC-06: erro de Neo4j indisponível é tratado graciosamente
