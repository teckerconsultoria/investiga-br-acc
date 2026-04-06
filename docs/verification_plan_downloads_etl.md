# Plano de Verificação — Download Scripts & ETL Pipelines

**Revisado:** 2026-04-06
**Modo:** Diagnóstico — sem execução de downloads. Verificar se configurações de script e pipeline estão corretas.

**Critérios de exclusão:**
- Fontes GCP/BigQuery → descartadas
- Fontes com configuração desatualizada sem caminho claro → descartadas
- Orphans (script sem pipeline) → descartados temporariamente

---

## ✅ Verificadas — Testadas e Funcionando

| Fonte | Script | ETL | Resultado |
|-------|--------|-----|-----------|
| `tcu` | `download_tcu.py` | `tcu` | 45.369 sanctions (2026-04-05) |
| `transferegov` | `download_transferegov.py` | `transferegov` | 71.871 amendments (2026-04-05) |
| `pgfn` | `download_pgfn.py` | `pgfn` | ~24M registros (2026-04-06) |
| `ceaf` | `download_ceaf_api.py` | `ceaf` | API-based, testado |
| `sanctions` | `download_sanctions_api.py` | `sanctions` | CEIS+CNEP via API, testado |
| `cnpj` | `download_cnpj.py` | `cnpj` | Dados na VPS |
| `tse` | `download_tse.py` | `tse` | Dados na VPS |
| `transparencia` | `download_transparencia.py` | `transparencia` | Dados na VPS |
| `leniency` | `download_leniency.py` | `leniency` | Dados na VPS |

---

## Tier 1 — DIAGNÓSTICO

Para cada fonte: verificar se **URL/API ainda responde**, **campos esperados pelo ETL batem com o schema atual da fonte**, e **script tem parâmetros mínimos** (output-dir, skip-existing).

Legenda de status:
- `?` — não diagnosticado
- `✅` — configuração OK
- `⚠️` — problema identificado
- `❌` — quebrado

### Alta Prioridade

| # | Fonte | Script | Pipeline | Status | Diagnóstico |
|---|-------|--------|----------|--------|-------------|
| 1 | `pncp` | `download_pncp.py` | `pncp` | ? | URL API, paginação incremental |
| 2 | `siconfi` | `download_siconfi.py` | `siconfi` | ? | URL API SICONFI, campos municipais |
| 3 | `camara_inquiries` | `download_camara_inquiries.py` | `camara_inquiries` | ? | API Câmara pública |
| 4 | `senado_cpis` | `download_senado_cpis.py` | `senado_cpis` | ? | API Senado pública |
| 5 | `querido_diario` | `download_querido_diario.py` | `querido_diario` | ? | API pública |

### Prioridade Normal

| # | Fonte | Script | Pipeline | Status | Diagnóstico |
|---|-------|--------|----------|--------|-------------|
| 6 | `cvm` | `download_cvm.py` | `cvm` | ? | URL bulk download CVM |
| 7 | `cvm_funds` | `download_cvm_funds.py` | `cvm_funds` | ? | Fundos de investimento |
| 8 | `icij` | `download_icij.py` | `icij` | ? | Panama/Pandora Papers |
| 9 | `opensanctions` | `download_opensanctions.py` | `opensanctions` | ? | Dataset público |
| 10 | `ofac` | `download_ofac.py` | `ofac` | ? | SDN list EUA |
| 11 | `eu_sanctions` | `download_eu_sanctions.py` | `eu_sanctions` | ? | Lista sanções UE |
| 12 | `un_sanctions` | `download_un_sanctions.py` | `un_sanctions` | ? | Lista sanções ONU |
| 13 | `world_bank` | `download_world_bank.py` | `world_bank` | ? | Debarred firms |
| 14 | `bcb` | `download_bcb.py` | `bcb` | ? | Banco Central |
| 15 | `pep_cgu` | `download_pep_cgu.py` | `pep_cgu` | ? | PEP list CGU |
| 16 | `holdings` | `download_holdings.py` | `holdings` | ? | Participações societárias |
| 17 | `camara` | `download_camara.py` | `camara` | ? | Dados de deputados |
| 18 | `senado` | `download_senado.py` | `senado` | ? | Dados de senadores |
| 19 | `cpgf` | `download_cpgf.py` | `cpgf` | ? | Cartão pagamento federal |
| 20 | `viagens` | `download_viagens.py` | `viagens` | ? | Viagens a serviço |
| 21 | `renuncias` | `download_renuncias.py` | `renuncias` | ? | Renúncias fiscais |
| 22 | `tse_bens` | `download_tse_bens.py` | `tse_bens` | ? | Bens declarados candidatos |
| 23 | `tse_filiados` | `download_tse_filiados.py` | `tse_filiados` | ? | Filiados partidários |

### A Confirmar (script pode não existir)

| # | Fonte | Ação se script existir | Ação se não existir |
|---|-------|------------------------|---------------------|
| 24 | `bndes` | Diagnosticar URL | Criar script |
| 25 | `ibama` | Diagnosticar URL | Criar script |
| 26 | `datasus` | Diagnosticar URL | Criar script |
| 27 | `inep` | Diagnosticar URL | Criar script |

---

## On-demand (não batch)

| Fonte | Situação |
|-------|----------|
| `datajud` | API pública com API key disponível; coleta sob demanda (não agendável em bulk) |

---

## Descartadas

| Fonte | Motivo |
|-------|--------|
| `caged` | BigQuery — requer GCP billing project |
| `comprasnet` | Script não existe; coberto por `transparencia` e `pncp` |
| `siop` | API retorna 403 |
| `cnpj_bq` / `dou` / `mides` / `stf` | BigQuery — fora do escopo |
| `senado_cpi_archive` | Orphan — descartado temporariamente |
| `senado_parlamentares` | Orphan — descartado temporariamente |
| `tesouro_emendas` | Orphan — descartado temporariamente |

---

## Como executar o diagnóstico

Para cada fonte no Tier 1:

```bash
# 1. Verificar se URL base ainda responde (sem baixar nada)
uv run python scripts/download_<source>.py --help

# 2. Inspecionar campos esperados pelo ETL
grep -n "row\[" etl/src/bracc_etl/pipelines/<source>.py | head -20

# 3. Confirmar que o diretório de dados esperado bate com o script
grep -n "data_dir\|output.dir\|Path" scripts/download_<source>.py | head -10
```

---

## Próximos Passos

1. ✅ PGFN rodando — aguardar conclusão
2. Diagnosticar Tier 1 alta prioridade: pncp, siconfi, camara_inquiries, senado_cpis, querido_diario
3. Diagnosticar Tier 1 prioridade normal (23 fontes)
4. Confirmar existência de scripts para bndes, ibama, datasus, inep
