# Matriz Completa — Fontes × Download Scripts × ETL Pipelines

**Revisado:** 2026-04-06
**Status:** Diagnóstico completo concluído

---

## Legenda

| Status | Significado |
|--------|-------------|
| ✅ | Funcionando — script + ETL alinhados, fonte acessível |
| ⚠️ | Parcial — funcionando mas com dependência externa (token, API key, GCP) |
| ❌ | Quebrado — fonte inacessível (403/404/500), URL mudou |
| 🔵 | On-demand — coleta manual sob demanda |
| 🗑️ | Descartada — fora do escopo temporária ou permanentemente |

---

## ✅ EM OPERAÇÃO (9 fontes)

Testadas e produzindo dados ativamente.

| # | Fonte | Download Script | ETL Pipeline | Último Resultado |
|---|-------|-----------------|--------------|------------------|
| 1 | `tcu` | `download_tcu.py` | `tcu` | 45.369 sanctions (2026-04-05) |
| 2 | `transferegov` | `download_transferegov.py` | `transferegov` | 71.871 amendments (2026-04-05) |
| 3 | `pgfn` | `download_pgfn.py` | `pgfn` | ~24M registros (2026-04-06) |
| 4 | `ceaf` | `download_ceaf_api.py` | `ceaf` | API-based, testado |
| 5 | `sanctions` | `download_sanctions_api.py` | `sanctions` | CEIS+CNEP via API, testado |
| 6 | `cnpj` | `download_cnpj.py` | `cnpj` | Dados na VPS |
| 7 | `tse` | `download_tse.py` | `tse` | Dados na VPS |
| 8 | `transparencia` | `download_transparencia.py` | `transparencia` | Dados na VPS |
| 9 | `leniency` | `download_leniency.py` | `leniency` | Dados na VPS |

---

## ✅ DIAGNOSTICADAS — Prontas para Uso (13 fontes)

Script + ETL alinhados, campos validados, fonte acessível.

### Alta Prioridade (3)

| # | Fonte | Download Script | ETL Pipeline | Output | Diagnóstico |
|---|-------|-----------------|--------------|--------|-------------|
| 10 | `siconfi` | `download_siconfi.py` | `siconfi` | CSV (dca_states_YYYY, dca_mun_YYYY) | API 200. Campos: cod_ibge, ente/instituicao, exercicio, conta, coluna, valor |
| 11 | `senado_cpis` | `download_senado_cpis.py` | `senado_cpis` | CSV (inquiries, requirements, sessions, members) | API Senado 200. Depende de `download_senado_cpi_archive.py` para histórico |
| 12 | `querido_diario` | `download_querido_diario.py` | `querido_diario` | JSONL (acts.jsonl) | API Querido Diário 200. Campos: act_id, municipality_name/code, uf, date, title, text, text_status, txt_url |

### Prioridade Normal (11)

| # | Fonte | Download Script | ETL Pipeline | Output | Diagnóstico |
|---|-------|-----------------|--------------|--------|-------------|
| 13 | `pncp` | `download_pncp.py` | `pncp` | JSON (pncp_YYYYMM.json) | **TESTADO** — API 200, 182 registros/dia. Campos confirmados: numeroControlePNCP, orgaoEntidade.cnpj, valorTotalHomologado/Estimado, dataPublicacaoPncp, objetoCompra, modalidadeId, situacaoCompraNome, processo, srp, unidadeOrgao. Script tem checkpoint, retry, manifest, skip-existing. |
| 14 | `cvm` | `download_cvm.py` | `cvm` | ZIP (processo_sancionador) | dados.cvm.gov.br 200 |
| 15 | `cvm_funds` | `download_cvm_funds.py` | `cvm_funds` | CSV (cad_fi) | dados.cvm.gov.br 200 |
| 16 | `icij` | `download_icij.py` | `icij` | ZIP (offshoreleaks) | offshoreleaks-data.icij.org 200 |
| 17 | `opensanctions` | `download_opensanctions.py` | `opensanctions` | JSON (entities.ftm) | data.opensanctions.org 200 |
| 18 | `ofac` | `download_ofac.py` | `ofac` | CSV (sdn, add, alt) | treasury.gov 200 |
| 19 | `un_sanctions` | `download_un_sanctions.py` | `un_sanctions` | XML (consolidated) | scsanctions.un.org 200 |
| 20 | `pep_cgu` | `download_pep_cgu.py` | `pep_cgu` | — | Portal Transparência 200 |
| 21 | `holdings` | `download_holdings.py` | `holdings` | CSV.gz | brasil.io 200, fallback S3 |
| 22 | `camara` | `download_camara.py` | `camara` | CSV.zip | camara.leg.br 200 |
| 23 | `cpgf` | `download_cpgf.py` | `cpgf` | — | Portal Transparência 302→200 |

---

## 🔵 ON-DEMAND (1 fonte)

Coleta manual sob demanda, não agendável em bulk.

| # | Fonte | Download Script | ETL Pipeline | Situação |
|---|-------|-----------------|--------------|----------|
| 27 | `datajud` | `download_datajud.py` | `datajud` | API pública com API key disponível. Coleta sob demanda. |

---

## 🗑️ DESCARTADAS (24 fontes)

### Host de Download Bloqueado (4 fontes)

`dadosabertos-download.cgu.gov.br` retorna 403 — problema no servidor CGU, não no cliente.

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `pep_cgu` | `download_pep_cgu.py` (existe) | `pep_cgu` | 403 Forbidden — host bloqueado (confirmado 2026-04-06) |
| `renuncias` | `download_renuncias.py` (existe) | `renuncias` | 302 → 403 Forbidden |
| `senado` | `download_senado.py` (existe) | `senado` | URL 404 — endpoint descontinuado |
| `viagens` | `download_viagens.py` (existe) | `viagens` | 500 Internal Server Error |

### Dependência Externa / Configuração Não Disponível (2 fontes)

Requerem token/API key não configurados — descartadas temporariamente.

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `eu_sanctions` | `download_eu_sanctions.py` (existe) | `eu_sanctions` | API retorna 403 — requer `EU_SANCTIONS_TOKEN` não configurado |
| `world_bank` | `download_world_bank.py` (existe) | `world_bank` | Legacy CSV 404, JSON API requer `WORLD_BANK_API_KEY` não configurado |

### BigQuery / GCP / Parcial (5 fontes)

Requerem GCP billing project ou cobertura incompleta — fora do escopo.

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `camara_inquiries` | `download_camara_inquiries.py` (existe) | `camara_inquiries` | Modo padrão requer GCP billing; modo `api_only` não cobre histórico completo |
| `tse_bens` | `download_tse_bens.py` (existe) | `tse_bens` | BigQuery-only (`basedosdados.br_tse_eleicoes`) |
| `tse_filiados` | `download_tse_filiados.py` (existe) | `tse_filiados` | BigQuery-only (`basedosdados.br_tse_filiacao_partidaria`) |
| `caged` | `download_caged.py` (existe) | `caged` | BigQuery — requer GCP billing project |
| `cnpj_bq` | `download_cnpj_bq.py` (existe) | — | BigQuery — fora do escopo |

### Fonte Inacessível (6 fontes)

URLs quebradas, APIs retornando 403/404/500.

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `senado` | `download_senado.py` (existe) | `senado` | URL 404 — estrutura de URL mudou ou endpoint descontinuado |
| `viagens` | `download_viagens.py` (existe) | `viagens` | API Transparência retorna 500 Internal Server Error |
| `renuncias` | `download_renuncias.py` (existe) | `renuncias` | Redirect 302 → 403 Forbidden (dadosabertos-download.cgu.gov.br) |
| `bcb` | `download_bcb.py` (existe) | `bcb` | Endpoint retorna 400 Bad Request — URL mudou ou parâmetros diferentes |
| `siop` | `download_siop.py` (existe) | `siop` | API retorna 403 |
| `dou` | `download_dou.py` (existe) | `dou` | BigQuery — fora do escopo |

### Sem Script de Download (4 fontes)

ETL pipeline existe mas script de download não foi criado.

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `bndes` | **NÃO existe** | `bndes` | Sem script de download |
| `ibama` | **NÃO existe** | `ibama` | Sem script de download |
| `datasus` | **NÃO existe** | `datasus` | Sem script de download |
| `inep` | **NÃO existe** | `inep` | Sem script de download |

### Orphans / Cobertas por Outras Fontes (4 fontes)

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `comprasnet` | **NÃO existe** | `comprasnet` | Coberto por `transparencia` e `pncp` |
| `senado_cpi_archive` | `download_senado_cpi_archive.py` | — | Orphan — sem pipeline correspondente |
| `senado_parlamentares` | `download_senado_parlamentares.py` | — | Orphan — sem pipeline correspondente |
| `tesouro_emendas` | `download_tesouro_emendas.py` | `tesouro_emendas` | Orphan — descartado temporariamente |

### BigQuery — Fora do Escopo (2 fontes adicionais)

| Fonte | Download Script | ETL Pipeline | Motivo |
|-------|-----------------|--------------|--------|
| `mides` | `download_mides.py` (existe) | `mides` | BigQuery — fora do escopo |
| `stf` | `download_stf.py` (existe) | `stf` | BigQuery — fora do escopo |

---

## Resumo por Status

| Categoria | Contagem | Fontes |
|-----------|----------|--------|
| ✅ Em Operação | 9 | tcu, transferegov, pgfn, ceaf, sanctions, cnpj, tse, transparencia, leniency |
| ✅ Prontas para Uso | 13 | siconfi, senado_cpis, querido_diario, **pncp**, cvm, cvm_funds, icij, opensanctions, ofac, un_sanctions, holdings, camara, cpgf |
| 🔵 On-Demand | 1 | datajud |
| 🗑️ Descartadas | 24 | pep_cgu, eu_sanctions, world_bank, camara_inquiries, tse_bens, tse_filiados, caged, cnpj_bq, senado, viagens, renuncias, bcb, siop, dou, bndes, ibama, datasus, inep, comprasnet, senado_cpi_archive, senado_parlamentares, tesouro_emendas, mides, stf |

**Total de fontes avaliadas:** 47
**Fontes ativas (✅ + 🔵):** 23
**Fontes descartadas (🗑️):** 24

---

## Notas Técnicas

### Scripts e ETL — Field Mapping Validation

Para todas as fontes diagnosticadas, verificamos que:
1. **Campos de output do script** → **campos de input do ETL** estão alinhados
2. **Diretório de dados** (`data/<source>/`) é consistente entre script e pipeline
3. **Parâmetros mínimos** (`--output-dir`, `--skip-existing`) estão presentes nos scripts

### Como Reativar uma Fonte Descartada

Para fontes com **URL quebrada**:
1. Investigar nova URL/estrutura da fonte oficial
2. Atualizar `download_<source>.py` com endpoint correto
3. Validar campos de output contra ETL pipeline
4. Mover de 🗑️ para ✅ ou ⚠️

Para fontes **sem script**:
1. Criar `download_<source>.py` seguindo padrão das fontes existentes
2. Validar output contra campos esperados pelo ETL em `etl/src/bracc_etl/pipelines/<source>.py`
3. Mover de 🗑️ para ✅
