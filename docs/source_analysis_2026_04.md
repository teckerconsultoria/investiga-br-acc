# Source-by-Source Analysis Report

**Generated:** 2026-04-05
**Scope:** All data sources in investiga-br-acc pipeline
**Authentication Status:**
- Portal da Transparência API: `chave-api-dados: <PORTAL_API_KEY>` — 2/16 working (CEAF, CEIS/CNEP), 14/16 returning 403
- dados.gov.br API: `chave-api-dados-abertos: <JWT>` — Working, but mostly metadata catalog pointing to external portals
- Other APIs: Câmara, Senado, PNCP, SICONFI, Querido Diário — public, no auth required

---

## Executive Summary

| Category | Count | Status |
|----------|-------|--------|
| ✅ Implemented & Healthy | 33 | Working, loaded |
| ⚠️ Implemented but Partial/Stale | 9 | Needs fixing |
| 🔴 Blocked External | 1 | DataJud (credentials) |
| 📋 Not Built (discovered) | 60+ | Future candidates |
| ❌ Out of Scope (BigQuery) | 7 | GCP/BigQuery sources |

### Key Findings

1. **dados.gov.br** é um catálogo de metadados, não fonte primária. A maioria dos datasets aponta para portais externos (dados.antt.gov.br, dados.transportes.gov.br, etc). Única exceção: API de Transferências Constitucionais do Tesouro.

2. **Portal da Transparência API** está 87% bloqueado (403). Das 16 fontes potenciais, apenas CEAF e CEIS/CNEP funcionam. Fontes bloqueadas: cpgf, pep, sanções, leniências, renúncias-fiscais, siop, despesas, empenhos, repasses, pagamentos, etc.

3. **Fontes stale/partial** precisam de atenção imediata: comprasnet, pncp, caged, siop, siconfi, camara_inquiries, senado_cpis, querido_diario, datajud.

4. **Fontes sem download script** (manual download): tcu, transferegov, pgfn — precisam de automação.

---

## 1. FONTES IMPLEMENTADAS & SAUDÁVEIS (33 fontes)

### 1.1 File Download — Funcionando (25 fontes)

| # | Source | Priority | Category | Download Method | Volume | API Available | Migration Worth It? | Notes |
|---|--------|----------|----------|-----------------|--------|---------------|---------------------|-------|
| 1 | **cnpj** | P0 | identity | Nextcloud/HTTP ZIPs | ~85GB | No | No | Stable URLs, working well |
| 2 | **tse** | P0 | electoral | HTTP ZIPs (dadosabertos.tse.jus.br) | Large | No | No | Biennial, core data |
| 3 | **transparencia** | P0 | contracts | Monthly ZIPs from Portal | Large | ❌ 403 (expense endpoints) | No | Contracts core, keep file-based |
| 4 | **bndes** | P1 | finance | HTTP from bndes.gov.br | Medium | No | No | Dev bank loans, stable |
| 5 | **pgfn** | P0 | fiscal | Manual (no script) | Large | No | No | Needs download script |
| 6 | **ibama** | P1 | environment | HTTP from ibama.gov.br | Medium | No | No | Environmental embargoes |
| 7 | **tcu** | P1 | audit | Manual (no script) | Small | No | No | Needs download script |
| 8 | **transferegov** | P0 | transfers | Manual (no script) | Medium | No | No | Needs download script |
| 9 | **inep** | P2 | education | HTTP from inep.gov.br | Large | No | No | Annual education census |
| 10 | **datasus** | P1 | health | HTTP from opendatasus.saude.gov.br | Large | No | No | Health establishments |
| 11 | **icij** | P1 | offshore | HTTP from offshoreleaks.icij.org | Small | No | No | Offshore leaks, yearly |
| 12 | **opensanctions** | P1 | sanctions | HTTP from opensanctions.org | Medium | No | No | Global PEP matching |
| 13 | **cvm** | P1 | market | HTTP from dados.cvm.gov.br | Medium | No | No | Securities proceedings |
| 14 | **cvm_funds** | P1 | market | HTTP from dados.cvm.gov.br | Medium | No | No | Investment fund registry |
| 15 | **cepim** | P1 | integrity | Monthly ZIPs from Portal | Small | ✅ Untested | Low | Small dataset, file works |
| 16 | **cpgf** | P2 | spending | Monthly ZIPs from Portal | Medium | ❌ 403 | No | LGPD masked CPFs |
| 17 | **leniency** | P0 | integrity | Single ZIP from Portal | Small | ❌ 403 | No | 112 records, high signal |
| 18 | **ofac** | P1 | sanctions | HTTP from treasury.gov | Small | No | No | US sanctions |
| 19 | **eu_sanctions** | P1 | sanctions | HTTP from data.europa.eu | Small | No | No | EU sanctions |
| 20 | **un_sanctions** | P1 | sanctions | HTTP from sancitions.un.org (XML) | Small | No | No | UN sanctions |
| 21 | **world_bank** | P1 | sanctions | HTTP from worldbank.org | Small | No | No | Debarred firms |
| 22 | **holdings** | P1 | ownership | HTTP from brasil.io | Small | No | No | Derived from CNPJ |
| 23 | **viagens** | P2 | spending | Yearly ZIPs from Portal | Medium | ✅ Untested | Medium | LGPD masked |
| 24 | **renuncias** | P1 | fiscal | Yearly ZIPs from Portal | Medium | ❌ 403 | No | R$414B+ tax waivers |
| 25 | **tse_bens** | P1 | electoral | HTTP from dadosabertos.tse.jus.br | Large | No | No | Candidate assets |
| 26 | **tse_filiados** | P1 | electoral | HTTP from dadosabertos.tse.jus.br | Large | No | No | Party memberships |
| 27 | **bcb** | P1 | finance | HTTP from dadosabertos.bcb.gov.br | Small | No | No | BCB penalties |
| 28 | **pep_cgu** | P1 | integrity | Single ZIP from Portal | Small | ❌ 403 | No | 133.8K PEP records |
| 29 | **sanctions** | P0 | sanctions | Already migrated to API | 23.8K | ✅ Working | Done | CEIS/CNEP |
| 30 | **ceaf** | P1 | integrity | Already migrated to API | 4.1K | ✅ Working | Done | Expelled servants |

### 1.2 API — Funcionando (5 fontes)

| # | Source | Priority | Category | API URL | Auth | Status | Notes |
|---|--------|----------|----------|---------|------|--------|-------|
| 1 | **camara** | P1 | legislative | dadosabertos.camara.leg.br/api/v2 | None | ✅ Working | Deputy CEAP expenses |
| 2 | **senado** | P1 | legislative | dadosabertos.senado.leg.br | None | ✅ Working | Senator CEAPS expenses |
| 3 | **camara_inquiries** | P0 | legislative | dadosabertos.camara.leg.br/api/v2 | None | ⚠️ Partial | Sessions still low |
| 4 | **senado_cpis** | P0 | legislative | dadosabertos.senado.leg.br | None | ⚠️ Partial | Needs richer data |
| 5 | **querido_diario** | P1 | municipal | api.queridodiario.ok.org.br | None | ⚠️ Partial | Text availability gap |

### 1.3 BigQuery — Fora do Escopo (7 fontes)

| # | Source | Priority | Category | BigQuery Table | Status | Notes |
|---|--------|----------|----------|----------------|--------|-------|
| 1 | **caged** | P1 | labor | br_me_caged | ⚠️ Stale | OUT OF SCOPE |
| 2 | **cnpj_bq** | P0 | identity | br_me_cnpj | — | OUT OF SCOPE |
| 3 | **mides** | P0 | municipal | world_wb_mides | ✅ Working | OUT OF SCOPE |
| 4 | **stf** | P1 | judiciary | br_stf_corte_aberta | ✅ Working | OUT OF SCOPE |
| 5 | **tse_bens** | P1 | electoral | br_tse_eleicoes.bens_candidato | — | OUT OF SCOPE |
| 6 | **tse_filiados** | P1 | electoral | br_tse_eleicoes.filiacao_partidaria | — | OUT OF SCOPE |
| 7 | **dou** | P0 | gazette | br_dou (via BigQuery) | ✅ Working | OUT OF SCOPE |
| 8 | **rais** | P1 | labor | br_me_rais | ✅ Working | OUT OF SCOPE |

**Nota:** Fontes BigQuery foram marcadas como FORA DO ESCOPO por decisão do projeto. Algumas têm implementações alternativas via file download (tse_bens, tse_filiados, rais).

---

## 2. FONTES STALE/PARCIAIS — PRECISAM DE FIX (9 fontes)

### 2.1 Análise Detalhada

| # | Source | Status | Issue Root Cause | Fix Required | Complexity | API Available |
|---|--------|--------|-------------------|--------------|------------|---------------|
| 1 | **comprasnet** | 🔴 Stale | Needs freshness backfill. Dataset em dados.gov.br pode estar desatualizado | Re-download from dados.gov.br or direct ComprasNet API | Medium | Maybe |
| 2 | **pncp** | 🔴 Stale | API paginates by month, downloading 35+ files (2021-2026). Still running | Fix pagination to complete faster, add incremental updates | Medium | ✅ Yes |
| 3 | **caged** | 🔴 Stale | Pipeline rewritten as aggregate LaborStats. Needs re-download from PDET FTP | Update download script for new FTP URL | Low | No |
| 4 | **siop** | ⚠️ Partial | Author linkage limited. API returns 403 | Keep file-based, improve data enrichment | Low | ❌ 403 |
| 5 | **siconfi** | ⚠️ Partial | No CNPJ direct links. Downloading 5,570 municípios × 5 anos | Keep downloading, add post-processing for CNPJ linking | Medium | ✅ Yes |
| 6 | **camara_inquiries** | ⚠️ Partial | Sessions data still low volume | API is working, may need to query additional endpoints | Low | ✅ Yes |
| 7 | **senado_cpis** | ⚠️ Partial | Needs richer sessions and requirements data | API is working, expand query scope | Low | ✅ Yes |
| 8 | **querido_diario** | ⚠️ Partial | Text availability gap in API responses | API is working, handle missing text gracefully | Low | ✅ Yes |
| 9 | **datajud** | 🔴 Blocked | Credentials not fully operational in prod | Wait for API key access, or use alternative | High | ✅ Yes (needs key) |

### 2.2 Prioridade de Fix

| Prioridade | Sources | Ação Recomendada |
|------------|---------|-------------------|
| **URGENTE** | pncp, comprasnet | Fix pagination e freshness — dados de contratos são core para o grafo |
| **ALTA** | caged, siconfi | Atualizar scripts de download — dados importantes para labor e fiscal |
| **MÉDIA** | siop, camara_inquiries, senado_cpis, querido_diario | Melhorar queries e data enrichment — API já funciona |
| **BAIXA** | datajud | Aguardar credenciais operacionais |

---

## 3. FONTES SEM DOWNLOAD SCRIPT (3 fontes)

Estas fontes têm pipelines ETL implementados mas **não têm scripts de download automatizados** — requerem download manual.

| # | Source | Priority | Expected Location | Download From | Script Needed |
|---|--------|----------|-------------------|---------------|---------------|
| 1 | **tcu** | P1 | `data/tcu/*.csv` | portal.tcu.gov.br/ords/f?p=INIDONEAS | `download_tcu.py` |
| 2 | **transferegov** | P0 | `data/transferegov/*.csv` | transferegov.sistema.gov.br/portal/download-de-dados | `download_transferegov.py` |
| 3 | **pgfn** | P0 | `data/pgfn/arquivo_lai_SIDA_*.csv` | regularize.pgfn.gov.br/dados-abertos | `download_pgfn.py` |

**Recomendação:** Criar scripts de download para estas 3 fontes — todas são P0/P1 e críticas para o grafo.

---

## 4. FONTES DO CATÁLOGO dados.gov.br (Análise)

### 4.1 Finding Principal

**dados.gov.br é um catálogo de metadados, não uma fonte primária de dados.** Dos 300 datasets catalogados:
- 299 apontam para portais externos (dados.antt.gov.br, dados.transportes.gov.br, etc.)
- 1 tem API direta: **Tesouro Transferências Constitucionais** (`sisweb.tesouro.gov.br`)

### 4.2 Datasets Relevantes Encontrados

| Dataset | Org | Formats | Updated | Direct URL | Use for Project |
|---------|-----|---------|---------|------------|-----------------|
| acidentes-quilometro-rodovias | ANTT | CSV | 2026-03 | dados.antt.gov.br | LOW — highway data |
| acervo-de-dados-tecnicos | ANP | CSV (26 resources) | 2026-03 | dados.gov.br | MEDIUM — oil/gas |
| acompanhamento-de-contratos | DNIT | CSV | 2025-02 | servicos.dnit.gov.br | MEDIUM — contracts |
| api-de-transferencias-constitucionais | Tesouro | **API** | 2026-02 | sisweb.tesouro.gov.br | HIGH — transfers |
| termo-de-ajustamento-tac | ANATEL | ZIP(CSV) | 2026-04 | anatel.gov.br | LOW — settlements |

### 4.3 Agências NÃO Representadas no Catálogo

As seguintes fontes críticas do projeto **não estão** no catálogo dados.gov.br:
- Receita Federal (CNPJ)
- CVM
- SICONFI/Tesouro (além do dataset de transferências)
- PNCP
- SENADO/CÂMARA
- STJ/CNJ/CARF
- IBAMA/ICMBio
- ANVISA, ANAC, ANTAQ, SUSEP, ANEEL, ANM

**Conclusão:** O projeto deve continuar usando fontes diretas de cada agência.

---

## 5. MAPA DE AUTENTICAÇÃO

| Portal | Header | Token/Key | Status | Endpoints Working |
|--------|--------|-----------|--------|-------------------|
| Portal da Transparência | `chave-api-dados` | `PORTAL_API_KEY` | ⚠️ Parcial | CEAF, CEIS, CNEP |
| Portal da Transparência | `chave-api-dados` | `PORTAL_API_KEY` | ❌ 403 | pep, cpgf, sanções, leniências, renúncias, siop, despesas, empenhos, repasses, pagamentos, beneficio-remuneração, auxilio-alimentação, bolsas-estudo, cotas-parlamentares, senadores, deputados |
| dados.gov.br | `chave-api-dados-abertos` | JWT Token | ✅ Working | Metadata catalog only |
| PNCP | None | — | ✅ Public | API works |
| Câmara | None | — | ✅ Public | API works |
| Senado | None | — | ✅ Public | API works |
| SICONFI | None | — | ✅ Public | API works |
| Querido Diário | None | — | ✅ Public | API works |
| DataJud | API Key | `DATAJUD_API_KEY` | 🔴 Blocked | Needs credentials |

---

## 6. ROADMAP PRIORIZADO

### 6.1 Quick Wins (1-2 dias cada)

| # | Ação | Source | Impact | Esforço |
|---|------|--------|--------|---------|
| 1 | Criar `download_tcu.py` | tcu | HIGH — 45K sanctions | Low |
| 2 | Criar `download_transferegov.py` | transferegov | HIGH — 71K amendments | Low |
| 3 | Criar `download_pgfn.py` | pgfn | HIGH — 24M tax debt records | Low |
| 4 | Testar API `/viagens` endpoint | viagens | MEDIUM — simplify yearly downloads | Low |
| 5 | Testar API `/cepim` endpoint | cepim | LOW — small dataset | Low |

### 6.2 Medium Effort (1 semana cada)

| # | Ação | Source | Impact | Esforço |
|---|------|--------|--------|---------|
| 1 | Fix PNCP pagination e incremental updates | pncp | HIGH — contracts freshness | Medium |
| 2 | Fix ComprasNet freshness backfill | comprasnet | HIGH — 1.08M contracts | Medium |
| 3 | Update CAGED FTP download | caged | MEDIUM — labor movements | Medium |
| 4 | Fix SICONFI CNPJ linking | siconfi | MEDIUM — municipal finance | Medium |
| 5 | Improve camara_inquiries data volume | camara_inquiries | MEDIUM — legislative coverage | Medium |
| 6 | Improve senado_cpis data richness | senado_cpis | MEDIUM — CPI investigations | Medium |

### 6.3 Long-Term (2+ semanas)

| # | Ação | Source | Impact | Esforço |
|---|------|--------|--------|---------|
| 1 | Negotiate DataJud API credentials | datajud | VERY HIGH — all court data | High |
| 2 | Build regulatory agency pipelines (11 agencies) | anp, aneel, anm, etc. | MEDIUM — regulatory data | High |
| 3 | Build state TCE pipelines (27 states) | tce_sp, tce_rj, etc. | MEDIUM — state audit | High |
| 4 | Resolve Portal API 403 issues | 14 blocked endpoints | HIGH — if access granted | High |

### 6.4 Não Priorizar (Baixo ROI)

| Source | Razão |
|--------|-------|
| pep_cgu → API | 403, file funciona, dataset pequeno |
| cpgf → API | 403, file funciona, CPFs mascarados |
| leniency → API | 403, file funciona, 112 records |
| renuncias → API | 403, file funciona |
| dados.gov.br como fonte primária | É catálogo de metadados, não fonte de dados |

---

## 7. RECOMENDAÇÕES ESTRATÉGICAS

### 7.1 Imediatas (Esta Semana)
1. Criar 3 scripts de download faltantes (tcu, transferegov, pgfn)
2. Fix PNCP pagination
3. Fix ComprasNet freshness

### 7.2 Curto Prazo (Próximas 2 Semanas)
1. Fix CAGED FTP download
2. Fix SICONFI CNPJ linking
3. Testar endpoints de API não-bloqueados (viagens, cepim)
4. Melhorar camara_inquiries e senado_cpis

### 7.3 Médio Prazo (1-2 Meses)
1. Negotiate DataJud credentials
2. Avaliar se vale a pena construir pipelines para agências regulatórias do dados.gov.br
3. Planejar expansão para fontes estaduais (TCEs)

### 7.4 Decisões de Arquitetura
1. **Manter file-based** para fontes Portal da Transparência bloqueadas por 403
2. **Não investir** em dados.gov.br como fonte primária (é metadado)
3. **Priorizar** fixes de fontes stale sobre construção de fontes novas
4. **Aguardar** credenciais DataJud antes de investir tempo na integração

---

## 8. FONTES FORA DO ESCOPO (Decisões Anteriores)

| Source | Razão |
|--------|-------|
| caged (BigQuery) | GCP/BigQuery fora do escopo |
| cnpj_bq (BigQuery) | GCP/BigQuery fora do escopo |
| mides (BigQuery) | GCP/BigQuery fora do escopo |
| stf (BigQuery) | GCP/BigQuery fora do escopo |
| tse_bens (BigQuery) | GCP/BigQuery fora do escopo (tem alternativa file) |
| tse_filiados (BigQuery) | GCP/BigQuery fora do escopo (tem alternativa file) |
| camara_inquiries (BigQuery) | GCP/BigQuery fora do escopo (tem alternativa API) |
| datajud | API key pública, on-demand, NÃO é prioridade |
| pncp, querido_diario, senado_cpis, senado_parlamentares, siconfi | Serão avaliados posteriormente (agora em análise) |

---

*Fim do relatório. Próximo passo: implementar roadmap priorizado.*
