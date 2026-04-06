# PRD — VigiaGov API
**Inteligência sobre Fornecedores Públicos com Narrativa LLM**

---

## 1. Visão Geral

### 1.1 Problema

Contratar ou estender crédito a uma empresa no Brasil exige consultar múltiplas fontes desconexas: Receita Federal para situação cadastral, Portal da Transparência para contratos e sanções, CEIS/CNEP para impedimentos. Nenhuma ferramenta comercial existente cruza essas fontes e entrega uma interpretação inteligível — apenas JSON bruto.

Empresas sancionadas continuam recebendo contratos públicos. Fintechs aprovam crédito B2B sem saber que o tomador vive de licitações irregulares. Auditores perdem horas consolidando dados manualmente.

### 1.2 Solução

VigiaGov é uma API B2B que cruza automaticamente dados da Receita Federal, Portal da Transparência (CGU) e listas de sanções (CEIS/CNEP), gerando um **score de risco explicável** e uma **narrativa em linguagem natural** sobre o perfil do fornecedor público consultado.

### 1.3 Posicionamento

> *"A única API que cruza situação cadastral, contratos públicos e sanções — e explica o que isso significa."*

---

## 2. Objetivos

### 2.1 Objetivo do MVP
Validar a qualidade dos cruzamentos de dados e da narrativa LLM em pesquisas reais de due diligence conduzidas pelo próprio time fundador (uso interno).

### 2.2 Objetivos de Produto (curto/médio prazo)
- Entregar score de risco explicável em menos de 5 segundos por consulta
- Gerar narrativa interpretativa em português sobre qualquer CNPJ consultado
- Construir base de dados cacheada para reduzir latência e custo de re-consulta
- Evoluir para alertas proativos via webhook

---

## 3. Público-Alvo

### 3.1 MVP (interno)
- **Alessandro / Tecker Consulting** — due diligence em forensics, imobiliário e investigações

### 3.2 V2 em diante (B2B)
| Segmento | Caso de uso principal |
|---|---|
| Fintechs de crédito B2B | Verificar se o tomador depende de contratos públicos irregulares |
| Plataformas de licitação | Validar idoneidade de fornecedores antes de habilitá-los |
| Escritórios de auditoria | Due diligence automatizada em massa |
| Legaltech / Compliance | Relatórios de risco para clientes corporativos |
| Prefeituras / TCEs | Verificação de fornecedores antes de assinatura de contrato |

---

## 4. Fontes de Dados

| Fonte | O que fornece | Acesso |
|---|---|---|
| Receita Federal (via BrasilAPI / ReceitaWS) | Dados cadastrais, QSA, situação | Aberto |
| Portal da Transparência — CGU | Contratos, convênios, licitações | Aberto |
| CEIS | Empresas inidôneas e suspensas | Aberto |
| CNEP | Acordos de leniência e punições | Aberto |
| CADIN (futuro) | Inadimplência com o governo federal | Restrito — fase futura |

---

## 5. Arquitetura Técnica

### 5.1 Stack

| Camada | Tecnologia |
|---|---|
| API | FastAPI (Python) |
| Banco de dados | Supabase (PostgreSQL) |
| Cache | Supabase (tabela com TTL por fonte) |
| LLM / Narrativa | OpenRouter (modelo configurável por chamada) |
| Autenticação futura | Supabase Auth + API Keys |
| Billing futuro | Stripe |

### 5.2 Fluxo de uma Consulta

```
Cliente → POST /empresa/{cnpj}
    │
    ├─ Cache hit? → retorna resultado armazenado
    │
    ├─ Cache miss →
    │     ├─ Receita Federal (cadastro + QSA)
    │     ├─ Transparência (contratos + licitações)
    │     ├─ CEIS/CNEP (sanções)
    │     └─ Agrega dados estruturados
    │
    ├─ Score engine → calcula score 0–100
    │
    ├─ OpenRouter → gera narrativa explicativa
    │
    ├─ Persiste no Supabase (cache + histórico)
    │
    └─ Retorna JSON enriquecido ao cliente
```

### 5.3 Estratégia de Cache

| Fonte | TTL sugerido |
|---|---|
| Dados cadastrais (Receita) | 7 dias |
| Contratos (Transparência) | 24 horas |
| Sanções (CEIS/CNEP) | 12 horas |
| Score + narrativa | Junto com a fonte mais curta |

---

## 6. Endpoints — MVP

```
GET  /empresa/{cnpj}              → perfil completo com score e narrativa
GET  /empresa/{cnpj}/cadastro     → dados cadastrais e QSA
GET  /empresa/{cnpj}/contratos    → histórico de contratos federais
GET  /empresa/{cnpj}/sancoes      → registros em CEIS e CNEP
GET  /empresa/{cnpj}/score        → score 0–100 com fatores detalhados
```

### Estrutura de resposta — `/empresa/{cnpj}`

```json
{
  "cnpj": "00.000.000/0001-00",
  "razao_social": "Exemplo Ltda",
  "situacao_cadastral": "ATIVA",
  "data_abertura": "2010-03-15",
  "socios": [...],
  "contratos_publicos": {
    "total_contratos": 14,
    "valor_total_brl": 4200000.00,
    "orgaos_contratantes": ["Ministério da Saúde", "FNDE"],
    "ultimo_contrato": "2024-08-01"
  },
  "sancoes": {
    "ceis": false,
    "cnep": true,
    "detalhes": [...]
  },
  "score": {
    "valor": 31,
    "classificacao": "ALTO RISCO",
    "fatores": [
      {"fator": "Registro ativo no CNEP", "impacto": "negativo", "peso": "alto"},
      {"fator": "Contratos federais ativos", "impacto": "positivo", "peso": "médio"},
      {"fator": "Situação cadastral regular", "impacto": "positivo", "peso": "alto"}
    ]
  },
  "narrativa": "A empresa Exemplo Ltda está ativa desde 2010 e acumulou R$ 4,2 milhões em contratos federais. No entanto, possui registro ativo no CNEP decorrente de acordo de leniência firmado em 2022, o que representa risco significativo para novas contratações públicas ou operações de crédito. Recomenda-se verificação aprofundada antes de qualquer relação contratual.",
  "fontes": ["Receita Federal", "Portal da Transparência", "CNEP"],
  "consultado_em": "2026-04-06T14:32:00Z",
  "cache": false
}
```

---

## 7. Score de Risco

### 7.1 Fatores e Pesos

| Fator | Direção | Peso |
|---|---|---|
| CNPJ inapto ou baixado | Negativo | 🔴 Alto |
| Registro no CEIS | Negativo | 🔴 Alto |
| Registro no CNEP | Negativo | 🔴 Alto |
| Sócios com empresas sancionadas | Negativo | 🔴 Alto |
| Contratos públicos recentes ativos | Positivo | 🟡 Médio |
| Tempo de existência > 5 anos | Positivo | 🟢 Baixo |
| Capital social declarado | Positivo | 🟢 Baixo |

### 7.2 Classificações

| Score | Classificação |
|---|---|
| 80–100 | ✅ BAIXO RISCO |
| 60–79 | 🟡 RISCO MODERADO |
| 40–59 | 🟠 RISCO ELEVADO |
| 0–39 | 🔴 ALTO RISCO |

---

## 8. Narrativa LLM

### 8.1 Modelo
OpenRouter — modelo padrão configurável (ex: `google/gemini-flash-1.5` para custo, `anthropic/claude-3.5-sonnet` para qualidade máxima).

### 8.2 Prompt Base (sistema)

```
Você é um analista especializado em compliance e contratos públicos brasileiros.
Com base nos dados estruturados abaixo, escreva um parágrafo objetivo em português 
sobre o perfil de risco da empresa, destacando os pontos mais relevantes para 
uma decisão de contratação ou crédito. Seja direto e conclusivo.
Não invente dados. Baseie-se apenas nas informações fornecidas.
```

### 8.3 Qualidade esperada
- Máximo 5 linhas
- Linguagem técnica mas acessível
- Sempre conclui com recomendação (contratar com ressalva / evitar / verificar)

---

## 9. Roadmap Incremental

### MVP — Uso Interno (atual)
- [ ] Scraper/client para Receita Federal (BrasilAPI)
- [ ] Client para Portal da Transparência (contratos)
- [ ] Client para CEIS e CNEP
- [ ] Agregador de dados + score engine simples
- [ ] Integração OpenRouter para narrativa
- [ ] Persistência no Supabase (cache + histórico)
- [ ] FastAPI com endpoints básicos
- [ ] Testes em consultas reais de due diligence

### V2 — API Privada Beta
- [ ] Autenticação por API Key (Supabase)
- [ ] Rate limiting por plano
- [ ] Endpoint de sócios com vínculos entre empresas
- [ ] Documentação OpenAPI (Swagger)
- [ ] Dashboard interno de uso e custos

### V3 — Produto Comercial
- [ ] Webhook de alertas por CNPJ monitorado
- [ ] Monitor contínuo (mudança de situação, nova sanção, novo contrato)
- [ ] Billing por volume (Stripe)
- [ ] Neo4j para grafos de sócios em N graus
- [ ] Endpoint de análise por município (concentração de contratos)
- [ ] White-label para prefeituras e TCEs

---

## 10. Riscos e Mitigações

| Risco | Mitigação |
|---|---|
| APIs gov instáveis ou com mudança de schema | Camada de adaptadores isolados por fonte; alertas de erro |
| Custo LLM escalando com volume | Cache de narrativa; modelo mais barato no OpenRouter para MVP |
| Concorrência dos grandes (Neoway, BigDataCorp) | Foco no nicho de contratos públicos + narrativa explicável |
| Dados desatualizados enganando o usuário | TTL conservador; exibir sempre `consultado_em` e `cache: true/false` |
| LGPD — dados de sócios (pessoas físicas) | Apenas dados já públicos (Receita + CGU); documentar base legal (legítimo interesse / transparência pública) |

---

## 11. Métricas de Sucesso

### MVP
- Qualidade subjetiva da narrativa em 20 consultas reais
- Latência < 5s em cache miss
- Score coerente com avaliação manual do analista

### V2+
- Tempo médio de resposta < 2s (com cache)
- Taxa de cache hit > 70%
- NPS de clientes beta > 40

---

## 12. Interface Web

### 12.1 Visão Geral
Interface minimalista de uso interno (MVP), single-page, sem framework pesado. Serve como camada visual sobre a API para acelerar pesquisas de due diligence sem necessidade de cliente REST manual.

### 12.2 Estética e Tom
- **Dark**, fundo quase preto (`#0a0a0a`), tipografia monospace para dados técnicos
- Minimalismo utilitário — sem ornamentos, toda atenção vai para o dado
- Paleta restrita: fundo escuro + branco frio + acento âmbar (`#f5a623`) para alertas e score

### 12.3 Componentes

#### Input
- Campo CNPJ com máscara automática (`00.000.000/0000-00`)
- Validação de dígito verificador no frontend
- Botão "Consultar" com estado de loading animado

#### Output — Resultado da Consulta
| Componente | Descrição |
|---|---|
| **Score Visual** | Gauge ou barra colorida (verde/amarelo/laranja/vermelho) com número e classificação |
| **Narrativa** | Parágrafo LLM em destaque, tipografia diferenciada |
| **Seção: Cadastro** | Colapsável — razão social, situação, CNAE, sócios, data de abertura |
| **Seção: Contratos** | Colapsável — total, valor, órgãos, último contrato |
| **Seção: Sanções** | Colapsável — CEIS/CNEP com badges visuais |
| **Seção: Fatores do Score** | Colapsável — lista de fatores com impacto e peso |

#### Exportação
- **JSON** — dump completo da resposta da API (botão "Exportar JSON")
- **PDF** — relatório formatado com logo, data, dados estruturados e narrativa (botão "Exportar PDF")

#### Painel de Status das APIs
- Seção fixa no rodapé ou sidebar
- Verifica disponibilidade de cada fonte em tempo real
- Exibe latência da última chamada bem-sucedida

### 12.4 Stack da Interface
- HTML5 + CSS3 + JavaScript vanilla (single-file)
- Exportação PDF via `jsPDF`
- Sem dependências de framework — portabilidade máxima

---

## 13. API Health Check

### 13.1 Objetivo
Monitorar em tempo real a disponibilidade e latência das fontes de dados externas utilizadas pela VigiaGov API, permitindo diagnóstico rápido de falhas antes de atribuir erros à própria API.

### 13.2 Endpoint

```
GET /status
```

### 13.3 Estrutura de Resposta

```json
{
  "status_geral": "DEGRADADO",
  "verificado_em": "2026-04-06T14:32:00Z",
  "fontes": [
    {
      "nome": "Receita Federal (BrasilAPI)",
      "url_probe": "https://brasilapi.com.br/api/cnpj/v1/00000000000191",
      "status": "OK",
      "latencia_ms": 312,
      "ultimo_sucesso": "2026-04-06T14:32:00Z"
    },
    {
      "nome": "Portal da Transparência",
      "url_probe": "https://api.portaldatransparencia.gov.br/api-de-dados/contratos",
      "status": "OK",
      "latencia_ms": 890,
      "ultimo_sucesso": "2026-04-06T14:31:45Z"
    },
    {
      "nome": "CEIS (CGU)",
      "url_probe": "https://api.portaldatransparencia.gov.br/api-de-dados/ceis",
      "status": "LENTO",
      "latencia_ms": 3200,
      "ultimo_sucesso": "2026-04-06T14:30:10Z"
    },
    {
      "nome": "CNEP (CGU)",
      "url_probe": "https://api.portaldatransparencia.gov.br/api-de-dados/cnep",
      "status": "OK",
      "latencia_ms": 540,
      "ultimo_sucesso": "2026-04-06T14:32:00Z"
    },
    {
      "nome": "OpenRouter (LLM)",
      "url_probe": "https://openrouter.ai/api/v1/models",
      "status": "OK",
      "latencia_ms": 180,
      "ultimo_sucesso": "2026-04-06T14:32:00Z"
    }
  ]
}
```

### 13.4 Status possíveis por fonte

| Status | Critério |
|---|---|
| `OK` | Resposta < 1500ms |
| `LENTO` | Resposta entre 1500ms e 5000ms |
| `INDISPONÍVEL` | Timeout > 5000ms ou erro HTTP |

### 13.5 Status Geral

| Valor | Critério |
|---|---|
| `OPERACIONAL` | Todas as fontes OK |
| `DEGRADADO` | Pelo menos uma fonte LENTA |
| `PARCIAL` | Pelo menos uma fonte INDISPONÍVEL |
| `CRÍTICO` | Receita Federal ou OpenRouter INDISPONÍVEIS |

### 13.6 Comportamento na Interface
- Painel de status no rodapé da interface web, atualizado a cada 60 segundos
- Indicador visual por fonte: ponto verde / amarelo / vermelho + latência em ms
- Status geral em badge no topo da página
- Ao detectar fonte INDISPONÍVEL, exibe aviso contextual ao tentar consultar

---

## 14. Fora do Escopo (por ora)

- Interface web ou mobile
- Dados de pessoa física (CPF)
- Integração com bureaus de crédito privados (Serasa, SPC)
- Análise de balanços financeiros
- Processos judiciais (Escavador, Jusbrasil)

---

*Documento vivo — versão 0.1 | Abril 2026 | Tecker Consulting*
