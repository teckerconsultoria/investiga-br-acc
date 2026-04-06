"""Persist CNPJa API results into Neo4j so they integrate with the graph.

When a CNPJ is looked up via the CNPJa API and is not yet in the graph, we:
1. MERGE a Company node with the returned data.
2. MERGE Person / Partner nodes for each partner (member).
3. MERGE SOCIO_DE relationships between partners and the company.

This means the entity, once fetched, becomes part of the graph and can be
explored via connections, patterns, and investigations — just like bulk-loaded
data, but acquired on demand.

A ``cnpja_fetched_at`` property is set on the Company node so we can honour
``CNPJA_CACHE_TTL_DAYS`` and re-fetch stale entries.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bracc.config import settings

if TYPE_CHECKING:
    from neo4j import AsyncSession

    from bracc.services.cnpja_client import CnpjaCompanyData, CnpjaMember

logger = logging.getLogger(__name__)


def _format_cnpj(digits: str) -> str:
    d = re.sub(r"\D", "", digits).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _format_cpf(digits: str) -> str:
    d = re.sub(r"\D", "", digits).zfill(11)
    return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"


def _is_valid_cpf(tax_id: str | None) -> bool:
    if not tax_id:
        return False
    digits = re.sub(r"\D", "", tax_id)
    return len(digits) == 11 and digits != "0" * 11


def _is_valid_cnpj(tax_id: str | None) -> bool:
    if not tax_id:
        return False
    digits = re.sub(r"\D", "", tax_id)
    return len(digits) == 14 and digits != "0" * 14


# ---------------------------------------------------------------------------
# Check cache freshness
# ---------------------------------------------------------------------------

_CACHE_CHECK_CYPHER = """
MATCH (c:Company)
WHERE c.cnpj = $cnpj OR c.cnpj = $cnpj_formatted
RETURN c.cnpja_fetched_at AS fetched_at
LIMIT 1
"""


async def is_cache_fresh(session: AsyncSession, cnpj_digits: str) -> bool:
    """Return True if we already have a recent CNPJa-sourced Company node."""
    formatted = _format_cnpj(cnpj_digits)
    result = await session.run(
        _CACHE_CHECK_CYPHER,
        {"cnpj": cnpj_digits, "cnpj_formatted": formatted},
    )
    record = await result.single()
    if record is None or record["fetched_at"] is None:
        return False
    fetched_at = record["fetched_at"]
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)
    age_days = (datetime.now(tz=UTC) - fetched_at.replace(tzinfo=UTC)).days
    return age_days < settings.cnpja_cache_ttl_days


# ---------------------------------------------------------------------------
# Upsert company node
# ---------------------------------------------------------------------------

_MERGE_COMPANY_CYPHER = """
MERGE (c:Company {cnpj: $cnpj})
SET c.razao_social          = $razao_social,
    c.nome_fantasia         = $nome_fantasia,
    c.natureza_juridica     = $natureza_juridica,
    c.cnae_principal        = $cnae_principal,
    c.capital_social        = $capital_social,
    c.porte_empresa         = $porte_empresa,
    c.uf                    = $uf,
    c.municipio             = $municipio,
    c.situacao_cadastral    = $situacao_cadastral,
    c.data_abertura         = $data_abertura,
    c.source                = $source,
    c.cnpja_fetched_at      = datetime($fetched_at)
RETURN c, elementId(c) AS entity_id, labels(c) AS entity_labels
"""


async def upsert_company(
    session: AsyncSession, data: CnpjaCompanyData
) -> dict:
    """Create or update a Company node from CNPJa data. Returns the record."""
    cnpj_formatted = _format_cnpj(data.cnpj)
    now_iso = datetime.now(tz=UTC).isoformat()

    cnae_str = ""
    if data.cnae_principal.id:
        cnae_str = f"{data.cnae_principal.id} - {data.cnae_principal.text}"

    natureza_str = ""
    if data.natureza_juridica_id:
        natureza_str = f"{data.natureza_juridica_id} - {data.natureza_juridica_text}"

    result = await session.run(
        _MERGE_COMPANY_CYPHER,
        {
            "cnpj": cnpj_formatted,
            "razao_social": data.razao_social,
            "nome_fantasia": data.nome_fantasia or "",
            "natureza_juridica": natureza_str,
            "cnae_principal": cnae_str,
            "capital_social": data.capital_social,
            "porte_empresa": data.porte_text or "",
            "uf": data.address.state,
            "municipio": data.address.municipality,
            "situacao_cadastral": data.situacao_cadastral,
            "data_abertura": data.data_abertura or "",
            "source": "cnpja",
            "fetched_at": now_iso,
        },
    )
    record = await result.single()
    logger.info("Upserted Company %s (%s)", cnpj_formatted, data.razao_social)
    return record


# ---------------------------------------------------------------------------
# Upsert members (partners)
# ---------------------------------------------------------------------------

_MERGE_PERSON_SOCIO_CYPHER = """
MERGE (p:Person {cpf: $cpf})
ON CREATE SET p.name = $name, p.source = $source
MERGE (c:Company {cnpj: $cnpj})
MERGE (p)-[r:SOCIO_DE]->(c)
SET r.qualificacao  = $qualificacao,
    r.data_entrada  = $data_entrada,
    r.source        = $source
"""

_MERGE_COMPANY_SOCIO_CYPHER = """
MERGE (pj:Company {cnpj: $partner_cnpj})
ON CREATE SET pj.razao_social = $name, pj.source = $source
MERGE (c:Company {cnpj: $cnpj})
MERGE (pj)-[r:SOCIO_DE]->(c)
SET r.qualificacao  = $qualificacao,
    r.data_entrada  = $data_entrada,
    r.source        = $source
"""

_MERGE_PARTNER_SOCIO_CYPHER = """
MERGE (pt:Partner {partner_id: $partner_id})
ON CREATE SET pt.name = $name, pt.source = $source
MERGE (c:Company {cnpj: $cnpj})
MERGE (pt)-[r:SOCIO_DE]->(c)
SET r.qualificacao  = $qualificacao,
    r.data_entrada  = $data_entrada,
    r.source        = $source
"""


async def upsert_members(
    session: AsyncSession, company_cnpj_formatted: str, members: list[CnpjaMember]
) -> int:
    """Create partner nodes and SOCIO_DE relationships. Returns count created."""
    count = 0
    for m in members:
        qualificacao = m.role_text or ""
        data_entrada = m.since or ""
        source = "cnpja"

        if m.person_type == "LEGAL" and _is_valid_cnpj(m.person_tax_id):
            # Company → Company partnership
            partner_cnpj = _format_cnpj(re.sub(r"\D", "", m.person_tax_id or ""))
            await session.run(
                _MERGE_COMPANY_SOCIO_CYPHER,
                {
                    "partner_cnpj": partner_cnpj,
                    "name": m.person_name,
                    "cnpj": company_cnpj_formatted,
                    "qualificacao": qualificacao,
                    "data_entrada": data_entrada,
                    "source": source,
                },
            )
        elif m.person_type == "NATURAL" and _is_valid_cpf(m.person_tax_id):
            # Person (strong CPF) → Company
            cpf_formatted = _format_cpf(re.sub(r"\D", "", m.person_tax_id or ""))
            await session.run(
                _MERGE_PERSON_SOCIO_CYPHER,
                {
                    "cpf": cpf_formatted,
                    "name": m.person_name,
                    "cnpj": company_cnpj_formatted,
                    "qualificacao": qualificacao,
                    "data_entrada": data_entrada,
                    "source": source,
                },
            )
        else:
            # Partner with no valid document
            partner_id = f"cnpja:{m.person_name}:{m.person_tax_id or 'unknown'}"
            await session.run(
                _MERGE_PARTNER_SOCIO_CYPHER,
                {
                    "partner_id": partner_id,
                    "name": m.person_name,
                    "cnpj": company_cnpj_formatted,
                    "qualificacao": qualificacao,
                    "data_entrada": data_entrada,
                    "source": source,
                },
            )
        count += 1

    logger.info("Upserted %d members for %s", count, company_cnpj_formatted)
    return count


# ---------------------------------------------------------------------------
# High-level: fetch + persist
# ---------------------------------------------------------------------------


async def fetch_and_cache(
    session: AsyncSession, cnpj_digits: str
) -> dict | None:
    """Fetch from CNPJa API if not cached, persist to Neo4j, return the Company record.

    Returns None if CNPJa is disabled or the CNPJ was not found.
    """
    from bracc.services.cnpja_client import CnpjaApiError, get_cnpja_client

    if not settings.cnpja_enabled or not settings.cnpja_api_key:
        return None

    # Check if we already have a fresh cached version
    if await is_cache_fresh(session, cnpj_digits):
        logger.debug("CNPJa cache still fresh for %s", cnpj_digits)
        return None  # caller should use existing Neo4j data

    client = get_cnpja_client()
    try:
        data = await client.fetch_office(cnpj_digits)
    except CnpjaApiError as exc:
        if exc.status_code == 404:
            logger.info("CNPJ %s not found in CNPJa", cnpj_digits)
            return None
        raise

    record = await upsert_company(session, data)
    cnpj_formatted = _format_cnpj(data.cnpj)
    await upsert_members(session, cnpj_formatted, data.members)

    return record
