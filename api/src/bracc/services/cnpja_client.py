"""Client for the CNPJa.com commercial API.

Replaces the bulk CNPJ database with on-demand lookups.
Docs: https://cnpja.com/en/api/reference

Endpoint used:
  GET /office/{taxId}   — returns establishment + company + members (partners)

Authentication:
  Header  Authorization: <api_key>
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

from bracc.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response data classes (mirrors relevant CNPJa JSON structure)
# ---------------------------------------------------------------------------
@dataclass
class CnpjaMember:
    """A partner / shareholder returned by CNPJa."""

    since: str | None = None
    role_id: int | None = None
    role_text: str | None = None
    person_id: str | None = None
    person_name: str = ""
    person_type: str | None = None  # "NATURAL" or "LEGAL"
    person_tax_id: str | None = None  # CPF or CNPJ
    person_age: str | None = None


@dataclass
class CnpjaActivity:
    id: int = 0
    text: str = ""


@dataclass
class CnpjaAddress:
    municipality: str = ""
    state: str = ""
    street: str = ""
    number: str = ""
    district: str = ""
    zip: str = ""
    city_ibge: int | None = None
    details: str = ""


@dataclass
class CnpjaCompanyData:
    """Parsed result of a CNPJa /office response."""

    cnpj: str = ""
    razao_social: str = ""
    nome_fantasia: str = ""
    natureza_juridica_id: int | None = None
    natureza_juridica_text: str = ""
    capital_social: float = 0.0
    porte_id: int | None = None
    porte_text: str = ""
    situacao_cadastral: str = ""
    data_situacao_cadastral: str = ""
    data_abertura: str = ""
    cnae_principal: CnpjaActivity = field(default_factory=CnpjaActivity)
    cnaes_secundarios: list[CnpjaActivity] = field(default_factory=list)
    address: CnpjaAddress = field(default_factory=CnpjaAddress)
    members: list[CnpjaMember] = field(default_factory=list)
    updated: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# JSON → dataclass mapping
# ---------------------------------------------------------------------------

def _parse_member(m: dict[str, Any]) -> CnpjaMember:
    role = m.get("role") or {}
    person = m.get("person") or {}
    return CnpjaMember(
        since=m.get("since"),
        role_id=role.get("id"),
        role_text=role.get("text", ""),
        person_id=str(person.get("id", "")),
        person_name=person.get("name", ""),
        person_type=person.get("type"),
        person_tax_id=person.get("taxId"),
        person_age=person.get("age"),
    )


def _parse_activity(a: dict[str, Any]) -> CnpjaActivity:
    return CnpjaActivity(id=a.get("id", 0), text=a.get("text", ""))


def _parse_address(addr: dict[str, Any]) -> CnpjaAddress:
    municipality_obj = addr.get("municipality") or {}
    state_obj = addr.get("state") or {}
    return CnpjaAddress(
        municipality=municipality_obj.get("name", addr.get("municipality", "")),
        state=state_obj.get("acronym", addr.get("state", "")),
        street=addr.get("street", ""),
        number=addr.get("number", ""),
        district=addr.get("district", ""),
        zip=addr.get("zip", ""),
        city_ibge=municipality_obj.get("ibge"),
        details=addr.get("details", ""),
    )


def parse_cnpja_response(data: dict[str, Any]) -> CnpjaCompanyData:
    """Convert raw CNPJa JSON into a structured ``CnpjaCompanyData``."""
    company = data.get("company") or {}
    nature = company.get("nature") or {}
    size = company.get("size") or {}
    status_obj = data.get("status") or {}
    address_data = data.get("address") or {}
    main_activity = data.get("mainActivity") or {}
    side_activities = data.get("sideActivities") or []
    members_raw = company.get("members") or []

    return CnpjaCompanyData(
        cnpj=data.get("taxId", ""),
        razao_social=company.get("name", ""),
        nome_fantasia=data.get("alias", ""),
        natureza_juridica_id=nature.get("id"),
        natureza_juridica_text=nature.get("text", ""),
        capital_social=float(company.get("equity", 0) or 0),
        porte_id=size.get("id"),
        porte_text=size.get("text", ""),
        situacao_cadastral=status_obj.get("text", ""),
        data_situacao_cadastral=status_obj.get("date", ""),
        data_abertura=data.get("founded", ""),
        cnae_principal=_parse_activity(main_activity),
        cnaes_secundarios=[_parse_activity(a) for a in side_activities],
        address=_parse_address(address_data),
        members=[_parse_member(m) for m in members_raw],
        updated=data.get("updated", ""),
        raw=data,
    )


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class CnpjaApiError(Exception):
    """Raised when the CNPJa API returns an error."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"CNPJa API {status_code}: {detail}")


class CnpjaClient:
    """Async HTTP client for the CNPJa commercial API."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
    ) -> None:
        self._api_key = api_key or settings.cnpja_api_key
        self._base_url = (base_url or settings.cnpja_base_url).rstrip("/")
        self._timeout = timeout or settings.cnpja_timeout
        self._http: httpx.AsyncClient | None = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": self._api_key},
                timeout=self._timeout,
            )
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def fetch_office(self, cnpj_digits: str) -> CnpjaCompanyData:
        """Fetch full establishment data for a 14-digit CNPJ.

        The ``strategy=CACHE`` parameter tells CNPJa to serve cached data
        when available, reducing cost and latency.
        """
        client = await self._client()
        resp = await client.get(
            f"/office/{cnpj_digits}",
            params={"strategy": "CACHE"},
        )
        if resp.status_code == 404:
            raise CnpjaApiError(404, "CNPJ not found")
        if resp.status_code == 401:
            raise CnpjaApiError(401, "Invalid or missing CNPJa API key")
        if resp.status_code == 429:
            raise CnpjaApiError(429, "CNPJa rate limit exceeded")
        if resp.status_code >= 400:
            raise CnpjaApiError(resp.status_code, resp.text[:200])

        data = resp.json()
        logger.info("CNPJa lookup OK for %s (updated=%s)", cnpj_digits, data.get("updated"))
        return parse_cnpja_response(data)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_client: CnpjaClient | None = None


def get_cnpja_client() -> CnpjaClient:
    global _client
    if _client is None:
        _client = CnpjaClient()
    return _client


async def close_cnpja_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
