"""Tests for the CNPJa on-demand API integration."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from bracc.services.cnpja_client import (
    CnpjaApiError,
    CnpjaClient,
    CnpjaCompanyData,
    parse_cnpja_response,
)
from bracc.services.cnpja_cache import (
    _format_cnpj,
    _format_cpf,
    _is_valid_cnpj,
    _is_valid_cpf,
)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

SAMPLE_CNPJA_RESPONSE = {
    "updated": "2025-07-17T14:46:27Z",
    "taxId": "33000167000101",
    "alias": "PETROBRAS",
    "founded": "1966-09-28",
    "head": True,
    "company": {
        "id": 33000167,
        "name": "PETROLEO BRASILEIRO S.A. PETROBRAS",
        "equity": 205431960490.52,
        "nature": {"id": 2046, "text": "Sociedade Anônima Aberta"},
        "size": {"id": 5, "acronym": "DEMAIS", "text": "Demais"},
        "members": [
            {
                "since": "2023-04-12",
                "role": {"id": 10, "text": "Diretor"},
                "person": {
                    "id": "12345678901",
                    "name": "JOAO DA SILVA",
                    "type": "NATURAL",
                    "taxId": "12345678901",
                    "age": "41-50",
                },
            },
            {
                "since": "2022-01-15",
                "role": {"id": 49, "text": "Sócio-Administrador"},
                "person": {
                    "id": "98765432000199",
                    "name": "EMPRESA PARCEIRA LTDA",
                    "type": "LEGAL",
                    "taxId": "98765432000199",
                },
            },
        ],
    },
    "status": {"id": 2, "text": "Ativa", "date": "2005-11-03"},
    "address": {
        "municipality": {"ibge": 3304557, "name": "RIO DE JANEIRO"},
        "state": {"ibge": 33, "acronym": "RJ"},
        "street": "AV REPUBLICA DO CHILE",
        "number": "65",
        "district": "CENTRO",
        "zip": "20031170",
        "details": "",
    },
    "mainActivity": {"id": 1921401, "text": "Fabricação de produtos do refino de petróleo"},
    "sideActivities": [
        {"id": 600102, "text": "Extração de petróleo e gás natural"},
    ],
}


def test_parse_cnpja_response_company_fields() -> None:
    data = parse_cnpja_response(SAMPLE_CNPJA_RESPONSE)
    assert isinstance(data, CnpjaCompanyData)
    assert data.cnpj == "33000167000101"
    assert data.razao_social == "PETROLEO BRASILEIRO S.A. PETROBRAS"
    assert data.nome_fantasia == "PETROBRAS"
    assert data.capital_social == 205431960490.52
    assert data.natureza_juridica_id == 2046
    assert data.natureza_juridica_text == "Sociedade Anônima Aberta"
    assert data.porte_text == "Demais"
    assert data.situacao_cadastral == "Ativa"
    assert data.data_abertura == "1966-09-28"


def test_parse_cnpja_response_address() -> None:
    data = parse_cnpja_response(SAMPLE_CNPJA_RESPONSE)
    assert data.address.municipality == "RIO DE JANEIRO"
    assert data.address.state == "RJ"
    assert data.address.street == "AV REPUBLICA DO CHILE"
    assert data.address.zip == "20031170"


def test_parse_cnpja_response_activities() -> None:
    data = parse_cnpja_response(SAMPLE_CNPJA_RESPONSE)
    assert data.cnae_principal.id == 1921401
    assert "petróleo" in data.cnae_principal.text.lower()
    assert len(data.cnaes_secundarios) == 1
    assert data.cnaes_secundarios[0].id == 600102


def test_parse_cnpja_response_members() -> None:
    data = parse_cnpja_response(SAMPLE_CNPJA_RESPONSE)
    assert len(data.members) == 2

    pf = data.members[0]
    assert pf.person_name == "JOAO DA SILVA"
    assert pf.person_type == "NATURAL"
    assert pf.person_tax_id == "12345678901"
    assert pf.role_text == "Diretor"

    pj = data.members[1]
    assert pj.person_name == "EMPRESA PARCEIRA LTDA"
    assert pj.person_type == "LEGAL"
    assert pj.person_tax_id == "98765432000199"


def test_parse_empty_response() -> None:
    data = parse_cnpja_response({})
    assert data.cnpj == ""
    assert data.razao_social == ""
    assert data.members == []
    assert data.capital_social == 0.0


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------

def test_format_cnpj() -> None:
    assert _format_cnpj("33000167000101") == "33.000.167/0001-01"


def test_format_cpf() -> None:
    assert _format_cpf("12345678901") == "123.456.789-01"


def test_is_valid_cpf() -> None:
    assert _is_valid_cpf("12345678901") is True
    assert _is_valid_cpf("00000000000") is False
    assert _is_valid_cpf(None) is False
    assert _is_valid_cpf("123") is False


def test_is_valid_cnpj() -> None:
    assert _is_valid_cnpj("33000167000101") is True
    assert _is_valid_cnpj("00000000000000") is False
    assert _is_valid_cnpj(None) is False
    assert _is_valid_cnpj("123") is False


# ---------------------------------------------------------------------------
# Client error handling
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_client_raises_on_404() -> None:
    client = CnpjaClient(api_key="test-key", base_url="https://api.cnpja.com")
    mock_resp = AsyncMock()
    mock_resp.status_code = 404
    mock_resp.text = "Not Found"

    with patch.object(client, "_client") as mock_http:
        mock_inner = AsyncMock()
        mock_inner.get.return_value = mock_resp
        mock_http.return_value = mock_inner

        with pytest.raises(CnpjaApiError) as exc_info:
            await client.fetch_office("00000000000000")
        assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_client_raises_on_401() -> None:
    client = CnpjaClient(api_key="bad-key", base_url="https://api.cnpja.com")
    mock_resp = AsyncMock()
    mock_resp.status_code = 401
    mock_resp.text = "Unauthorized"

    with patch.object(client, "_client") as mock_http:
        mock_inner = AsyncMock()
        mock_inner.get.return_value = mock_resp
        mock_http.return_value = mock_inner

        with pytest.raises(CnpjaApiError) as exc_info:
            await client.fetch_office("33000167000101")
        assert exc_info.value.status_code == 401


@pytest.mark.anyio
async def test_client_parses_success_response() -> None:
    client = CnpjaClient(api_key="test-key", base_url="https://api.cnpja.com")
    mock_resp = AsyncMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = SAMPLE_CNPJA_RESPONSE

    with patch.object(client, "_client") as mock_http:
        mock_inner = AsyncMock()
        mock_inner.get.return_value = mock_resp
        mock_http.return_value = mock_inner

        result = await client.fetch_office("33000167000101")
        assert result.razao_social == "PETROLEO BRASILEIRO S.A. PETROBRAS"
        assert len(result.members) == 2
