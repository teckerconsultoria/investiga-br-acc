#!/usr/bin/env python3
"""Download CEIS/CNEP sanctions data from Portal da Transparencia API.

The government migrated from static ZIP downloads to a paginated REST API.
This script fetches all pages for both CEIS and CNEP and produces flat CSVs
compatible with the existing ETL pipeline.

Usage:
    python etl/scripts/download_sanctions_api.py
    python etl/scripts/download_sanctions_api.py --output-dir ./data/sanctions
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import click
import httpx
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
PAGE_SIZE = 15  # API default page size
REQUEST_DELAY = 0.5  # seconds between requests
MAX_PAGES = 10000  # safety limit


def _flatten_ceis(rec: dict) -> dict:
    """Flatten CEIS API record into flat CSV columns."""
    sancionado = rec.get("sancionado") or {}
    tipo = rec.get("tipoSancao") or {}
    fundamentacoes = rec.get("fundamentacao") or []

    motivo = ""
    if fundamentacoes:
        motivo = fundamentacoes[0].get("codigo", "").strip()

    return {
        "cpf_cnpj": (sancionado.get("codigoFormatado") or "").strip(),
        "nome": (sancionado.get("nome") or "").strip(),
        "tipo_sancao": (tipo.get("descricaoResumida") or tipo.get("descricaoPortal") or "").strip(),
        "data_inicio": (rec.get("dataInicioSancao") or "").strip(),
        "data_fim": (rec.get("dataFimSancao") or "").strip(),
        "motivo": motivo,
    }


def _flatten_cnep(rec: dict) -> dict:
    """Flatten CNEP API record into flat CSV columns."""
    # CNEP may have slightly different structure; adapt as needed
    pessoa = rec.get("pessoa") or {}
    punido = rec.get("punido") or {}
    tipo = rec.get("tipoPunicao") or {}
    fundamentacoes = rec.get("fundamentacao") or []

    motivo = ""
    if fundamentacoes:
        motivo = fundamentacoes[0].get("codigo", "").strip()

    # CNEP uses 'punido' for the sanctioned entity
    doc = (pessoa.get("cpfFormatado") or pessoa.get("cnpjFormatado") or
           punido.get("cpfFormatado") or punido.get("cnpjFormatado") or "").strip()
    nome = (pessoa.get("nome") or pessoa.get("razaoSocialReceita") or
            punido.get("nome") or "").strip()

    return {
        "cpf_cnpj": doc,
        "nome": nome,
        "tipo_sancao": (tipo.get("descricao") or tipo.get("descricaoResumida") or "").strip(),
        "data_inicio": (rec.get("dataInicioSancao") or rec.get("dataPublicacao") or "").strip(),
        "data_fim": (rec.get("dataFimSancao") or "").strip(),
        "motivo": motivo,
    }


def _fetch_all_pages(endpoint: str, api_key: str) -> list[dict]:
    """Fetch all pages from an API endpoint."""
    url = f"{API_BASE}/{endpoint}"
    all_records: list[dict] = []
    pagina = 1

    while True:
        params = {"pagina": pagina}
        headers = {"chave-api-dados": api_key, "Accept": "application/json"}

        try:
            resp = httpx.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("API request failed for %s (page %d): %s", endpoint, pagina, e)
            break

        data = resp.json()
        if not data:
            break

        all_records.extend(data)
        logger.info("%s page %d: fetched %d records (total: %d)", endpoint, pagina, len(data), len(all_records))

        if len(data) < PAGE_SIZE or pagina >= MAX_PAGES:
            break

        pagina += 1
        time.sleep(REQUEST_DELAY)

    return all_records


@click.command()
@click.option(
    "--api-key",
    envvar="PORTAL_API_KEY",
    required=True,
    help="Portal da Transparencia API key (or set PORTAL_API_KEY env var)",
)
@click.option("--output-dir", default="./data/sanctions", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip if CSVs exist")
def main(api_key: str, output_dir: str, skip_existing: bool) -> None:
    """Download CEIS and CNEP sanctions data via API."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    success_count = 0

    for dataset, flatten_fn, filename in [
        ("ceis", _flatten_ceis, "ceis.csv"),
        ("cnep", _flatten_cnep, "cnep.csv"),
    ]:
        output_path = out / filename
        if skip_existing and output_path.exists():
            logger.info("Skipping (exists): %s", output_path)
            success_count += 1
            continue

        logger.info("=== Fetching %s ===", dataset.upper())
        records = _fetch_all_pages(dataset, api_key)

        if not records:
            logger.warning("No records fetched for %s", dataset)
            continue

        logger.info("Flattening %d %s records...", len(records), dataset)
        flat_records = [flatten_fn(r) for r in records]

        df = pd.DataFrame(flat_records)
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info("Wrote %d rows to %s", len(df), output_path)
        success_count += 1

    logger.info("=== Done: %d/2 datasets downloaded ===", success_count)
    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
