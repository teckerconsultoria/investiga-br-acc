from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    format_cnpj,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

_FINANCE_QUERY = (
    "UNWIND $rows AS row "
    "MERGE (f:Finance {finance_id: row.finance_id}) "
    "SET f.type = row.type, "
    "    f.inscription_number = row.inscription_number, "
    "    f.value = row.value, "
    "    f.date = row.date, "
    "    f.situation = row.situation, "
    "    f.revenue_type = row.revenue_type, "
    "    f.court_action = row.court_action, "
    "    f.source = row.source"
)

_REL_QUERY = (
    "UNWIND $rows AS row "
    "MERGE (c:Company {cnpj: row.source_key}) "
    "ON CREATE SET c.razao_social = row.company_name, c.name = row.company_name "
    "WITH c, row "
    "MATCH (f:Finance {finance_id: row.target_key}) "
    "MERGE (c)-[r:DEVE]->(f) "
    "SET r.value = row.value, "
    "    r.date = row.date"
)


class PgfnPipeline(Pipeline):
    """ETL pipeline for PGFN active tax debt (divida ativa da Uniao).

    Ingests company-only records (CNPJ). Person CPFs are pre-masked by PGFN
    and cannot be matched to existing Person nodes.
    Only PRINCIPAL debtors are loaded to avoid double-counting.

    Processes files in streaming chunks to avoid accumulating 24M records in RAM.
    """

    name = "pgfn"
    source_id = "pgfn"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._csv_files: list[Path] = []

    def _parse_value(self, value: str) -> float:
        """Parse numeric value (may use comma as decimal sep)."""
        if not value or not value.strip():
            return 0.0
        cleaned = value.strip().replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def extract(self) -> None:
        pgfn_dir = Path(self.data_dir) / "pgfn"
        if not pgfn_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, pgfn_dir)
            return
        self._csv_files = sorted(pgfn_dir.glob("arquivo_lai_SIDA_*_*.csv"))
        if not self._csv_files:
            logger.warning("[%s] No PGFN CSV files found in %s", self.name, pgfn_dir)
            return
        logger.info("[pgfn] Found %d CSV files to process", len(self._csv_files))

    def transform(self) -> None:
        # Streaming pipeline: transform happens inline during load().
        # Set rows_in to a non-zero sentinel so the base class require_data check
        # doesn't fire prematurely — final count is set at end of load().
        if self._csv_files:
            self.rows_in = 1

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver, batch_size=self.chunk_size)
        total_loaded = 0
        skipped_pf = 0
        skipped_corresponsavel = 0
        skipped_bad_cnpj = 0
        seen_inscricoes: set[str] = set()

        for csv_file in self._csv_files:
            logger.info("[pgfn] Processing %s", csv_file.name)
            finances: list[dict[str, Any]] = []
            relationships: list[dict[str, Any]] = []

            for chunk in pd.read_csv(
                csv_file,
                dtype=str,
                delimiter=";",
                encoding="latin-1",
                keep_default_na=False,
                chunksize=100_000,
            ):
                mask_pj = chunk["TIPO_PESSOA"].str.contains("jur", case=False, na=False)
                mask_principal = chunk["TIPO_DEVEDOR"] == "PRINCIPAL"
                skipped_pf += int((~mask_pj).sum())
                skipped_corresponsavel += int((mask_pj & ~mask_principal).sum())

                for _, row in chunk[mask_pj & mask_principal].iterrows():
                    cnpj_raw = str(row["CPF_CNPJ"]).strip()
                    digits = strip_document(cnpj_raw)
                    if len(digits) != 14:
                        skipped_bad_cnpj += 1
                        continue

                    inscricao = str(row["NUMERO_INSCRICAO"]).strip()
                    if not inscricao or inscricao in seen_inscricoes:
                        continue
                    seen_inscricoes.add(inscricao)

                    cnpj_formatted = format_cnpj(cnpj_raw)
                    finance_id = f"pgfn_{inscricao}"
                    valor = self._parse_value(str(row["VALOR_CONSOLIDADO"]))
                    date = parse_date(str(row["DATA_INSCRICAO"]))
                    nome = normalize_name(str(row["NOME_DEVEDOR"]))
                    situacao = str(row["SITUACAO_INSCRICAO"]).strip()
                    receita = str(row["RECEITA_PRINCIPAL"]).strip()
                    ajuizado = str(row["INDICADOR_AJUIZADO"]).strip()

                    finances.append({
                        "finance_id": finance_id,
                        "type": "divida_ativa",
                        "inscription_number": inscricao,
                        "value": valor,
                        "date": date,
                        "situation": situacao,
                        "revenue_type": receita,
                        "court_action": ajuizado,
                        "source": "pgfn",
                    })
                    relationships.append({
                        "source_key": cnpj_formatted,
                        "target_key": finance_id,
                        "value": valor,
                        "date": date,
                        "company_name": nome,
                    })

                    if self.limit and len(finances) >= self.limit:
                        break

                # Flush chunk to Neo4j immediately — don't accumulate across chunks
                if finances:
                    loader._run_batches(_FINANCE_QUERY, finances)
                    loader._run_batches(_REL_QUERY, relationships)
                    total_loaded += len(finances)
                    finances = []
                    relationships = []

                if self.limit and total_loaded >= self.limit:
                    break

            if self.limit and total_loaded >= self.limit:
                break

        self.rows_in = total_loaded
        self.rows_loaded = total_loaded

        logger.info("[pgfn] Loaded %d Finance nodes + DEVE relationships", total_loaded)
        logger.info(
            "[pgfn] Skipped: %d person (masked CPF), %d co-responsible, %d bad CNPJ",
            skipped_pf,
            skipped_corresponsavel,
            skipped_bad_cnpj,
        )
