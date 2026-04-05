#!/usr/bin/env python3
"""Download TCU (Tribunal de Contas da Uniao) sanction data.

Usage:
    python etl/scripts/download_tcu.py
    python etl/scripts/download_tcu.py --output-dir ./data/tcu
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))
from _download_utils import download_file, validate_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://sites.tcu.gov.br/dados-abertos/inidoneos-irregulares/arquivos"

FILES = [
    "inabilitados-funcao-publica.csv",
    "licitantes-inidoneos.csv",
    "resp-contas-julgadas-irregulares.csv",
    "resp-contas-julgadas-irreg-implicacao-eleitoral.csv",
]


@click.command()
@click.option("--output-dir", default="./data/tcu", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip existing files")
@click.option("--timeout", type=int, default=120, help="Download timeout in seconds")
def main(output_dir: str, skip_existing: bool, timeout: int) -> None:
    """Download TCU sanction lists (inidôneos, inabilitados, contas irregulares)."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    logger.info("=== TCU download: %d files ===", len(FILES))
    success = 0

    for filename in FILES:
        dest = out / filename

        if skip_existing and dest.exists():
            logger.info("Skipping (exists): %s", filename)
            success += 1
            continue

        url = f"{BASE_URL}/{filename}"
        if download_file(url, dest, timeout=timeout):
            validate_csv(dest, sep="|", encoding="utf-8")
            success += 1

    logger.info("=== Done: %d/%d files downloaded ===", success, len(FILES))
    if success == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
