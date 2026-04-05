"""Admin service for managing data sources, ETL pipelines, and bootstrap."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bracc.services.source_registry import load_source_registry

RUNS: dict[str, dict[str, Any]] = {}


def _load_contract(repo_root: Path) -> dict[str, Any]:
    path = repo_root / "config" / "bootstrap_all_contract.yml"
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        import yaml

        return yaml.safe_load(raw)


def _get_repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def list_sources() -> list[dict[str, Any]]:
    entries = load_source_registry()
    return [e.to_public_dict() for e in entries if e.in_universe_v1]


def get_source(pipeline_id: str) -> dict[str, Any] | None:
    for entry in load_source_registry():
        if entry.pipeline_id == pipeline_id and entry.in_universe_v1:
            return entry.to_public_dict()
    return None


def get_contract_sources() -> list[dict[str, Any]]:
    repo_root = _get_repo_root()
    contract = _load_contract(repo_root)
    return contract.get("sources", [])


def get_bootstrap_status() -> dict[str, Any] | None:
    repo_root = _get_repo_root()
    latest = repo_root / "audit-results" / "bootstrap-all" / "latest" / "summary.json"
    if not latest.exists():
        return None
    return json.loads(latest.read_text(encoding="utf-8"))


async def run_pipeline(
    pipeline_id: str,
    neo4j_password: str,
) -> AsyncGenerator[str, None]:
    repo_root = _get_repo_root()
    run_id = f"pipeline-{pipeline_id}-{uuid.uuid4().hex[:8]}"
    RUNS[run_id] = {
        "pipeline_id": pipeline_id,
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
    }

    cmd = [
        "docker",
        "compose",
        "--profile",
        "etl",
        "run",
        "--rm",
        "-e",
        f"NEO4J_PASSWORD={neo4j_password}",
        "etl",
        "bash",
        "-lc",
        f"cd /workspace/etl && uv run bracc-etl run --source {pipeline_id} "
        f"--neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j "
        f'--neo4j-password "$NEO4J_PASSWORD" --neo4j-database neo4j '
        f"--data-dir ../data --linking-tier full",
    ]

    yield json.dumps({"type": "start", "run_id": run_id, "cmd": " ".join(cmd[-1:])}) + "\n"

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )

        async def read_stream(stream, label):
            while True:
                line = await stream.readline()
                if not line:
                    break
                yield (
                    json.dumps(
                        {
                            "type": "log",
                            "source": label,
                            "line": line.decode(errors="replace").rstrip(),
                        }
                    )
                    + "\n"
                )

        async for chunk in read_stream(proc.stdout, "stdout"):
            yield chunk
        async for chunk in read_stream(proc.stderr, "stderr"):
            yield chunk

        await proc.wait()
        status = "success" if proc.returncode == 0 else "failed"
        RUNS[run_id]["status"] = status
        RUNS[run_id]["exit_code"] = proc.returncode
        yield json.dumps({"type": "end", "status": status, "exit_code": proc.returncode}) + "\n"
    except Exception as exc:
        RUNS[run_id]["status"] = "error"
        RUNS[run_id]["error"] = str(exc)
        yield json.dumps({"type": "error", "message": str(exc)}) + "\n"


async def run_bootstrap(
    neo4j_password: str,
    reset_db: bool = False,
    sources: str = "",
) -> AsyncGenerator[str, None]:
    repo_root = _get_repo_root()
    run_id = f"bootstrap-{uuid.uuid4().hex[:8]}"
    RUNS[run_id] = {"status": "running", "started_at": datetime.now(UTC).isoformat()}

    args = ["--noninteractive", "--yes-reset"] if reset_db else ["--noninteractive", "--no-reset"]
    if sources:
        args.extend(["--sources", sources])

    cmd = [
        "python3",
        str(repo_root / "scripts" / "run_bootstrap_all.py"),
        "--repo-root",
        str(repo_root),
    ] + args

    yield json.dumps({"type": "start", "run_id": run_id}) + "\n"

    try:
        env = {**os.environ, "NEO4J_PASSWORD": neo4j_password}
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        async def read_stream(stream, label):
            while True:
                line = await stream.readline()
                if not line:
                    break
                yield (
                    json.dumps(
                        {
                            "type": "log",
                            "source": label,
                            "line": line.decode(errors="replace").rstrip(),
                        }
                    )
                    + "\n"
                )

        async for chunk in read_stream(proc.stdout, "stdout"):
            yield chunk
        async for chunk in read_stream(proc.stderr, "stderr"):
            yield chunk

        await proc.wait()
        status = "success" if proc.returncode == 0 else "failed"
        RUNS[run_id]["status"] = status
        RUNS[run_id]["exit_code"] = proc.returncode
        yield json.dumps({"type": "end", "status": status, "exit_code": proc.returncode}) + "\n"
    except Exception as exc:
        RUNS[run_id]["status"] = "error"
        RUNS[run_id]["error"] = str(exc)
        yield json.dumps({"type": "error", "message": str(exc)}) + "\n"


def get_run_status(run_id: str) -> dict[str, Any] | None:
    return RUNS.get(run_id)


def list_runs() -> list[dict[str, Any]]:
    return [{"run_id": k, **v} for k, v in list(RUNS.items())[-50:]]
