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
    host = Path("/app/host")
    if (host / "docker-compose.yml").exists():
        return host
    base = Path(__file__).resolve().parents[4]
    if (base / "docs" / "source_registry_br_v1.csv").exists():
        return base
    return Path("/app")


def _get_host_repo_root() -> str | None:
    """Resolve the HOST filesystem path of the repo root for Docker volume mounts.

    When this API runs inside a container, /app/host is a bind mount from the
    host. Docker-in-Docker volume paths must use HOST paths (not container paths)
    because the Docker daemon resolves them on the host filesystem.
    """
    # 1. Explicit env var — most reliable when set in docker-compose.yml
    env_val = os.environ.get("HOST_REPO_ROOT", "").strip()
    if env_val:
        return env_val

    # 2. docker inspect on the current container — queries daemon directly
    try:
        import socket as _socket

        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{range .Mounts}}{{if eq .Destination \"/app/host\"}}{{.Source}}{{end}}{{end}}",
                _socket.gethostname(),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        path = result.stdout.strip()
        if path and path.startswith("/") and not path.startswith("/dev/"):
            return path
    except Exception:
        pass

    # 3. /proc/mounts — only valid for bind mounts (not block devices)
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "/app/host":
                    src = parts[0]
                    if src.startswith("/") and not src.startswith("/dev/"):
                        return src
    except OSError:
        pass

    return None


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

    compose_file = str(repo_root / "docker-compose.yml")

    # Step 1: ensure etl image exists (build if needed)
    build_cmd = [
        "docker",
        "compose",
        "-f",
        compose_file,
        "-p",
        "br-acc",
        "build",
        "etl",
    ]
    yield (
        json.dumps({"type": "log", "source": "system", "line": "Building ETL image (if needed)..."})
        + "\n"
    )
    build_proc = await asyncio.create_subprocess_exec(
        *build_cmd,
        cwd=str(repo_root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await build_proc.wait()
    if build_proc.returncode != 0:
        yield json.dumps({"type": "error", "message": "ETL image build failed"}) + "\n"
        RUNS[run_id]["status"] = "error"
        return

    # Step 2: run ETL via docker run with explicit host volume mounts.
    # docker compose run resolves '.' relative to the compose file path INSIDE
    # the container, but the Docker daemon needs HOST paths. We detect the real
    # host path via /proc/mounts and pass explicit -v flags to docker run.
    host_root = _get_host_repo_root()
    if host_root:
        volume_args = [
            "-v", f"{host_root}/etl:/workspace/etl",
            "-v", f"{host_root}/data:/workspace/data",
        ]
        etl_cmd = (
            f"cd /workspace/etl && uv run bracc-etl run --source {pipeline_id} "
            f"--neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j "
            f'--neo4j-password "$NEO4J_PASSWORD" --neo4j-database neo4j '
            f"--data-dir /workspace/data --linking-tier full"
        )
    else:
        # Fallback: code is baked into the image at /workspace/etl
        volume_args = []
        etl_cmd = (
            f"/workspace/etl/.venv/bin/bracc-etl run --source {pipeline_id} "
            f"--neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j "
            f'--neo4j-password "$NEO4J_PASSWORD" --neo4j-database neo4j '
            f"--data-dir /workspace/data --linking-tier full"
        )

    cmd = [
        "docker", "run", "--rm",
        "--name", f"etl-{run_id}",
        "--network", "br-acc_default",
        *volume_args,
        "-e", f"NEO4J_PASSWORD={neo4j_password}",
        "-e", "NEO4J_URI=bolt://neo4j:7687",
        "-e", "NEO4J_USER=neo4j",
        "-e", "PYTHONUNBUFFERED=1",
        "br-acc-etl",
        "bash", "-c", etl_cmd,
    ]

    yield json.dumps({"type": "start", "run_id": run_id, "cmd": f"etl run --source {pipeline_id}"}) + "\n"

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _drain(stream: asyncio.StreamReader, label: str) -> None:
            while True:
                line = await stream.readline()
                if not line:
                    break
                await queue.put(
                    json.dumps({"type": "log", "source": label, "line": line.decode(errors="replace").rstrip()})
                    + "\n"
                )
            await queue.put(None)

        t_out = asyncio.create_task(_drain(proc.stdout, "stdout"))
        t_err = asyncio.create_task(_drain(proc.stderr, "stderr"))
        done = 0
        while done < 2:
            item = await queue.get()
            if item is None:
                done += 1
            else:
                yield item
        await asyncio.gather(t_out, t_err)

        await proc.wait()
        status = "success" if proc.returncode == 0 else "failed"
        RUNS[run_id]["status"] = status
        RUNS[run_id]["exit_code"] = proc.returncode
        yield json.dumps({"type": "end", "status": status, "exit_code": proc.returncode}) + "\n"
    except Exception as exc:
        RUNS[run_id]["status"] = "error"
        RUNS[run_id]["error"] = str(exc)
        yield json.dumps({"type": "error", "message": str(exc)}) + "\n"


async def run_download(
    source: str,
) -> AsyncGenerator[str, None]:
    """Run download script for a data source and stream logs via WebSocket."""
    repo_root = _get_repo_root()
    run_id = f"download-{source}-{uuid.uuid4().hex[:8]}"
    RUNS[run_id] = {
        "source": source,
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
    }

    # Get source info to check access_mode
    source_info = get_source(source)
    access_mode = source_info.get("access_mode", "file") if source_info else "file"

    if access_mode == "bigquery":
        yield json.dumps({
            "type": "error",
            "message": f"Source '{source}' requires GCP BigQuery credentials. Not supported via admin panel."
        }) + "\n"
        RUNS[run_id]["status"] = "unsupported"
        return

    # Determine which download script to use
    # Portal da Transparencia sources use _api.py scripts
    api_script_map = {
        "ceaf": "download_ceaf_api.py",
        "sanctions": "download_sanctions_api.py",
    }

    script_name = api_script_map.get(source, f"download_{source}.py")
    download_script = repo_root / "etl" / "scripts" / script_name

    if not download_script.exists():
        yield json.dumps({
            "type": "error",
            "message": f"Download script not found: etl/scripts/{script_name}"
        }) + "\n"
        RUNS[run_id]["status"] = "error"
        return

    compose_file = str(repo_root / "docker-compose.yml")
    host_root = _get_host_repo_root()

    # Build download command
    # Use the data directory that matches the source
    data_dir_map = {
        "sanctions": "/workspace/data/sanctions",
    }
    data_dir = data_dir_map.get(source, f"/workspace/data/{source}")

    download_cmd = (
        f"cd /workspace/etl && "
        f"uv run python scripts/{script_name} --output-dir {data_dir} --no-skip-existing"
    )

    if host_root:
        volume_args = [
            "-v", f"{host_root}/data:/workspace/data",
        ]
    else:
        volume_args = []

    # Get the Portal API key from environment (or use default if not set)
    portal_api_key = os.environ.get("PORTAL_API_KEY", "")

    cmd = [
        "docker", "compose", "-f", compose_file, "-p", "br-acc", "run", "--rm",
        *volume_args,
        "-e", f"PORTAL_API_KEY={portal_api_key}",
        "etl",
        "bash", "-c", download_cmd,
    ]

    yield json.dumps({
        "type": "start",
        "run_id": run_id,
        "cmd": f"download --source {source} (via {script_name})",
        "access_mode": access_mode,
    }) + "\n"

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _drain(stream: asyncio.StreamReader, label: str) -> None:
            while True:
                line = await stream.readline()
                if not line:
                    break
                await queue.put(
                    json.dumps({"type": "log", "source": label, "line": line.decode(errors="replace").rstrip()})
                    + "\n"
                )
            await queue.put(None)

        t_out = asyncio.create_task(_drain(proc.stdout, "stdout"))
        t_err = asyncio.create_task(_drain(proc.stderr, "stderr"))
        done = 0
        while done < 2:
            item = await queue.get()
            if item is None:
                done += 1
            else:
                yield item
        await asyncio.gather(t_out, t_err)

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

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _drain(stream: asyncio.StreamReader, label: str) -> None:
            while True:
                line = await stream.readline()
                if not line:
                    break
                await queue.put(
                    json.dumps({"type": "log", "source": label, "line": line.decode(errors="replace").rstrip()})
                    + "\n"
                )
            await queue.put(None)

        t_out = asyncio.create_task(_drain(proc.stdout, "stdout"))
        t_err = asyncio.create_task(_drain(proc.stderr, "stderr"))
        done = 0
        while done < 2:
            item = await queue.get()
            if item is None:
                done += 1
            else:
                yield item
        await asyncio.gather(t_out, t_err)

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
