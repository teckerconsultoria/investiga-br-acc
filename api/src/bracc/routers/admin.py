"""Admin router for managing data sources, ETL pipelines, and bootstrap."""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from neo4j import AsyncSession
from pydantic import BaseModel

from bracc.config import settings
from bracc.dependencies import CurrentUser, get_session, get_optional_user
from bracc.models.user import UserResponse
from bracc.services import admin_service

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


class PipelineRunRequest(BaseModel):
    pipeline_id: str


class BootstrapRunRequest(BaseModel):
    sources: str = ""
    reset_db: bool = False


class ConfigUpdate(BaseModel):
    core_sources: list[str] | None = None


@router.get("/sources")
async def list_sources(user: CurrentUser) -> dict[str, list[dict[str, Any]]]:
    sources = admin_service.list_sources()
    return {"sources": sources}


@router.get("/sources/{pipeline_id}")
async def get_source(pipeline_id: str, user: CurrentUser) -> dict[str, Any]:
    source = admin_service.get_source(pipeline_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found")
    return source


@router.get("/contract/sources")
async def get_contract_sources(user: CurrentUser) -> dict[str, list[dict[str, Any]]]:
    return {"sources": admin_service.get_contract_sources()}


@router.post("/sources/{pipeline_id}/run")
async def run_pipeline(
    pipeline_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentUser,
) -> dict[str, str]:
    source = admin_service.get_source(pipeline_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found")
    return {"message": f"Pipeline {pipeline_id} execution started", "pipeline_id": pipeline_id}


@router.get("/bootstrap/status")
async def get_bootstrap_status(user: CurrentUser) -> dict[str, Any]:
    status_data = admin_service.get_bootstrap_status()
    if status_data is None:
        return {"status": "no_runs_found"}
    return {
        "status": "complete",
        "total_sources": status_data.get("total_sources", 0),
        "counts": status_data.get("counts", {}),
        "started_at": status_data.get("started_at_utc"),
        "ended_at": status_data.get("ended_at_utc"),
        "core_failures": status_data.get("core_failures", []),
    }


@router.get("/bootstrap/runs")
async def list_runs(user: CurrentUser) -> dict[str, list[dict[str, Any]]]:
    return {"runs": admin_service.list_runs()}


@router.get("/config")
async def get_config(user: CurrentUser) -> dict[str, Any]:
    sources = admin_service.get_contract_sources()
    core = set()
    for s in sources:
        if s.get("core"):
            core.add(s["pipeline_id"])
    return {"core_sources": sorted(core), "total_sources": len(sources)}


@router.put("/config")
async def update_config(body: ConfigUpdate, user: CurrentUser) -> dict[str, str]:
    if body.core_sources is None:
        raise HTTPException(status_code=400, detail="core_sources is required")
    return {"message": "Configuration updated (note: persistence not yet implemented)"}


@router.websocket("/ws/logs")
async def logs_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            run_type = msg.get("type")
            neo4j_password = msg.get("neo4j_password", settings.neo4j_password)

            if run_type == "pipeline":
                pipeline_id = msg.get("pipeline_id")
                if not pipeline_id:
                    await websocket.send_json({"type": "error", "message": "pipeline_id required"})
                    continue
                async for chunk in admin_service.run_pipeline(pipeline_id, neo4j_password):
                    await websocket.send_json(json.loads(chunk))
            elif run_type == "download":
                source = msg.get("source")
                if not source:
                    await websocket.send_json({"type": "error", "message": "source required"})
                    continue
                async for chunk in admin_service.run_download(source):
                    await websocket.send_json(json.loads(chunk))
            elif run_type == "bootstrap":
                sources = msg.get("sources", "")
                reset_db = msg.get("reset_db", False)
                async for chunk in admin_service.run_bootstrap(neo4j_password, reset_db, sources):
                    await websocket.send_json(json.loads(chunk))
            elif run_type == "ping":
                await websocket.send_json({"type": "pong"})
            else:
                await websocket.send_json({"type": "error", "message": f"Unknown type: {run_type}"})
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await websocket.send_json({"type": "error", "message": str(exc)})
        except Exception:
            pass
