"""
Main FastAPI application for the Schema Scribe server.

This module defines the FastAPI application and its API endpoints. It provides
a RESTful API wrapper around the core `DbWorkflow` and `DbtWorkflow` classes,
allowing them to be triggered programmatically.
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional

from schema_scribe.core.workflow_helpers import load_config
from schema_scribe.core.db_workflow import DbWorkflow
from schema_scribe.core.dbt_workflow import DbtWorkflow
from schema_scribe.core.exceptions import DataScribeError, CIError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


app = FastAPI(
    title="Schema Scribe Server",
    description="API for running Schema Scribe documentation workflows.",
    version="1.0.0",
)

# --- Pydantic Models for API Request/Response Validation ---


class ProfileInfo(BaseModel):
    """Defines the response structure for the profile discovery endpoint."""

    db_connections: List[str]
    llm_providers: List[str]
    output_profiles: List[str]


class RunDbWorkflowRequest(BaseModel):
    """Defines the request body for triggering the 'db' workflow."""

    db_profile: str
    llm_profile: str
    output_profile: str


class RunDbtWorkflowRequest(BaseModel):
    """
    Defines the request body for triggering the 'dbt' workflow.
    The mode flags (`update_yaml`, `check`, `drift`) are mutually exclusive.
    """

    dbt_project_dir: str
    llm_profile: Optional[str] = None
    db_profile: Optional[str] = None  # Required only for drift mode
    output_profile: Optional[str] = None
    # Mode flags
    update_yaml: bool = False
    check: bool = False
    drift: bool = False


# --- API Endpoints ---


@app.get("/api/profiles", response_model=ProfileInfo)
def get_profiles():
    """
    A discovery endpoint that returns available profiles from `config.yaml`.

    This is useful for UIs that need to populate dropdown menus with available
    connection, LLM, and output options.

    Raises:
        HTTPException(404): If `config.yaml` is not found.
        HTTPException(500): For other configuration loading errors.
    """
    try:
        config = load_config("config.yaml")
        return {
            "db_connections": list(config.get("db_connections", {}).keys()),
            "llm_providers": list(config.get("llm_providers", {}).keys()),
            "output_profiles": list(config.get("output_profiles", {}).keys()),
        }
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="config.yaml not found in the current directory.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to load config: {e}"
        )


@app.post("/api/run/db", status_code=200)
def run_db_workflow(request: RunDbWorkflowRequest):
    """
    Runs the 'db' documentation workflow.

    This endpoint triggers a synchronous run of the `DbWorkflow`. It maps internal
    `DataScribeError` exceptions to HTTP 400 Bad Request responses.

    Args:
        request: A `RunDbWorkflowRequest` with the db, llm, and output profiles.

    Returns:
        A success message if the workflow completes.
    """
    try:
        logger.info(
            f"Received request to run 'db' workflow with profile: {request.db_profile}"
        )
        workflow = DbWorkflow(
            config_path="config.yaml",
            db_profile=request.db_profile,
            llm_profile=request.llm_profile,
            output_profile=request.output_profile,
        )
        workflow.run()
        return {
            "status": "success",
            "message": f"DB workflow completed for {request.db_profile}.",
        }
    except DataScribeError as e:
        logger.error(
            f"Schema Scribe error running workflow: {e}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )


@app.post("/api/run/dbt", status_code=200)
def run_dbt_workflow(request: RunDbtWorkflowRequest):
    """
    Runs the 'dbt' documentation workflow with various modes.

    This endpoint triggers a synchronous run of the `DbtWorkflow`. It is designed
    for programmatic use, especially in CI/CD pipelines.

    - **CI Failures**: If `check` or `drift` mode is used and a failure is
      detected, this endpoint returns an **HTTP 409 Conflict** status, which
      can be used to fail a CI job.
    - **Interactive Mode**: The `interactive` mode is not supported via the API
      and is always disabled.

    Args:
        request: A `RunDbtWorkflowRequest` defining the project and execution mode.

    Returns:
        A success message if the workflow completes without a CI failure.
    """
    try:
        logger.info(
            f"Received request to run 'dbt' workflow for dir: {request.dbt_project_dir}"
        )
        if sum([request.update_yaml, request.check, request.drift]) > 1:
            raise HTTPException(
                status_code=400, detail="Modes are mutually exclusive."
            )
        if request.drift and not request.db_profile:
            raise HTTPException(
                status_code=400, detail="Drift mode requires a db_profile."
            )

        workflow = DbtWorkflow(
            dbt_project_dir=request.dbt_project_dir,
            db_profile=request.db_profile,
            llm_profile=request.llm_profile,
            config_path="config.yaml",
            output_profile=request.output_profile,
            update_yaml=request.update_yaml,
            check=request.check,
            interactive=False,  # Interactive mode is CLI-only
            drift=request.drift,
        )
        workflow.run()
        return {
            "status": "success",
            "message": f"dbt workflow completed for {request.dbt_project_dir}.",
        }
    except CIError as e:
        logger.warning(f"CI check failed during API call: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except DataScribeError as e:
        logger.error(
            f"Schema Scribe error running workflow: {e}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )


# --- Static File Serving ---

# Get the directory where this server file is located.
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(SERVER_DIR, "static")


# Serve the main index.html file from the root path.
@app.get("/", include_in_schema=False)
async def read_index():
    """Serves the main index.html file for the frontend."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return {
            "message": "Schema Scribe Server is running. Frontend 'index.html' not found."
        }
    return FileResponse(index_path)


# Mount the 'static' directory to serve all other static files (CSS, JS, etc.).
# This must come after the root endpoint to ensure the root is served correctly.
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
