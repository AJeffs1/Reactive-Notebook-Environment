"""
FastAPI Backend for Reactive Notebook

Provides:
- REST API for cell management
- WebSocket for real-time updates
- Notebook persistence
"""

import os
import json
import asyncio
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from parser import Cell, parse_notebook_file, serialize_notebook_file, create_cell, find_cell_by_id, remove_cell_by_id
from reactor import Reactor, CellState, CellStatus, cell_state_to_dict
from database import DatabaseManager


# --- Configuration ---

NOTEBOOK_FILE = os.environ.get("NOTEBOOK_FILE", "notebook.py")
# Support both local dev and Docker paths
FRONTEND_DIR = os.environ.get(
    "FRONTEND_DIR",
    os.path.join(os.path.dirname(__file__), "..", "frontend")
)


# --- Pydantic Models ---

class CellCreate(BaseModel):
    type: str = "python"
    code: str = ""
    as_var: Optional[str] = None
    after_id: Optional[str] = None  # Insert after this cell ID


class CellUpdate(BaseModel):
    code: Optional[str] = None
    type: Optional[str] = None
    as_var: Optional[str] = None


class DatabaseConfig(BaseModel):
    connection_string: str


class CellResponse(BaseModel):
    id: str
    type: str
    code: str
    as_var: Optional[str] = None


# --- Global State ---

cells: list[Cell] = []
reactor: Reactor = Reactor()
db_manager: DatabaseManager = DatabaseManager()
websocket_connections: list[WebSocket] = []


# --- WebSocket Broadcast ---

async def broadcast_status(cell_id: str, state: CellState):
    """Broadcast cell status update to all connected WebSocket clients."""
    message = {
        "type": "status",
        "data": cell_state_to_dict(state),
    }
    await broadcast_message(message)


async def broadcast_cells_updated():
    """Broadcast that cells list has changed."""
    message = {
        "type": "cells_updated",
        "data": [cell_to_response(c) for c in cells],
    }
    await broadcast_message(message)


async def broadcast_message(message: dict):
    """Send message to all connected WebSocket clients."""
    if not websocket_connections:
        return

    disconnected = []
    for ws in websocket_connections:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.append(ws)

    # Clean up disconnected clients
    for ws in disconnected:
        if ws in websocket_connections:
            websocket_connections.remove(ws)


def sync_status_callback(cell_id: str, state: CellState):
    """Synchronous wrapper for async broadcast (called from reactor)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(broadcast_status(cell_id, state))
        else:
            loop.run_until_complete(broadcast_status(cell_id, state))
    except RuntimeError:
        # No event loop, skip broadcast
        pass


# --- Helper Functions ---

def cell_to_response(cell: Cell) -> dict:
    """Convert Cell to API response format."""
    return {
        "id": cell.id,
        "type": cell.cell_type,
        "code": cell.code,
        "as": cell.as_var,
    }


def load_notebook():
    """Load notebook from file."""
    global cells
    if os.path.exists(NOTEBOOK_FILE):
        try:
            cells = parse_notebook_file(NOTEBOOK_FILE)
        except Exception as e:
            print(f"Error loading notebook: {e}")
            cells = []
    else:
        cells = []
    reactor.set_cells(cells)


def save_notebook():
    """Save notebook to file."""
    try:
        serialize_notebook_file(cells, NOTEBOOK_FILE)
    except Exception as e:
        print(f"Error saving notebook: {e}")


def execute_sql_cell(cell: Cell):
    """Execute a SQL cell and inject results into namespace."""
    from executor import ExecutionResult

    if not db_manager.is_connected():
        return ExecutionResult(
            cell_id=cell.id,
            success=False,
            error="No database connection configured",
        )

    var_name = cell.as_var or f"_sql_{cell.id}"

    try:
        df = db_manager.execute_query(cell.code)
        reactor.executor.inject_sql_result(var_name, df)

        # Render the result
        if hasattr(df, 'to_html'):
            if df.shape[0] > 50:
                result_html = df.head(50).to_html(classes='dataframe', index=True)
                result_html += f"<p><em>Showing 50 of {df.shape[0]} rows</em></p>"
            else:
                result_html = df.to_html(classes='dataframe', index=True)
            result_type = "html"
        else:
            result_html = str(df)
            result_type = "text"

        return ExecutionResult(
            cell_id=cell.id,
            success=True,
            result=result_html,
            result_type=result_type,
        )

    except Exception as e:
        import traceback
        return ExecutionResult(
            cell_id=cell.id,
            success=False,
            error=str(e),
            error_traceback=traceback.format_exc(),
        )


# --- Lifespan ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    load_notebook()
    reactor.set_status_callback(sync_status_callback)
    print(f"Loaded {len(cells)} cells from {NOTEBOOK_FILE}")
    yield
    # Shutdown
    save_notebook()
    db_manager.close()
    print("Notebook saved")


# --- FastAPI App ---

app = FastAPI(
    title="Reactive Notebook",
    description="A reactive notebook environment",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- REST Endpoints ---

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/cells")
async def get_cells():
    """Get all cells."""
    return {
        "cells": [cell_to_response(c) for c in cells],
        "states": {cid: cell_state_to_dict(s) for cid, s in reactor.get_all_states().items()},
    }


@app.post("/cells")
async def create_new_cell(cell_data: CellCreate):
    """Create a new cell."""
    new_cell = create_cell(
        cell_type=cell_data.type,
        code=cell_data.code,
        as_var=cell_data.as_var,
    )

    # Insert at position based on after_id
    if cell_data.after_id == "":
        # Empty string means insert at beginning
        cells.insert(0, new_cell)
    elif cell_data.after_id:
        # Insert after specified cell
        insert_idx = None
        for i, c in enumerate(cells):
            if c.id == cell_data.after_id:
                insert_idx = i + 1
                break
        if insert_idx is not None:
            cells.insert(insert_idx, new_cell)
        else:
            cells.append(new_cell)
    else:
        # null/None means append at end
        cells.append(new_cell)

    reactor.set_cells(cells)
    save_notebook()
    await broadcast_cells_updated()

    return cell_to_response(new_cell)


@app.get("/cells/{cell_id}")
async def get_cell(cell_id: str):
    """Get a specific cell."""
    cell = find_cell_by_id(cells, cell_id)
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    state = reactor.get_cell_state(cell_id)
    return {
        "cell": cell_to_response(cell),
        "state": cell_state_to_dict(state) if state else None,
    }


@app.put("/cells/{cell_id}")
async def update_cell(cell_id: str, cell_data: CellUpdate):
    """Update a cell's code or type."""
    cell = find_cell_by_id(cells, cell_id)
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    if cell_data.code is not None:
        cell.code = cell_data.code
    if cell_data.type is not None:
        cell.cell_type = cell_data.type
    if cell_data.as_var is not None:
        cell.as_var = cell_data.as_var

    reactor.set_cells(cells)
    # Don't save on every edit - save on run or shutdown to avoid reload loops
    await broadcast_cells_updated()

    return cell_to_response(cell)


@app.delete("/cells/{cell_id}")
async def delete_cell(cell_id: str):
    """Delete a cell and clean up its variables from namespace."""
    from dependency import analyze_cell

    # Find the cell before deleting
    cell = find_cell_by_id(cells, cell_id)
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    # Analyze what variables this cell defined
    analysis = analyze_cell(cell)
    variables_to_remove = analysis.writes

    # Remove cell from list
    if not remove_cell_by_id(cells, cell_id):
        raise HTTPException(status_code=404, detail="Cell not found")

    # Remove variables from namespace
    for var_name in variables_to_remove:
        if var_name in reactor.executor.namespace:
            del reactor.executor.namespace[var_name]

    # Clear cell state
    reactor.clear_cell_state(cell_id)

    reactor.set_cells(cells)
    save_notebook()
    await broadcast_cells_updated()

    return {"status": "deleted", "id": cell_id, "removed_variables": list(variables_to_remove)}


@app.post("/cells/{cell_id}/run")
async def run_cell(cell_id: str):
    """Run a cell and its downstream dependents."""
    cell = find_cell_by_id(cells, cell_id)
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    # Run in background to allow WebSocket updates
    results = reactor.run_cell(cell_id, sql_executor=execute_sql_cell)
    save_notebook()

    return {
        "results": [cell_state_to_dict(r) for r in results],
    }


@app.post("/cells/run-all")
async def run_all():
    """Run all cells in dependency order."""
    results = reactor.run_all_cells(sql_executor=execute_sql_cell)
    save_notebook()

    return {
        "results": [cell_state_to_dict(r) for r in results],
    }


@app.post("/cells/reset")
async def reset_notebook():
    """Reset all cell states and namespace."""
    reactor.reset()
    await broadcast_cells_updated()
    return {"status": "reset"}


@app.post("/cells/save")
async def save_notebook_endpoint():
    """Manually save the notebook to disk."""
    save_notebook()
    return {"status": "saved"}


# --- Database Configuration ---

@app.post("/config/db")
async def configure_database(config: DatabaseConfig):
    """Configure the Postgres database connection."""
    try:
        db_manager.connect(config.connection_string)
        return {"status": "connected"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/config/db")
async def get_database_status():
    """Get database connection status."""
    return {
        "connected": db_manager.is_connected(),
    }


@app.delete("/config/db")
async def disconnect_database():
    """Disconnect from database."""
    db_manager.close()
    return {"status": "disconnected"}


# --- WebSocket ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()
    websocket_connections.append(websocket)

    try:
        # Send initial state
        await websocket.send_json({
            "type": "init",
            "data": {
                "cells": [cell_to_response(c) for c in cells],
                "states": {cid: cell_state_to_dict(s) for cid, s in reactor.get_all_states().items()},
                "db_connected": db_manager.is_connected(),
            },
        })

        # Keep connection alive and handle incoming messages
        while True:
            try:
                data = await websocket.receive_json()
                # Handle client messages if needed
                if data.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                break

    finally:
        if websocket in websocket_connections:
            websocket_connections.remove(websocket)


# --- Static Files (Frontend) ---

# Serve frontend if directory exists
if os.path.exists(FRONTEND_DIR):
    @app.get("/")
    async def serve_frontend():
        """Serve the frontend HTML."""
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# --- Run ---

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
