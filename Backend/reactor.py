"""
Reactive Runner Module

Orchestrates reactive execution of notebook cells:
- Computes which cells need to re-run when a cell changes
- Executes cells in dependency order
- Handles error propagation (blocked status)
- Provides status updates via callbacks
"""

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Any

from parser import Cell, find_cell_by_id
from dependency import get_execution_order, build_dependency_graph
from executor import Executor, ExecutionResult


class CellStatus(str, Enum):
    """Possible cell execution statuses."""
    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    BLOCKED = "blocked"


@dataclass
class CellState:
    """Runtime state of a cell."""
    cell_id: str
    status: CellStatus = CellStatus.IDLE
    output: Optional[str] = None
    output_type: str = "text"  # "text", "html", "error"
    stdout: str = ""
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    blocked_by: Optional[str] = None  # Cell ID that blocked this cell


# Type for status update callback
StatusCallback = Callable[[str, CellState], None]


class Reactor:
    """
    Manages reactive execution of notebook cells.
    """

    def __init__(self, executor: Optional[Executor] = None):
        self.executor = executor or Executor()
        self.cells: list[Cell] = []
        self.cell_states: dict[str, CellState] = {}
        self._status_callback: Optional[StatusCallback] = None

    def set_cells(self, cells: list[Cell]):
        """Set the cells to manage."""
        self.cells = cells
        # Initialize states for new cells
        for cell in cells:
            if cell.id not in self.cell_states:
                self.cell_states[cell.id] = CellState(cell_id=cell.id)

        # Remove states for deleted cells
        cell_ids = {c.id for c in cells}
        to_remove = [cid for cid in self.cell_states if cid not in cell_ids]
        for cid in to_remove:
            del self.cell_states[cid]

    def clear_cell_state(self, cell_id: str):
        """Clear state for a specific cell."""
        if cell_id in self.cell_states:
            del self.cell_states[cell_id]

    def set_status_callback(self, callback: StatusCallback):
        """Set callback for status updates (used for WebSocket notifications)."""
        self._status_callback = callback

    def _notify_status(self, cell_id: str, state: CellState):
        """Notify about status change via callback."""
        if self._status_callback:
            self._status_callback(cell_id, state)

    def _update_status(self, cell_id: str, **kwargs):
        """Update cell state and notify."""
        state = self.cell_states.get(cell_id)
        if state:
            for key, value in kwargs.items():
                if hasattr(state, key):
                    setattr(state, key, value)
            self._notify_status(cell_id, state)

    def run_cell(self, cell_id: str, sql_executor: Optional[Callable] = None) -> list[CellState]:
        """
        Run a cell and all its downstream dependents.

        Args:
            cell_id: The cell to run
            sql_executor: Optional callback for executing SQL cells

        Returns:
            List of CellState objects for all executed cells
        """
        # Get execution order
        order, cycle = get_execution_order(self.cells, cell_id)

        if cycle:
            # Circular dependency detected
            for cid in cycle:
                self._update_status(
                    cid,
                    status=CellStatus.ERROR,
                    error=f"Circular dependency detected: {' -> '.join(cycle)}",
                )
            return [self.cell_states[cid] for cid in cycle]

        results = []
        failed_cells: set[str] = set()
        graph = build_dependency_graph(self.cells)

        for cid in order:
            cell = find_cell_by_id(self.cells, cid)
            if not cell:
                continue

            # Check if any upstream cell failed
            upstream_deps = graph.get(cid, set())
            blocking_cell = None
            for dep in upstream_deps:
                if dep in failed_cells:
                    blocking_cell = dep
                    break

            if blocking_cell:
                # This cell is blocked by a failed upstream cell
                self._update_status(
                    cid,
                    status=CellStatus.BLOCKED,
                    blocked_by=blocking_cell,
                    error=f"Blocked by failed cell: {blocking_cell}",
                )
                failed_cells.add(cid)  # Propagate blocked status
                results.append(self.cell_states[cid])
                continue

            # Mark as running
            self._update_status(cid, status=CellStatus.RUNNING, blocked_by=None)

            # Execute the cell
            if cell.cell_type == "sql":
                if sql_executor:
                    exec_result = sql_executor(cell)
                else:
                    exec_result = ExecutionResult(
                        cell_id=cid,
                        success=False,
                        error="No database connection configured",
                    )
            else:
                exec_result = self.executor.execute_cell(cell)

            # Update state based on result
            if exec_result.success:
                self._update_status(
                    cid,
                    status=CellStatus.SUCCESS,
                    output=exec_result.result,
                    output_type=exec_result.result_type,
                    stdout=exec_result.stdout,
                    error=None,
                    error_traceback=None,
                    blocked_by=None,
                )
            else:
                self._update_status(
                    cid,
                    status=CellStatus.ERROR,
                    output=None,
                    stdout=exec_result.stdout,
                    error=exec_result.error,
                    error_traceback=exec_result.error_traceback,
                    blocked_by=None,
                )
                failed_cells.add(cid)

            results.append(self.cell_states[cid])

        return results

    def run_all_cells(self, sql_executor: Optional[Callable] = None) -> list[CellState]:
        """
        Run all cells in dependency order.

        Args:
            sql_executor: Optional callback for executing SQL cells

        Returns:
            List of CellState objects for all cells
        """
        if not self.cells:
            return []

        # Start from cells with no dependencies
        graph = build_dependency_graph(self.cells)

        # Find root cells (no dependencies)
        root_cells = [c.id for c in self.cells if not graph.get(c.id)]

        if not root_cells:
            # All cells have dependencies - just start with the first one
            root_cells = [self.cells[0].id]

        all_results = []
        executed = set()

        for root_id in root_cells:
            if root_id not in executed:
                results = self.run_cell(root_id, sql_executor)
                for r in results:
                    if r.cell_id not in executed:
                        all_results.append(r)
                        executed.add(r.cell_id)

        return all_results

    def get_cell_state(self, cell_id: str) -> Optional[CellState]:
        """Get the current state of a cell."""
        return self.cell_states.get(cell_id)

    def get_all_states(self) -> dict[str, CellState]:
        """Get states of all cells."""
        return self.cell_states.copy()

    def reset(self):
        """Reset all cell states and the executor namespace."""
        self.executor.reset_namespace()
        for state in self.cell_states.values():
            state.status = CellStatus.IDLE
            state.output = None
            state.stdout = ""
            state.error = None
            state.error_traceback = None
            state.blocked_by = None


def cell_state_to_dict(state: CellState) -> dict:
    """Convert CellState to dictionary for JSON serialization."""
    return {
        "cell_id": state.cell_id,
        "status": state.status.value,
        "output": state.output,
        "output_type": state.output_type,
        "stdout": state.stdout,
        "error": state.error,
        "error_traceback": state.error_traceback,
        "blocked_by": state.blocked_by,
    }
