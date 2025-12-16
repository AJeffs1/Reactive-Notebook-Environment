"""
Unit tests for the Reactive Runner module.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser import Cell
from reactor import Reactor, CellStatus, CellState, cell_state_to_dict


class TestReactor:
    """Tests for Reactor class."""

    @pytest.fixture
    def reactor(self):
        """Create a fresh reactor for each test."""
        return Reactor()

    def test_set_cells(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),
        ]
        reactor.set_cells(cells)
        assert len(reactor.cells) == 2
        assert "c1" in reactor.cell_states
        assert "c2" in reactor.cell_states

    def test_run_single_cell(self, reactor):
        cells = [Cell(id="c1", code="x = 10", cell_type="python")]
        reactor.set_cells(cells)

        results = reactor.run_cell("c1")

        assert len(results) == 1
        assert results[0].status == CellStatus.SUCCESS
        assert reactor.executor.get_variable("x") == 10

    def test_run_cell_triggers_downstream(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = x + 5", cell_type="python"),
            Cell(id="c3", code="z = y * 2", cell_type="python"),
        ]
        reactor.set_cells(cells)

        results = reactor.run_cell("c1")

        # All three cells should be executed
        assert len(results) == 3
        assert all(r.status == CellStatus.SUCCESS for r in results)
        assert reactor.executor.get_variable("x") == 10
        assert reactor.executor.get_variable("y") == 15
        assert reactor.executor.get_variable("z") == 30

    def test_run_cell_only_affects_downstream(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),  # independent
            Cell(id="c3", code="z = x + 5", cell_type="python"),
        ]
        reactor.set_cells(cells)

        results = reactor.run_cell("c1")

        # Only c1 and c3 should be executed (c2 is independent)
        executed_ids = {r.cell_id for r in results}
        assert "c1" in executed_ids
        assert "c3" in executed_ids
        assert "c2" not in executed_ids

    def test_error_blocks_downstream(self, reactor):
        cells = [
            Cell(id="c1", code="x = 1/0", cell_type="python"),  # Error
            Cell(id="c2", code="y = x + 5", cell_type="python"),
        ]
        reactor.set_cells(cells)

        results = reactor.run_cell("c1")

        assert len(results) == 2
        assert results[0].status == CellStatus.ERROR
        assert results[1].status == CellStatus.BLOCKED
        assert results[1].blocked_by == "c1"

    def test_blocked_propagates_through_chain(self, reactor):
        cells = [
            Cell(id="c1", code="x = 1/0", cell_type="python"),  # Error
            Cell(id="c2", code="y = x + 5", cell_type="python"),
            Cell(id="c3", code="z = y * 2", cell_type="python"),
        ]
        reactor.set_cells(cells)

        results = reactor.run_cell("c1")

        assert results[0].status == CellStatus.ERROR
        assert results[1].status == CellStatus.BLOCKED
        assert results[2].status == CellStatus.BLOCKED

    def test_run_all_cells(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),
            Cell(id="c3", code="z = x + y", cell_type="python"),
        ]
        reactor.set_cells(cells)

        results = reactor.run_all_cells()

        assert len(results) >= 2  # At least the root cells
        assert reactor.executor.get_variable("x") == 10
        assert reactor.executor.get_variable("y") == 20
        assert reactor.executor.get_variable("z") == 30

    def test_get_cell_state(self, reactor):
        cells = [Cell(id="c1", code="x = 10", cell_type="python")]
        reactor.set_cells(cells)

        state = reactor.get_cell_state("c1")
        assert state is not None
        assert state.cell_id == "c1"
        assert state.status == CellStatus.IDLE

    def test_get_all_states(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),
        ]
        reactor.set_cells(cells)

        states = reactor.get_all_states()
        assert len(states) == 2
        assert "c1" in states
        assert "c2" in states

    def test_reset(self, reactor):
        cells = [Cell(id="c1", code="x = 10", cell_type="python")]
        reactor.set_cells(cells)
        reactor.run_cell("c1")

        assert reactor.executor.get_variable("x") == 10
        assert reactor.cell_states["c1"].status == CellStatus.SUCCESS

        reactor.reset()

        assert reactor.executor.get_variable("x") is None
        assert reactor.cell_states["c1"].status == CellStatus.IDLE

    def test_status_callback_called(self, reactor):
        """Test that status callback is invoked during execution."""
        status_updates = []

        def callback(cell_id, state):
            status_updates.append((cell_id, state.status))

        reactor.set_status_callback(callback)
        cells = [Cell(id="c1", code="x = 10", cell_type="python")]
        reactor.set_cells(cells)

        reactor.run_cell("c1")

        # Should have at least RUNNING and SUCCESS status updates
        statuses = [s for _, s in status_updates if _ == "c1"]
        assert CellStatus.RUNNING in statuses
        assert CellStatus.SUCCESS in statuses

    def test_cell_removal_cleans_state(self, reactor):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),
        ]
        reactor.set_cells(cells)
        assert "c1" in reactor.cell_states
        assert "c2" in reactor.cell_states

        # Remove c2
        reactor.set_cells([cells[0]])
        assert "c1" in reactor.cell_states
        assert "c2" not in reactor.cell_states


class TestCellState:
    """Tests for CellState dataclass."""

    def test_default_state(self):
        state = CellState(cell_id="c1")
        assert state.cell_id == "c1"
        assert state.status == CellStatus.IDLE
        assert state.output is None
        assert state.error is None

    def test_state_with_output(self):
        state = CellState(
            cell_id="c1",
            status=CellStatus.SUCCESS,
            output="<table>...</table>",
            output_type="html",
            stdout="printed output",
        )
        assert state.status == CellStatus.SUCCESS
        assert state.output == "<table>...</table>"
        assert state.output_type == "html"

    def test_state_with_error(self):
        state = CellState(
            cell_id="c1",
            status=CellStatus.ERROR,
            error="ZeroDivisionError",
            error_traceback="Traceback...",
        )
        assert state.status == CellStatus.ERROR
        assert state.error == "ZeroDivisionError"

    def test_blocked_state(self):
        state = CellState(
            cell_id="c2",
            status=CellStatus.BLOCKED,
            blocked_by="c1",
        )
        assert state.status == CellStatus.BLOCKED
        assert state.blocked_by == "c1"


class TestCellStateToDict:
    """Tests for cell_state_to_dict function."""

    def test_converts_to_dict(self):
        state = CellState(
            cell_id="c1",
            status=CellStatus.SUCCESS,
            output="result",
            output_type="text",
            stdout="output",
        )
        d = cell_state_to_dict(state)

        assert d["cell_id"] == "c1"
        assert d["status"] == "success"
        assert d["output"] == "result"
        assert d["output_type"] == "text"
        assert d["stdout"] == "output"

    def test_converts_error_state(self):
        state = CellState(
            cell_id="c1",
            status=CellStatus.ERROR,
            error="Error message",
            error_traceback="Traceback...",
        )
        d = cell_state_to_dict(state)

        assert d["status"] == "error"
        assert d["error"] == "Error message"
        assert d["error_traceback"] == "Traceback..."


class TestCellStatus:
    """Tests for CellStatus enum."""

    def test_status_values(self):
        assert CellStatus.IDLE.value == "idle"
        assert CellStatus.RUNNING.value == "running"
        assert CellStatus.SUCCESS.value == "success"
        assert CellStatus.ERROR.value == "error"
        assert CellStatus.BLOCKED.value == "blocked"

    def test_status_is_string_enum(self):
        assert str(CellStatus.SUCCESS) == "CellStatus.SUCCESS"
        assert CellStatus.SUCCESS == "success"


class TestReactiveExecution:
    """Integration tests for reactive execution behavior."""

    @pytest.fixture
    def reactor(self):
        return Reactor()

    def test_spreadsheet_like_updates(self, reactor):
        """Test that changing upstream value propagates correctly."""
        cells = [
            Cell(id="c1", code="price = 100", cell_type="python"),
            Cell(id="c2", code="tax_rate = 0.1", cell_type="python"),
            Cell(id="c3", code="tax = price * tax_rate", cell_type="python"),
            Cell(id="c4", code="total = price + tax", cell_type="python"),
        ]
        reactor.set_cells(cells)

        # Initial run
        reactor.run_all_cells()
        assert reactor.executor.get_variable("total") == 110

        # Change price
        cells[0].code = "price = 200"
        reactor.set_cells(cells)
        reactor.run_cell("c1")

        # tax and total should be updated
        assert reactor.executor.get_variable("price") == 200
        assert reactor.executor.get_variable("tax") == 20
        assert reactor.executor.get_variable("total") == 220

    def test_independent_cells_not_affected(self, reactor):
        """Test that independent cells are not re-executed."""
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),  # independent
        ]
        reactor.set_cells(cells)

        # Run c1
        results = reactor.run_cell("c1")

        # Only c1 should be in results
        assert len(results) == 1
        assert results[0].cell_id == "c1"
