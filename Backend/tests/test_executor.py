"""
Unit tests for the Execution Engine module.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser import Cell
from executor import Executor, ExecutionResult, format_output


class TestExecutor:
    """Tests for Executor class."""

    @pytest.fixture
    def executor(self):
        """Create a fresh executor for each test."""
        return Executor()

    def test_execute_simple_assignment(self, executor):
        cell = Cell(id="c1", code="x = 10", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True
        assert executor.get_variable("x") == 10

    def test_execute_uses_previous_variables(self, executor):
        cell1 = Cell(id="c1", code="x = 10", cell_type="python")
        cell2 = Cell(id="c2", code="y = x + 5", cell_type="python")

        executor.execute_cell(cell1)
        result = executor.execute_cell(cell2)

        assert result.success is True
        assert executor.get_variable("y") == 15

    def test_execute_captures_stdout(self, executor):
        cell = Cell(id="c1", code='print("Hello, World!")', cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True
        assert "Hello, World!" in result.stdout

    def test_execute_captures_multiple_prints(self, executor):
        cell = Cell(id="c1", code='print("line1")\nprint("line2")', cell_type="python")
        result = executor.execute_cell(cell)
        assert "line1" in result.stdout
        assert "line2" in result.stdout

    def test_execute_handles_result_variable(self, executor):
        cell = Cell(id="c1", code="_result = 42", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True
        assert result.result == "42"
        # _result should be removed from namespace after capture
        assert "_result" not in executor.namespace

    def test_execute_handles_error(self, executor):
        cell = Cell(id="c1", code="x = 1/0", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is False
        assert result.error is not None
        assert "division by zero" in result.error.lower()

    def test_execute_handles_name_error(self, executor):
        cell = Cell(id="c1", code="print(undefined_variable)", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is False
        assert "undefined_variable" in result.error

    def test_execute_handles_syntax_error(self, executor):
        cell = Cell(id="c1", code="def broken(", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is False
        assert result.error is not None

    def test_execute_empty_code(self, executor):
        cell = Cell(id="c1", code="", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True
        assert result.stdout == ""

    def test_execute_whitespace_only(self, executor):
        cell = Cell(id="c1", code="   \n\t\n   ", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True

    def test_execute_sql_cell_returns_error(self, executor):
        cell = Cell(id="sql1", code="SELECT * FROM users", cell_type="sql")
        result = executor.execute_cell(cell)
        assert result.success is False
        assert "database module" in result.error.lower()

    def test_reset_namespace(self, executor):
        cell = Cell(id="c1", code="x = 100", cell_type="python")
        executor.execute_cell(cell)
        assert executor.get_variable("x") == 100

        executor.reset_namespace()
        assert executor.get_variable("x") is None

    def test_set_variable(self, executor):
        executor.set_variable("test_var", [1, 2, 3])
        assert executor.get_variable("test_var") == [1, 2, 3]

    def test_inject_sql_result(self, executor):
        executor.inject_sql_result("df", {"col": [1, 2, 3]})
        assert executor.get_variable("df") == {"col": [1, 2, 3]}

    def test_namespace_persists_across_cells(self, executor):
        cells = [
            Cell(id="c1", code="a = 1", cell_type="python"),
            Cell(id="c2", code="b = 2", cell_type="python"),
            Cell(id="c3", code="c = a + b", cell_type="python"),
        ]
        for cell in cells:
            executor.execute_cell(cell)

        assert executor.get_variable("a") == 1
        assert executor.get_variable("b") == 2
        assert executor.get_variable("c") == 3

    def test_execute_with_imports(self, executor):
        cell = Cell(id="c1", code="import math\nx = math.sqrt(16)", cell_type="python")
        result = executor.execute_cell(cell)
        assert result.success is True
        assert executor.get_variable("x") == 4.0

    def test_execute_function_definition_and_call(self, executor):
        cell1 = Cell(id="c1", code="def double(n):\n    return n * 2", cell_type="python")
        cell2 = Cell(id="c2", code="result = double(21)", cell_type="python")

        executor.execute_cell(cell1)
        executor.execute_cell(cell2)

        assert executor.get_variable("result") == 42

    def test_execute_class_definition_and_use(self, executor):
        cell1 = Cell(id="c1", code="""class Counter:
    def __init__(self):
        self.count = 0
    def inc(self):
        self.count += 1
        return self.count""", cell_type="python")
        cell2 = Cell(id="c2", code="c = Counter()\nc.inc()\nc.inc()\nresult = c.count", cell_type="python")

        executor.execute_cell(cell1)
        executor.execute_cell(cell2)

        assert executor.get_variable("result") == 2


class TestExecutionResult:
    """Tests for ExecutionResult dataclass."""

    def test_result_success(self):
        result = ExecutionResult(
            cell_id="c1",
            success=True,
            stdout="Hello",
            result="42",
            result_type="text",
        )
        assert result.cell_id == "c1"
        assert result.success is True
        assert result.stdout == "Hello"
        assert result.result == "42"

    def test_result_error(self):
        result = ExecutionResult(
            cell_id="c1",
            success=False,
            error="ZeroDivisionError",
            error_traceback="Traceback...",
        )
        assert result.success is False
        assert result.error == "ZeroDivisionError"


class TestFormatOutput:
    """Tests for format_output function."""

    def test_format_success(self):
        result = ExecutionResult(
            cell_id="c1",
            success=True,
            stdout="output",
            result="42",
            result_type="text",
        )
        output = format_output(result)
        assert output["cell_id"] == "c1"
        assert output["success"] is True
        assert output["stdout"] == "output"
        assert output["result"] == "42"

    def test_format_error(self):
        result = ExecutionResult(
            cell_id="c1",
            success=False,
            stdout="",
            error="Error message",
            error_traceback="Traceback...",
        )
        output = format_output(result)
        assert output["success"] is False
        assert output["error"] == "Error message"
        assert output["error_traceback"] == "Traceback..."


class TestDataFrameRendering:
    """Tests for DataFrame rendering (requires pandas)."""

    @pytest.fixture
    def executor(self):
        return Executor()

    def test_dataframe_result_renders_html(self, executor):
        cell = Cell(id="c1", code="""import pandas as pd
df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
_result = df""", cell_type="python")
        result = executor.execute_cell(cell)

        assert result.success is True
        assert result.result_type == "html"
        assert "<table" in result.result
        assert "dataframe" in result.result
