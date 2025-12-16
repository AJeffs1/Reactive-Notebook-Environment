"""
Execution Engine Module

Executes Python code cells with:
- Shared namespace (globals dict)
- Output capture (stdout, _result variable)
- DataFrame rendering (to_html conversion)
- Error handling
"""

import io
import sys
import contextlib
import traceback
from dataclasses import dataclass
from typing import Any, Optional

from parser import Cell


@dataclass
class ExecutionResult:
    """Result of executing a cell."""
    cell_id: str
    success: bool
    stdout: str = ""
    result: Optional[str] = None  # Rendered result (HTML or text)
    result_type: str = "text"  # "text", "html", "error"
    error: Optional[str] = None
    error_traceback: Optional[str] = None


class Executor:
    """
    Executes Python code cells with a shared namespace.
    """

    def __init__(self):
        # Shared namespace for all cells
        self.namespace: dict[str, Any] = {
            "__builtins__": __builtins__,
        }
        # Pre-import common libraries
        self._setup_namespace()

    def _setup_namespace(self):
        """Pre-import common libraries into the namespace."""
        setup_code = """
import pandas as pd
import numpy as np
"""
        try:
            exec(setup_code, self.namespace)
        except ImportError:
            # Libraries not available, that's fine
            pass

    def reset_namespace(self):
        """Reset the namespace to initial state."""
        self.namespace.clear()
        self.namespace["__builtins__"] = __builtins__
        self._setup_namespace()

    def execute_cell(self, cell: Cell) -> ExecutionResult:
        """
        Execute a single cell.

        For Python cells: executes code with exec()
        For SQL cells: handled separately (see database.py)

        Args:
            cell: The Cell to execute

        Returns:
            ExecutionResult with success status and output
        """
        if cell.cell_type == "sql":
            # SQL cells are handled by the database module
            # This should not be called directly for SQL cells
            return ExecutionResult(
                cell_id=cell.id,
                success=False,
                error="SQL cells must be executed through the database module",
            )

        return self._execute_python_cell(cell)

    def _execute_python_cell(self, cell: Cell) -> ExecutionResult:
        """Execute a Python cell."""
        code = cell.code.strip()

        if not code:
            return ExecutionResult(
                cell_id=cell.id,
                success=True,
                stdout="",
                result=None,
            )

        # Capture stdout
        stdout_capture = io.StringIO()

        try:
            with contextlib.redirect_stdout(stdout_capture):
                with contextlib.redirect_stderr(stdout_capture):
                    exec(code, self.namespace)

            # Get captured stdout
            stdout = stdout_capture.getvalue()

            # Check for _result variable (convention for displaying values)
            result = None
            result_type = "text"

            if "_result" in self.namespace:
                raw_result = self.namespace.pop("_result")
                result, result_type = self._render_result(raw_result)

            return ExecutionResult(
                cell_id=cell.id,
                success=True,
                stdout=stdout,
                result=result,
                result_type=result_type,
            )

        except Exception as e:
            # Capture the exception
            stdout = stdout_capture.getvalue()
            error_tb = traceback.format_exc()

            return ExecutionResult(
                cell_id=cell.id,
                success=False,
                stdout=stdout,
                error=str(e),
                error_traceback=error_tb,
                result_type="error",
            )

    def _render_result(self, value: Any) -> tuple[str, str]:
        """
        Render a result value to a displayable format.

        Args:
            value: The value to render

        Returns:
            Tuple of (rendered_string, type) where type is "html" or "text"
        """
        # Check for DataFrame-like objects (pandas)
        if hasattr(value, 'to_html'):
            try:
                # Limit rows for large DataFrames
                if hasattr(value, 'shape') and value.shape[0] > 50:
                    html = value.head(50).to_html(classes='dataframe', index=True)
                    html += f"<p><em>Showing 50 of {value.shape[0]} rows</em></p>"
                else:
                    html = value.to_html(classes='dataframe', index=True)
                return html, "html"
            except Exception:
                pass

        # Check for matplotlib figures
        if hasattr(value, 'savefig'):
            try:
                buf = io.BytesIO()
                value.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                import base64
                img_str = base64.b64encode(buf.read()).decode()
                html = f'<img src="data:image/png;base64,{img_str}" />'
                return html, "html"
            except Exception:
                pass

        # Default: convert to string representation
        try:
            return repr(value), "text"
        except Exception:
            return str(value), "text"

    def get_variable(self, name: str) -> Any:
        """Get a variable from the namespace."""
        return self.namespace.get(name)

    def set_variable(self, name: str, value: Any):
        """Set a variable in the namespace."""
        self.namespace[name] = value

    def inject_sql_result(self, var_name: str, data: Any):
        """
        Inject SQL query results into the namespace.

        Args:
            var_name: Variable name to store the result
            data: The data (typically a DataFrame)
        """
        self.namespace[var_name] = data


def format_output(result: ExecutionResult) -> dict:
    """
    Format execution result for API response.

    Args:
        result: ExecutionResult from execution

    Returns:
        Dictionary suitable for JSON serialization
    """
    output = {
        "cell_id": result.cell_id,
        "success": result.success,
        "stdout": result.stdout,
    }

    if result.result is not None:
        output["result"] = result.result
        output["result_type"] = result.result_type

    if result.error:
        output["error"] = result.error
        output["error_traceback"] = result.error_traceback

    return output
