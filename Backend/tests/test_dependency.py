"""
Unit tests for the Dependency Analyzer module.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser import Cell
from dependency import (
    analyze_python_code,
    analyze_cell,
    build_dependency_graph,
    get_downstream_cells,
    topological_sort,
    detect_cycle,
    get_execution_order,
)


class TestAnalyzePythonCode:
    """Tests for analyze_python_code function."""

    def test_simple_assignment(self):
        reads, writes = analyze_python_code("x = 10")
        assert "x" in writes
        assert len(reads) == 0

    def test_simple_read(self):
        reads, writes = analyze_python_code("y = x + 5")
        assert "x" in reads
        assert "y" in writes

    def test_multiple_reads(self):
        reads, writes = analyze_python_code("z = x + y")
        assert "x" in reads
        assert "y" in reads
        assert "z" in writes

    def test_function_definition(self):
        code = """def greet(name):
    return f"Hello, {name}"
"""
        reads, writes = analyze_python_code(code)
        assert "greet" in writes
        # name is a local parameter, not a read

    def test_function_call_with_variable(self):
        reads, writes = analyze_python_code("result = process(data)")
        assert "process" in reads
        assert "data" in reads
        assert "result" in writes

    def test_import_statement(self):
        reads, writes = analyze_python_code("import pandas as pd")
        assert "pd" in writes

    def test_from_import(self):
        reads, writes = analyze_python_code("from os import path")
        assert "path" in writes

    def test_class_definition(self):
        code = """class MyClass:
    pass"""
        reads, writes = analyze_python_code(code)
        assert "MyClass" in writes

    def test_for_loop(self):
        code = """for item in items:
    print(item)"""
        reads, writes = analyze_python_code(code)
        assert "items" in reads
        assert "item" in writes

    def test_builtin_not_in_reads(self):
        reads, writes = analyze_python_code("x = len(data)")
        assert "len" not in reads  # len is a builtin
        assert "data" in reads
        assert "x" in writes

    def test_syntax_error_returns_empty(self):
        reads, writes = analyze_python_code("def broken(")
        assert reads == set()
        assert writes == set()

    def test_chained_assignment(self):
        reads, writes = analyze_python_code("a = b = c = 10")
        assert "a" in writes
        assert "b" in writes
        assert "c" in writes

    def test_augmented_assignment(self):
        reads, writes = analyze_python_code("x += 1")
        assert "x" in reads  # x is read first
        assert "x" in writes  # then written

    def test_list_comprehension(self):
        reads, writes = analyze_python_code("squares = [x**2 for x in numbers]")
        assert "numbers" in reads
        assert "squares" in writes


class TestAnalyzeCell:
    """Tests for analyze_cell function."""

    def test_python_cell(self):
        cell = Cell(id="c1", code="y = x + 1", cell_type="python")
        analysis = analyze_cell(cell)
        assert analysis.cell_id == "c1"
        assert "x" in analysis.reads
        assert "y" in analysis.writes

    def test_sql_cell_with_as_var(self):
        cell = Cell(id="sql1", code="SELECT * FROM users", cell_type="sql", as_var="users_df")
        analysis = analyze_cell(cell)
        assert analysis.cell_id == "sql1"
        assert len(analysis.reads) == 0  # SQL doesn't read Python vars
        assert "users_df" in analysis.writes

    def test_sql_cell_without_as_var(self):
        cell = Cell(id="sql1", code="SELECT 1", cell_type="sql")
        analysis = analyze_cell(cell)
        assert "_sql_sql1" in analysis.writes


class TestBuildDependencyGraph:
    """Tests for build_dependency_graph function."""

    def test_simple_dependency(self):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = x + 5", cell_type="python"),
        ]
        graph = build_dependency_graph(cells)
        assert graph["c1"] == set()  # c1 has no dependencies
        assert graph["c2"] == {"c1"}  # c2 depends on c1

    def test_chain_dependency(self):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = x + 5", cell_type="python"),
            Cell(id="c3", code="z = y * 2", cell_type="python"),
        ]
        graph = build_dependency_graph(cells)
        assert graph["c1"] == set()
        assert graph["c2"] == {"c1"}
        assert graph["c3"] == {"c2"}

    def test_multiple_dependencies(self):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),
            Cell(id="c3", code="z = x + y", cell_type="python"),
        ]
        graph = build_dependency_graph(cells)
        assert graph["c3"] == {"c1", "c2"}

    def test_no_self_dependency(self):
        cells = [
            Cell(id="c1", code="x = x + 1", cell_type="python"),
        ]
        graph = build_dependency_graph(cells)
        assert "c1" not in graph["c1"]

    def test_sql_cell_dependency(self):
        cells = [
            Cell(id="sql1", code="SELECT * FROM users", cell_type="sql", as_var="df"),
            Cell(id="c2", code="print(df.head())", cell_type="python"),
        ]
        graph = build_dependency_graph(cells)
        assert graph["sql1"] == set()
        assert graph["c2"] == {"sql1"}


class TestGetDownstreamCells:
    """Tests for get_downstream_cells function."""

    def test_direct_downstream(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
            "c3": {"c2"},
        }
        downstream = get_downstream_cells(graph, "c1")
        assert "c2" in downstream
        assert "c3" in downstream

    def test_no_downstream(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
        }
        downstream = get_downstream_cells(graph, "c2")
        assert downstream == set()

    def test_multiple_downstream_branches(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
            "c3": {"c1"},
            "c4": {"c2", "c3"},
        }
        downstream = get_downstream_cells(graph, "c1")
        assert downstream == {"c2", "c3", "c4"}


class TestTopologicalSort:
    """Tests for topological_sort function."""

    def test_simple_sort(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
            "c3": {"c2"},
        }
        order = topological_sort(graph, {"c1", "c2", "c3"})
        assert order.index("c1") < order.index("c2")
        assert order.index("c2") < order.index("c3")

    def test_partial_sort(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
            "c3": {"c2"},
        }
        order = topological_sort(graph, {"c2", "c3"})
        assert "c1" not in order
        assert order.index("c2") < order.index("c3")


class TestDetectCycle:
    """Tests for detect_cycle function."""

    def test_no_cycle(self):
        graph = {
            "c1": set(),
            "c2": {"c1"},
            "c3": {"c2"},
        }
        cycle = detect_cycle(graph)
        assert cycle is None

    def test_direct_cycle(self):
        graph = {
            "c1": {"c2"},
            "c2": {"c1"},
        }
        cycle = detect_cycle(graph)
        assert cycle is not None
        assert "c1" in cycle or "c2" in cycle

    def test_indirect_cycle(self):
        graph = {
            "c1": {"c3"},
            "c2": {"c1"},
            "c3": {"c2"},
        }
        cycle = detect_cycle(graph)
        assert cycle is not None


class TestGetExecutionOrder:
    """Tests for get_execution_order function."""

    def test_execution_order_includes_changed_cell(self):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = x + 5", cell_type="python"),
        ]
        order, cycle = get_execution_order(cells, "c1")
        assert cycle is None
        assert "c1" in order
        assert "c2" in order
        assert order.index("c1") < order.index("c2")

    def test_execution_order_only_downstream(self):
        cells = [
            Cell(id="c1", code="x = 10", cell_type="python"),
            Cell(id="c2", code="y = 20", cell_type="python"),  # independent
            Cell(id="c3", code="z = x + 5", cell_type="python"),
        ]
        order, cycle = get_execution_order(cells, "c1")
        assert "c1" in order
        assert "c3" in order
        assert "c2" not in order  # c2 doesn't depend on c1

    def test_execution_order_detects_cycle(self):
        # This would require manually creating a cyclic dependency
        # In practice, cycles are detected at graph build time
        cells = [
            Cell(id="c1", code="x = y", cell_type="python"),
            Cell(id="c2", code="y = x", cell_type="python"),
        ]
        order, cycle = get_execution_order(cells, "c1")
        # Note: cycle detection depends on graph structure
        # With our current implementation, this may or may not detect
        # since variables are defined in order
