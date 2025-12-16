"""
Dependency Analyzer Module

Uses Python's ast module to analyze code and extract:
- Variables read (dependencies)
- Variables written (outputs)
- Build dependency graph between cells
- Detect circular dependencies
"""

import ast
from dataclasses import dataclass
from typing import Optional

from parser import Cell


@dataclass
class CellAnalysis:
    """Result of analyzing a cell's code."""
    cell_id: str
    reads: set[str]   # Variables this cell reads
    writes: set[str]  # Variables this cell writes/defines


class VariableVisitor(ast.NodeVisitor):
    """
    AST visitor that extracts variable reads and writes from Python code.
    """

    def __init__(self):
        self.reads: set[str] = set()
        self.writes: set[str] = set()
        # Reads that must come from upstream (e.g., augmented assignments)
        self.required_reads: set[str] = set()
        # Track variables in current scope to avoid false positives
        self._local_scope: set[str] = set()

    def visit_Name(self, node: ast.Name):
        """Handle variable references."""
        if isinstance(node.ctx, ast.Load):
            # Reading a variable - only count as dependency if not locally defined
            if node.id not in self._local_scope:
                self.reads.add(node.id)
        elif isinstance(node.ctx, ast.Store):
            # Writing to a variable
            self.writes.add(node.id)
            self._local_scope.add(node.id)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign):
        """Handle augmented assignments like x += 1, x -= 1, etc."""
        # The target is both read and written
        if isinstance(node.target, ast.Name):
            # Read first (to get current value) - this is a required read
            # even though we also write to it
            if node.target.id not in self._local_scope:
                self.reads.add(node.target.id)
                self.required_reads.add(node.target.id)
            # Then write
            self.writes.add(node.target.id)
            self._local_scope.add(node.target.id)
        # Visit the value being added/subtracted/etc
        self.visit(node.value)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """Handle function definitions - the function name is written."""
        self.writes.add(node.name)
        self._local_scope.add(node.name)
        # Don't recurse into function body - those are local variables
        # But we do want to capture variables used in default arguments
        for default in node.args.defaults:
            self.visit(default)
        for default in node.args.kw_defaults:
            if default:
                self.visit(default)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        """Handle async function definitions."""
        self.writes.add(node.name)
        self._local_scope.add(node.name)
        for default in node.args.defaults:
            self.visit(default)
        for default in node.args.kw_defaults:
            if default:
                self.visit(default)

    def visit_ClassDef(self, node: ast.ClassDef):
        """Handle class definitions - the class name is written."""
        self.writes.add(node.name)
        self._local_scope.add(node.name)
        # Visit base classes as they are dependencies
        for base in node.bases:
            self.visit(base)
        # Don't recurse into class body

    def visit_Import(self, node: ast.Import):
        """Handle import statements."""
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name.split('.')[0]
            self.writes.add(name)
            self._local_scope.add(name)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        """Handle from ... import statements."""
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name
            if name != '*':
                self.writes.add(name)
                self._local_scope.add(name)

    def visit_For(self, node: ast.For):
        """Handle for loops - loop variable is written."""
        # Visit the target to capture the loop variable
        self._visit_target(node.target)
        # Visit the iterable (it's a read)
        self.visit(node.iter)
        # Visit the body
        for stmt in node.body:
            self.visit(stmt)
        for stmt in node.orelse:
            self.visit(stmt)

    def visit_comprehension(self, node: ast.comprehension):
        """Handle comprehension targets."""
        self._visit_target(node.target)
        self.visit(node.iter)
        for if_clause in node.ifs:
            self.visit(if_clause)

    def _visit_target(self, target):
        """Helper to visit assignment targets."""
        if isinstance(target, ast.Name):
            self.writes.add(target.id)
            self._local_scope.add(target.id)
        elif isinstance(target, ast.Tuple) or isinstance(target, ast.List):
            for elt in target.elts:
                self._visit_target(elt)


def analyze_python_code(code: str) -> tuple[set[str], set[str]]:
    """
    Analyze Python code to extract variable reads and writes.

    Args:
        code: Python source code string

    Returns:
        Tuple of (reads, writes) sets of variable names
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        # If code has syntax errors, return empty sets
        return set(), set()

    visitor = VariableVisitor()
    visitor.visit(tree)

    # Filter out built-in names from reads
    builtins = set(dir(__builtins__)) if isinstance(__builtins__, dict) else set(dir(__builtins__))
    common_builtins = {
        'print', 'len', 'range', 'str', 'int', 'float', 'list', 'dict', 'set',
        'tuple', 'bool', 'type', 'isinstance', 'hasattr', 'getattr', 'setattr',
        'open', 'file', 'input', 'output', 'sum', 'min', 'max', 'abs', 'round',
        'sorted', 'reversed', 'enumerate', 'zip', 'map', 'filter', 'any', 'all',
        'None', 'True', 'False', 'Exception', 'ValueError', 'TypeError', 'KeyError',
        '__name__', '__file__', '__doc__',
    }

    # Filter out builtins and local writes, but keep required_reads
    reads = (visitor.reads - common_builtins - visitor.writes) | visitor.required_reads

    return reads, visitor.writes


def analyze_cell(cell: Cell) -> CellAnalysis:
    """
    Analyze a cell to determine its dependencies and outputs.

    For SQL cells, the 'as' variable is the output.
    For Python cells, we parse the code with AST.

    Args:
        cell: The Cell object to analyze

    Returns:
        CellAnalysis with reads and writes
    """
    if cell.cell_type == "sql":
        # SQL cells don't read Python variables (for now)
        # They write to their 'as' variable
        writes = {cell.as_var} if cell.as_var else {f"_sql_{cell.id}"}
        return CellAnalysis(
            cell_id=cell.id,
            reads=set(),
            writes=writes,
        )
    else:
        # Python cell - use AST analysis
        reads, writes = analyze_python_code(cell.code)
        return CellAnalysis(
            cell_id=cell.id,
            reads=reads,
            writes=writes,
        )


def build_dependency_graph(cells: list[Cell]) -> dict[str, set[str]]:
    """
    Build a dependency graph from a list of cells.

    The graph maps cell_id -> set of upstream cell_ids that it depends on.

    Args:
        cells: List of Cell objects

    Returns:
        Dictionary mapping cell_id to set of cell_ids it depends on
    """
    # First, analyze all cells
    analyses = {cell.id: analyze_cell(cell) for cell in cells}

    # Build a map of variable -> cell_id that writes it
    # If multiple cells write the same variable, the later one wins
    var_to_cell: dict[str, str] = {}
    for cell in cells:
        analysis = analyses[cell.id]
        for var in analysis.writes:
            var_to_cell[var] = cell.id

    # Now build the dependency graph
    graph: dict[str, set[str]] = {}

    for cell in cells:
        analysis = analyses[cell.id]
        dependencies = set()

        for var in analysis.reads:
            if var in var_to_cell:
                upstream_cell = var_to_cell[var]
                if upstream_cell != cell.id:  # Don't depend on self
                    dependencies.add(upstream_cell)

        graph[cell.id] = dependencies

    return graph


def get_downstream_cells(graph: dict[str, set[str]], cell_id: str) -> set[str]:
    """
    Get all cells that depend on a given cell (transitively).

    Args:
        graph: Dependency graph (cell_id -> upstream dependencies)
        cell_id: The cell to find dependents of

    Returns:
        Set of cell_ids that depend on the given cell
    """
    # Invert the graph to get downstream relationships
    downstream: dict[str, set[str]] = {cid: set() for cid in graph}

    for cid, deps in graph.items():
        for dep in deps:
            if dep in downstream:
                downstream[dep].add(cid)

    # BFS to find all transitive dependents
    result = set()
    queue = list(downstream.get(cell_id, set()))

    while queue:
        current = queue.pop(0)
        if current not in result:
            result.add(current)
            queue.extend(downstream.get(current, set()))

    return result


def topological_sort(graph: dict[str, set[str]], cell_ids: set[str]) -> list[str]:
    """
    Topologically sort a subset of cells based on dependencies.

    Args:
        graph: Full dependency graph
        cell_ids: Subset of cell_ids to sort

    Returns:
        List of cell_ids in execution order (dependencies first)
    """
    # Filter graph to only include requested cells
    subgraph = {
        cid: deps & cell_ids
        for cid, deps in graph.items()
        if cid in cell_ids
    }

    result = []
    visited = set()
    temp_visited = set()

    def visit(node: str):
        if node in temp_visited:
            return  # Cycle detected, but we'll handle elsewhere
        if node in visited:
            return

        temp_visited.add(node)

        for dep in subgraph.get(node, set()):
            visit(dep)

        temp_visited.remove(node)
        visited.add(node)
        result.append(node)

    for cell_id in cell_ids:
        if cell_id not in visited:
            visit(cell_id)

    return result


def detect_cycle(graph: dict[str, set[str]]) -> Optional[list[str]]:
    """
    Detect if there's a circular dependency in the graph.

    Args:
        graph: Dependency graph (cell_id -> upstream dependencies)

    Returns:
        List of cell_ids forming a cycle, or None if no cycle exists
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in graph}
    parent = {node: None for node in graph}

    def dfs(node: str) -> Optional[list[str]]:
        color[node] = GRAY

        for neighbor in graph.get(node, set()):
            if neighbor not in color:
                continue

            if color[neighbor] == GRAY:
                # Found a cycle - reconstruct it
                cycle = [neighbor, node]
                current = node
                while parent[current] and parent[current] != neighbor:
                    current = parent[current]
                    cycle.append(current)
                return cycle

            if color[neighbor] == WHITE:
                parent[neighbor] = node
                result = dfs(neighbor)
                if result:
                    return result

        color[node] = BLACK
        return None

    for node in graph:
        if color[node] == WHITE:
            result = dfs(node)
            if result:
                return result

    return None


def get_execution_order(cells: list[Cell], changed_cell_id: str) -> tuple[list[str], Optional[list[str]]]:
    """
    Get the execution order for cells after a cell changes.

    Args:
        cells: All cells in the notebook
        changed_cell_id: The cell that was modified/run

    Returns:
        Tuple of (execution_order, cycle) where:
        - execution_order: List of cell_ids to execute (including changed cell)
        - cycle: List of cell_ids forming a cycle, or None
    """
    graph = build_dependency_graph(cells)

    # Check for cycles
    cycle = detect_cycle(graph)
    if cycle:
        return [], cycle

    # Get downstream cells
    downstream = get_downstream_cells(graph, changed_cell_id)

    # Include the changed cell itself
    to_execute = downstream | {changed_cell_id}

    # Sort in execution order
    order = topological_sort(graph, to_execute)

    return order, None
