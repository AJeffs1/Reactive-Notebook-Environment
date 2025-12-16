"""
Unit tests for the Cell Parser module.
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser import (
    Cell,
    parse_marker,
    parse_notebook,
    serialize_cell,
    serialize_notebook,
    create_cell,
    find_cell_by_id,
    remove_cell_by_id,
)


class TestParseMarker:
    """Tests for parse_marker function."""

    def test_parse_simple_id(self):
        result = parse_marker("id: abc123")
        assert result == {"id": "abc123"}

    def test_parse_id_and_type(self):
        result = parse_marker("id: abc123, type: sql")
        assert result == {"id": "abc123", "type": "sql"}

    def test_parse_full_marker(self):
        result = parse_marker("id: abc123, type: sql, as: users_df")
        assert result == {"id": "abc123", "type": "sql", "as": "users_df"}

    def test_parse_with_extra_spaces(self):
        result = parse_marker("id:  abc123 ,  type:  python")
        assert result == {"id": "abc123", "type": "python"}


class TestParseNotebook:
    """Tests for parse_notebook function."""

    def test_parse_empty_content(self):
        cells = parse_notebook("")
        assert cells == []

    def test_parse_single_python_cell(self):
        content = """# %% [id: cell1]
x = 10
y = 20"""
        cells = parse_notebook(content)
        assert len(cells) == 1
        assert cells[0].id == "cell1"
        assert cells[0].cell_type == "python"
        assert cells[0].code == "x = 10\ny = 20"

    def test_parse_multiple_cells(self):
        content = """# %% [id: cell1]
x = 10

# %% [id: cell2]
y = x + 5"""
        cells = parse_notebook(content)
        assert len(cells) == 2
        assert cells[0].id == "cell1"
        assert cells[1].id == "cell2"
        assert "x = 10" in cells[0].code
        assert "y = x + 5" in cells[1].code

    def test_parse_sql_cell(self):
        content = """# %% [id: sql1, type: sql, as: users_df]
SELECT * FROM users"""
        cells = parse_notebook(content)
        assert len(cells) == 1
        assert cells[0].id == "sql1"
        assert cells[0].cell_type == "sql"
        assert cells[0].as_var == "users_df"
        assert cells[0].code == "SELECT * FROM users"

    def test_parse_mixed_cells(self):
        content = """# %% [id: py1]
import pandas as pd

# %% [id: sql1, type: sql, as: df]
SELECT * FROM users

# %% [id: py2]
print(df.head())"""
        cells = parse_notebook(content)
        assert len(cells) == 3
        assert cells[0].cell_type == "python"
        assert cells[1].cell_type == "sql"
        assert cells[2].cell_type == "python"

    def test_parse_preserves_code_formatting(self):
        content = """# %% [id: cell1]
def hello():
    print("Hello")
    return True"""
        cells = parse_notebook(content)
        assert "    print" in cells[0].code  # Indentation preserved


class TestSerializeCell:
    """Tests for serialize_cell function."""

    def test_serialize_python_cell(self):
        cell = Cell(id="abc123", code="x = 10", cell_type="python")
        result = serialize_cell(cell)
        assert "# %% [id: abc123]" in result
        assert "x = 10" in result
        assert "type:" not in result  # python is default, not included

    def test_serialize_sql_cell(self):
        cell = Cell(id="sql1", code="SELECT * FROM users", cell_type="sql", as_var="df")
        result = serialize_cell(cell)
        assert "# %% [id: sql1, type: sql, as: df]" in result
        assert "SELECT * FROM users" in result

    def test_serialize_sql_cell_without_as(self):
        cell = Cell(id="sql1", code="SELECT 1", cell_type="sql")
        result = serialize_cell(cell)
        assert "type: sql" in result
        assert "as:" not in result


class TestSerializeNotebook:
    """Tests for serialize_notebook function."""

    def test_serialize_empty_notebook(self):
        result = serialize_notebook([])
        assert result == ""

    def test_serialize_roundtrip(self):
        """Parse then serialize should preserve content."""
        original = """# %% [id: cell1]
x = 10

# %% [id: cell2, type: sql, as: df]
SELECT * FROM users"""
        cells = parse_notebook(original)
        serialized = serialize_notebook(cells)
        reparsed = parse_notebook(serialized)

        assert len(reparsed) == 2
        assert reparsed[0].id == "cell1"
        assert reparsed[0].code == "x = 10"
        assert reparsed[1].id == "cell2"
        assert reparsed[1].cell_type == "sql"
        assert reparsed[1].as_var == "df"


class TestCellManagement:
    """Tests for cell management functions."""

    def test_create_cell_generates_id(self):
        cell = create_cell()
        assert cell.id is not None
        assert len(cell.id) == 8  # UUID hex[:8]

    def test_create_cell_with_params(self):
        cell = create_cell(cell_type="sql", code="SELECT 1", as_var="result")
        assert cell.cell_type == "sql"
        assert cell.code == "SELECT 1"
        assert cell.as_var == "result"

    def test_find_cell_by_id(self):
        cells = [
            Cell(id="a", code="1", cell_type="python"),
            Cell(id="b", code="2", cell_type="python"),
            Cell(id="c", code="3", cell_type="python"),
        ]
        found = find_cell_by_id(cells, "b")
        assert found is not None
        assert found.code == "2"

    def test_find_cell_by_id_not_found(self):
        cells = [Cell(id="a", code="1", cell_type="python")]
        found = find_cell_by_id(cells, "nonexistent")
        assert found is None

    def test_remove_cell_by_id(self):
        cells = [
            Cell(id="a", code="1", cell_type="python"),
            Cell(id="b", code="2", cell_type="python"),
        ]
        result = remove_cell_by_id(cells, "a")
        assert result is True
        assert len(cells) == 1
        assert cells[0].id == "b"

    def test_remove_cell_by_id_not_found(self):
        cells = [Cell(id="a", code="1", cell_type="python")]
        result = remove_cell_by_id(cells, "nonexistent")
        assert result is False
        assert len(cells) == 1


class TestCellDataclass:
    """Tests for Cell dataclass."""

    def test_cell_to_dict(self):
        cell = Cell(id="abc", code="x = 1", cell_type="python", as_var=None)
        d = cell.to_dict()
        assert d["id"] == "abc"
        assert d["code"] == "x = 1"
        assert d["type"] == "python"
        assert d["as"] is None

    def test_cell_from_dict(self):
        d = {"id": "abc", "code": "x = 1", "type": "sql", "as": "df"}
        cell = Cell.from_dict(d)
        assert cell.id == "abc"
        assert cell.code == "x = 1"
        assert cell.cell_type == "sql"
        assert cell.as_var == "df"
