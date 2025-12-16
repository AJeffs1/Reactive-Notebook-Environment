"""
Cell Parser Module

Parses .py files with cell markers in the format:
    # %% [id: abc123, type: python|sql, as: varname]

Extracts cells and can serialize them back to .py format.
"""

import re
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Cell:
    """Represents a single notebook cell."""
    id: str
    code: str
    cell_type: str = "python"  # "python" or "sql"
    as_var: Optional[str] = None  # Variable name for SQL results

    def to_dict(self) -> dict:
        """Convert cell to dictionary representation."""
        return {
            "id": self.id,
            "code": self.code,
            "type": self.cell_type,
            "as": self.as_var,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Cell":
        """Create cell from dictionary."""
        return cls(
            id=data["id"],
            code=data.get("code", ""),
            cell_type=data.get("type", "python"),
            as_var=data.get("as"),
        )


# Regex pattern for cell marker: # %% [id: xxx, type: yyy, as: zzz]
# All fields except id are optional
CELL_MARKER_PATTERN = re.compile(
    r'^# %%\s*\[([^\]]+)\]\s*$'
)


def parse_marker(marker_content: str) -> dict:
    """
    Parse the content inside the cell marker brackets.

    Example: "id: abc123, type: sql, as: users_df"
    Returns: {"id": "abc123", "type": "sql", "as": "users_df"}
    """
    result = {}
    parts = marker_content.split(',')

    for part in parts:
        part = part.strip()
        if ':' in part:
            key, value = part.split(':', 1)
            result[key.strip()] = value.strip()

    return result


def generate_cell_id() -> str:
    """Generate a unique cell ID."""
    return uuid.uuid4().hex[:8]


def parse_notebook(content: str) -> list[Cell]:
    """
    Parse notebook content into a list of cells.

    Args:
        content: The raw content of a .py notebook file

    Returns:
        List of Cell objects
    """
    cells = []
    lines = content.split('\n')

    current_cell = None
    current_code_lines = []

    for line in lines:
        match = CELL_MARKER_PATTERN.match(line)

        if match:
            # Save previous cell if exists
            if current_cell is not None:
                current_cell.code = '\n'.join(current_code_lines).strip()
                cells.append(current_cell)

            # Parse new cell marker
            marker_data = parse_marker(match.group(1))

            cell_id = marker_data.get('id', generate_cell_id())
            cell_type = marker_data.get('type', 'python')
            as_var = marker_data.get('as')

            current_cell = Cell(
                id=cell_id,
                code="",
                cell_type=cell_type,
                as_var=as_var,
            )
            current_code_lines = []

        elif current_cell is not None:
            # Add line to current cell's code
            current_code_lines.append(line)

    # Don't forget the last cell
    if current_cell is not None:
        current_cell.code = '\n'.join(current_code_lines).strip()
        cells.append(current_cell)

    return cells


def parse_notebook_file(filepath: str) -> list[Cell]:
    """
    Parse a notebook file from disk.

    Args:
        filepath: Path to the .py notebook file

    Returns:
        List of Cell objects
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    return parse_notebook(content)


def serialize_cell(cell: Cell) -> str:
    """
    Serialize a single cell to string format.

    Args:
        cell: The Cell object to serialize

    Returns:
        String representation with marker and code
    """
    # Build marker parts
    parts = [f"id: {cell.id}"]

    if cell.cell_type != "python":
        parts.append(f"type: {cell.cell_type}")

    if cell.as_var:
        parts.append(f"as: {cell.as_var}")

    marker = f"# %% [{', '.join(parts)}]"

    return f"{marker}\n{cell.code}"


def serialize_notebook(cells: list[Cell]) -> str:
    """
    Serialize a list of cells to notebook format.

    Args:
        cells: List of Cell objects

    Returns:
        Complete notebook content as string
    """
    if not cells:
        return ""

    return '\n\n'.join(serialize_cell(cell) for cell in cells) + '\n'


def serialize_notebook_file(cells: list[Cell], filepath: str) -> None:
    """
    Write cells to a notebook file.

    Args:
        cells: List of Cell objects
        filepath: Path to write the .py file
    """
    content = serialize_notebook(cells)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


# --- Convenience functions for cell management ---

def create_cell(cell_type: str = "python", code: str = "", as_var: Optional[str] = None) -> Cell:
    """Create a new cell with a generated ID."""
    return Cell(
        id=generate_cell_id(),
        code=code,
        cell_type=cell_type,
        as_var=as_var,
    )


def find_cell_by_id(cells: list[Cell], cell_id: str) -> Optional[Cell]:
    """Find a cell by its ID."""
    for cell in cells:
        if cell.id == cell_id:
            return cell
    return None


def remove_cell_by_id(cells: list[Cell], cell_id: str) -> bool:
    """Remove a cell by its ID. Returns True if found and removed."""
    for i, cell in enumerate(cells):
        if cell.id == cell_id:
            cells.pop(i)
            return True
    return False
