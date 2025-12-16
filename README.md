# Reactive Notebook

A reactive notebook where cells auto-update when their dependencies change. Built for Querio's technical challenge.

![Screenshot](docs/screenshot.png)

## What it does

executes Python or SQL in cde cells. When variable are changed upstream, everything downstream re-runs automatically. it maintains all similar functionality of a jupyter ntoebook.

The backend parses your code with Python's `ast` module to asertain out which variables each cell reads/writes, builds a dependency graph, and handles execution order via topological sort.

The project runs in a docker container, i i did this as its clean and was drastically easier to run on my pc. 

## Running it

```bash
docker-compose up --build
# Open http://localhost:8000
```

Default Postgres connection string
postgresql://notebook:notebook@postgres:5432/notebook

Tests:
```bash
docker-compose exec notebook pytest tests/ -v
```

## Stack

```
Frontend (HTML/JS + CodeMirror)
        │
        │ WebSocket + REST
        ▼
FastAPI Backend
  ├── parser.py      - cell parsing
  ├── dependency.py  - AST analysis, dep graph
  ├── executor.py    - runs code, captures output
  ├── reactor.py     - orchestrates reactive execution
  └── database.py    - postgres connection
```

## Design choices

- **Shared namespace** - all cells share one globals dict. Simple and it works.
- **AST for dependencies** - parsing beats regex. Handles edge cases properly.
- **WebSocket** - real-time status updates without polling.
- **CodeMirror** - syntax highlighting without a build step.
- **`.py` file format** - notebooks are valid Python files with `# %%` markers. Git-friendly.

## Shortcuts

- `Ctrl+Enter` - run cell
- `Shift+Enter` - run and move to next
- `Ctrl+S` - save

## Notes

Found [Marimo](https://marimo.io/) while researching - impressive reactive notebook, worth checking out.

Kept scope tight but added unit tests because it's good practice.

i had noticed some minor lag when runnign so i added the feature to manually run them, my pc is struggling so wasnt able to fully identfy but i beleive its okay

**If I kept going, I'd add:** save multiple notebooks, execution timing, matplotlib/plotly output, and a variable inspector panel.
