/**
 * Reactive Notebook Frontend
 *
 * Handles:
 * - Cell rendering and management
 * - WebSocket connection for real-time updates
 * - Keyboard shortcuts
 * - Database connection UI
 */

// === State ===

let cells = [];
let cellStates = {};
let ws = null;
let currentCellId = null;
// Track cells being edited and their pending server content
let editingCells = new Set();
let pendingUpdates = {}; // cellId -> server content
// Store CodeMirror editor instances by cell ID
let editors = {};
// Track unsaved changes per cell
let unsavedCells = new Set();
// Track last executed code per cell (to detect stale output)
let lastExecutedCode = {};
// Auto-run mode (false = manual, true = auto)
let autoRunMode = false;

// === DOM Elements ===

const cellsContainer = document.getElementById('cells-container');
const cellTemplate = document.getElementById('cell-template');
const insertDividerTemplate = document.getElementById('insert-divider-template');
const dbConnectionInput = document.getElementById('db-connection-string');
const dbConnectBtn = document.getElementById('db-connect-btn');
const dbStatusDot = document.getElementById('db-status-dot');
const dbStatusText = document.getElementById('db-status-text');
const saveBtn = document.getElementById('save-btn');
const runAllBtn = document.getElementById('run-all-btn');
const resetBtn = document.getElementById('reset-btn');
const addPythonBtn = document.getElementById('add-python-btn');
const addSqlBtn = document.getElementById('add-sql-btn');
const autoRunToggle = document.getElementById('auto-run-toggle');
const runModeText = document.getElementById('run-mode-text');

// === WebSocket ===

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('WebSocket connected');
    };

    ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        handleWebSocketMessage(message);
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting...');
        setTimeout(connectWebSocket, 2000);
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
    };
}

function handleWebSocketMessage(message) {
    switch (message.type) {
        case 'init':
            // Initial state from server
            cells = message.data.cells;
            cellStates = message.data.states;
            updateDbStatus(message.data.db_connected);
            renderAllCells();
            break;

        case 'status':
            // Cell status update
            const state = message.data;
            cellStates[state.cell_id] = state;
            updateCellUI(state.cell_id);
            break;

        case 'cells_updated':
            // Cells list changed
            const newCells = message.data;
            const structureChanged = newCells.length !== cells.length ||
                newCells.some((c, i) => cells[i]?.id !== c.id);

            // Update cells array
            const oldCells = cells;
            cells = newCells;

            if (structureChanged) {
                // Structure changed - full re-render
                renderAllCells();
            } else {
                // Content changed - check each cell
                for (const cell of cells) {
                    const oldCell = oldCells.find(c => c.id === cell.id);
                    if (oldCell && oldCell.code !== cell.code) {
                        if (editingCells.has(cell.id)) {
                            // User is editing this cell - mark as pending
                            pendingUpdates[cell.id] = cell.code;
                            showPendingIndicator(cell.id, true);
                        } else {
                            // Not editing - update directly
                            updateCellContent(cell.id, cell.code);
                        }
                    }
                }
            }
            break;

        case 'pong':
            // Heartbeat response
            break;
    }
}

// === API Calls ===

async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
        },
    };

    if (body) {
        options.body = JSON.stringify(body);
    }

    const response = await fetch(endpoint, options);

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'API error');
    }

    return response.json();
}

async function createCell(type, afterId = null) {
    try {
        const result = await apiCall('/cells', 'POST', {
            type,
            code: '',
            after_id: afterId,
        });
        return result;
    } catch (error) {
        console.error('Failed to create cell:', error);
        alert('Failed to create cell: ' + error.message);
    }
}

async function updateCell(cellId, updates) {
    try {
        await apiCall(`/cells/${cellId}`, 'PUT', updates);
        // Clear pending update since we just sent our version
        delete pendingUpdates[cellId];
        showPendingIndicator(cellId, false);
        // Clear unsaved indicator
        showUnsavedIndicator(cellId, false);
    } catch (error) {
        console.error('Failed to update cell:', error);
    }
}

async function deleteCell(cellId) {
    try {
        await apiCall(`/cells/${cellId}`, 'DELETE');
    } catch (error) {
        console.error('Failed to delete cell:', error);
        alert('Failed to delete cell: ' + error.message);
    }
}

async function runCell(cellId) {
    try {
        // Sync current content before running
        const editor = editors[cellId];
        if (editor) {
            const code = editor.getValue();
            await updateCell(cellId, { code: code });
            // Store the code we're about to execute
            lastExecutedCode[cellId] = code;
            // Clear stale indicator since we're running now
            showStaleIndicator(cellId, false);
        }
        await apiCall(`/cells/${cellId}/run`, 'POST');
    } catch (error) {
        console.error('Failed to run cell:', error);
        alert('Failed to run cell: ' + error.message);
    }
}

async function runAllCells() {
    try {
        await apiCall('/cells/run-all', 'POST');
    } catch (error) {
        console.error('Failed to run all cells:', error);
        alert('Failed to run all cells: ' + error.message);
    }
}

async function resetNotebook() {
    try {
        await apiCall('/cells/reset', 'POST');
    } catch (error) {
        console.error('Failed to reset notebook:', error);
        alert('Failed to reset notebook: ' + error.message);
    }
}

async function saveNotebook() {
    try {
        await apiCall('/cells/save', 'POST');
        console.log('Notebook saved');
    } catch (error) {
        console.error('Failed to save notebook:', error);
        alert('Failed to save notebook: ' + error.message);
    }
}

async function connectDatabase(connectionString) {
    try {
        await apiCall('/config/db', 'POST', {
            connection_string: connectionString,
        });
        updateDbStatus(true);
    } catch (error) {
        console.error('Failed to connect to database:', error);
        alert('Failed to connect: ' + error.message);
        updateDbStatus(false);
    }
}

// === UI Updates ===

function updateDbStatus(connected) {
    if (connected) {
        dbStatusDot.classList.add('connected');
        dbStatusText.textContent = 'Connected';
        dbConnectBtn.textContent = 'Reconnect';
    } else {
        dbStatusDot.classList.remove('connected');
        dbStatusText.textContent = 'Not connected';
        dbConnectBtn.textContent = 'Connect';
    }
}

function updateCellContent(cellId, code) {
    const editor = editors[cellId];
    if (editor && editor.getValue() !== code) {
        editor.setValue(code || '');
    }
}

function showPendingIndicator(cellId, show) {
    const cellDiv = document.querySelector(`.cell[data-cell-id="${cellId}"]`);
    if (!cellDiv) return;

    const syncBtn = cellDiv.querySelector('.sync-btn');
    if (syncBtn) {
        syncBtn.style.display = show ? 'inline-block' : 'none';
    }

    if (show) {
        cellDiv.classList.add('has-pending-update');
    } else {
        cellDiv.classList.remove('has-pending-update');
    }
}

function showUnsavedIndicator(cellId, show) {
    const cellDiv = document.querySelector(`.cell[data-cell-id="${cellId}"]`);
    if (!cellDiv) return;

    const unsavedDiv = cellDiv.querySelector('.cell-unsaved');
    if (unsavedDiv) {
        unsavedDiv.style.display = show ? 'flex' : 'none';
    }

    if (show) {
        unsavedCells.add(cellId);
    } else {
        unsavedCells.delete(cellId);
    }
}

function showStaleIndicator(cellId, show) {
    const cellDiv = document.querySelector(`.cell[data-cell-id="${cellId}"]`);
    if (!cellDiv) return;

    const staleIndicator = cellDiv.querySelector('.stale-indicator');
    if (staleIndicator) {
        staleIndicator.style.display = show ? 'inline' : 'none';
    }
}

function checkIfStale(cellId) {
    const editor = editors[cellId];
    if (!editor) return;

    const currentCode = editor.getValue();
    const executedCode = lastExecutedCode[cellId];

    // Only show stale if cell has been run before and code changed
    if (executedCode !== undefined && currentCode !== executedCode) {
        showStaleIndicator(cellId, true);
    } else {
        showStaleIndicator(cellId, false);
    }
}

function syncCellFromServer(cellId) {
    const serverCode = pendingUpdates[cellId];
    if (serverCode !== undefined) {
        updateCellContent(cellId, serverCode);
        delete pendingUpdates[cellId];
        showPendingIndicator(cellId, false);
    }
}

function renderAllCells() {
    // Clean up old CodeMirror instances
    for (const cellId in editors) {
        if (editors[cellId]) {
            editors[cellId].toTextArea(); // Destroy CodeMirror instance
        }
    }
    editors = {};

    cellsContainer.innerHTML = '';

    // Add insert divider at the top (to insert before first cell)
    const topDivider = createInsertDivider(null);
    cellsContainer.appendChild(topDivider);

    for (const cell of cells) {
        const cellElement = createCellElement(cell);
        cellsContainer.appendChild(cellElement);

        // Add insert divider after each cell
        const divider = createInsertDivider(cell.id);
        cellsContainer.appendChild(divider);
    }

    // Refresh all CodeMirror editors after DOM is ready
    requestAnimationFrame(() => {
        for (const cellId in editors) {
            if (editors[cellId]) {
                editors[cellId].refresh();
            }
        }
    });
}

function createInsertDivider(afterCellId) {
    const template = insertDividerTemplate.content.cloneNode(true);
    const divider = template.querySelector('.insert-divider');

    // Use empty string for "insert at beginning", cell ID for "insert after this cell"
    const insertAfterId = afterCellId === null ? '' : afterCellId;
    divider.dataset.afterId = insertAfterId;

    const pythonBtn = divider.querySelector('.insert-python');
    const sqlBtn = divider.querySelector('.insert-sql');

    pythonBtn.addEventListener('click', () => {
        createCell('python', insertAfterId);
    });

    sqlBtn.addEventListener('click', () => {
        createCell('sql', insertAfterId);
    });

    return divider;
}

function createCellElement(cell) {
    const template = cellTemplate.content.cloneNode(true);
    const cellDiv = template.querySelector('.cell');

    cellDiv.dataset.cellId = cell.id;

    // Type badge
    const typeBadge = cellDiv.querySelector('.cell-type-badge');
    typeBadge.textContent = cell.type;
    typeBadge.classList.add(cell.type);

    // SQL config
    const sqlConfig = cellDiv.querySelector('.cell-sql-config');
    const asVarInput = cellDiv.querySelector('.as-var-input');
    if (cell.type === 'sql') {
        sqlConfig.style.display = 'flex';
        asVarInput.value = cell.as || '';
        asVarInput.addEventListener('change', () => {
            updateCell(cell.id, { as_var: asVarInput.value });
        });
    }

    // Code input - Initialize CodeMirror
    const codeInput = cellDiv.querySelector('.code-input');

    // Determine the mode based on cell type
    const mode = cell.type === 'sql' ? 'text/x-sql' : 'python';

    // Initialize CodeMirror editor
    const editor = CodeMirror.fromTextArea(codeInput, {
        mode: mode,
        lineNumbers: true,
        indentUnit: 4,
        tabSize: 4,
        indentWithTabs: false,
        lineWrapping: true,
        viewportMargin: Infinity,
        extraKeys: {
            'Tab': (cm) => {
                cm.replaceSelection('    ', 'end');
            },
            'Ctrl-Enter': () => {
                runCell(cell.id);
            },
            'Shift-Enter': () => {
                runCell(cell.id);
                focusNextCell(cell.id);
            },
            'Ctrl-S': (cm) => {
                cm.getInputField().blur(); // Trigger save via blur
                saveNotebook();
                return false; // Prevent default
            }
        }
    });

    // Set initial value
    editor.setValue(cell.code || '');

    // Store editor instance
    editors[cell.id] = editor;

    // Track editing state
    let saveTimeout;

    editor.on('focus', () => {
        currentCellId = cell.id;
        editingCells.add(cell.id);
    });

    editor.on('blur', () => {
        // Delay removing from editing set to allow for click events
        setTimeout(() => {
            editingCells.delete(cell.id);
            // If there's a pending update and we're done editing, apply it
            if (pendingUpdates[cell.id] !== undefined) {
                // Don't auto-apply - let user decide via sync button
            }
        }, 200);
    });

    editor.on('change', () => {
        // Mark as editing
        editingCells.add(cell.id);

        // Show unsaved indicator immediately
        showUnsavedIndicator(cell.id, true);

        // Check if output is now stale
        checkIfStale(cell.id);

        // Debounced save (and auto-run if enabled)
        clearTimeout(saveTimeout);
        saveTimeout = setTimeout(async () => {
            await updateCell(cell.id, { code: editor.getValue() });
            // Auto-run if enabled
            if (autoRunMode) {
                runCell(cell.id);
            }
        }, 1000); // 1 second debounce
    });

    // Save button (in cell header)
    const saveCellBtn = cellDiv.querySelector('.save-cell-btn');
    if (saveCellBtn) {
        saveCellBtn.addEventListener('click', () => {
            clearTimeout(saveTimeout);
            updateCell(cell.id, { code: editor.getValue() });
        });
    }

    // Run button
    const runBtn = cellDiv.querySelector('.run-btn');
    runBtn.addEventListener('click', () => {
        runCell(cell.id);
    });

    // Sync button (for pending updates)
    const syncBtn = cellDiv.querySelector('.sync-btn');
    if (syncBtn) {
        syncBtn.addEventListener('click', () => {
            syncCellFromServer(cell.id);
        });
    }

    // Delete button
    const deleteBtn = cellDiv.querySelector('.delete-btn');
    deleteBtn.addEventListener('click', () => {
        if (confirm('Delete this cell?')) {
            deleteCell(cell.id);
        }
    });

    // Apply current state
    const state = cellStates[cell.id];
    if (state) {
        applyCellState(cellDiv, state);
    }

    // Show pending indicator if there's a pending update
    if (pendingUpdates[cell.id] !== undefined) {
        showPendingIndicator(cell.id, true);
    }

    return cellDiv;
}

function updateCellUI(cellId) {
    const cellDiv = document.querySelector(`.cell[data-cell-id="${cellId}"]`);
    if (!cellDiv) return;

    const state = cellStates[cellId];
    if (!state) return;

    applyCellState(cellDiv, state);
}

function applyCellState(cellDiv, state) {
    // Update status class
    cellDiv.classList.remove('status-idle', 'status-running', 'status-success', 'status-error', 'status-blocked');
    cellDiv.classList.add(`status-${state.status}`);

    // Update status indicator
    const indicator = cellDiv.querySelector('.status-indicator');
    indicator.classList.remove('running', 'success', 'error', 'blocked');
    indicator.classList.add(state.status);

    // Update status text
    const statusText = cellDiv.querySelector('.status-text');
    statusText.textContent = state.status;

    // Update output
    const stdoutDiv = cellDiv.querySelector('.output-stdout');
    const resultDiv = cellDiv.querySelector('.output-result');
    const errorDiv = cellDiv.querySelector('.output-error');
    const emptyDiv = cellDiv.querySelector('.output-empty');

    // Clear previous output
    stdoutDiv.innerHTML = '';
    resultDiv.innerHTML = '';
    errorDiv.innerHTML = '';

    let hasOutput = false;

    // Show stdout
    if (state.stdout) {
        stdoutDiv.textContent = state.stdout;
        hasOutput = true;
    }

    // Show result
    if (state.output) {
        if (state.output_type === 'html') {
            resultDiv.innerHTML = state.output;
        } else {
            resultDiv.textContent = state.output;
        }
        hasOutput = true;
    }

    // Show error
    if (state.error) {
        errorDiv.textContent = state.error;
        if (state.error_traceback) {
            errorDiv.textContent += '\n\n' + state.error_traceback;
        }
        hasOutput = true;
    }

    // Show blocked message
    if (state.status === 'blocked' && state.blocked_by) {
        errorDiv.innerHTML = `<div class="blocked-message">Blocked by failed cell: ${state.blocked_by}</div>`;
        hasOutput = true;
    }

    // Toggle empty state visibility
    if (emptyDiv) {
        emptyDiv.style.display = hasOutput ? 'none' : 'block';
    }

    // Track executed code when cell finishes running (success or error)
    if (state.status === 'success' || state.status === 'error') {
        const editor = editors[state.cell_id];
        if (editor) {
            lastExecutedCode[state.cell_id] = editor.getValue();
            showStaleIndicator(state.cell_id, false);
        }
    }
}

function focusNextCell(currentCellId) {
    const cellIds = cells.map(c => c.id);
    const currentIndex = cellIds.indexOf(currentCellId);

    if (currentIndex >= 0 && currentIndex < cellIds.length - 1) {
        const nextCellId = cellIds[currentIndex + 1];
        const nextEditor = editors[nextCellId];
        if (nextEditor) {
            nextEditor.focus();
        }
    }
}

// === Keyboard Shortcuts ===

document.addEventListener('keydown', (e) => {
    // Ctrl+S: Save notebook
    if (e.ctrlKey && e.key === 's' && !e.shiftKey) {
        e.preventDefault();
        saveNotebook();
    }

    // Ctrl+Enter: Run current cell
    if (e.ctrlKey && e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        if (currentCellId) {
            runCell(currentCellId);
        }
    }

    // Shift+Enter: Run current cell and move to next
    if (e.shiftKey && e.key === 'Enter' && !e.ctrlKey) {
        e.preventDefault();
        if (currentCellId) {
            runCell(currentCellId);
            focusNextCell(currentCellId);
        }
    }

    // Ctrl+Shift+N: New cell below
    if (e.ctrlKey && e.shiftKey && e.key === 'N') {
        e.preventDefault();
        createCell('python', currentCellId);
    }
});

// === Event Listeners ===

// Database connection
dbConnectBtn.addEventListener('click', () => {
    const connectionString = dbConnectionInput.value.trim();
    if (connectionString) {
        connectDatabase(connectionString);
    } else {
        alert('Please enter a connection string');
    }
});

// Save notebook
saveBtn.addEventListener('click', () => {
    saveNotebook();
});

// Run all cells
runAllBtn.addEventListener('click', () => {
    runAllCells();
});

// Reset notebook
resetBtn.addEventListener('click', () => {
    if (confirm('Reset all cell states and clear the namespace?')) {
        resetNotebook();
    }
});

// Add Python cell
addPythonBtn.addEventListener('click', () => {
    createCell('python');
});

// Add SQL cell
addSqlBtn.addEventListener('click', () => {
    createCell('sql');
});

// Auto-run mode toggle
autoRunToggle.addEventListener('change', () => {
    autoRunMode = autoRunToggle.checked;
    runModeText.textContent = autoRunMode ? 'Auto' : 'Manual';
});

// === Initialize ===

// Connect WebSocket on load
connectWebSocket();

// Heartbeat to keep connection alive
setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'ping' }));
    }
}, 30000);
