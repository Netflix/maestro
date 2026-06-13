/**
 * UI functions for the Maestro UI
 */
import { formatDate, calculateDuration } from './utils.js';

// Set of error IDs to prevent duplicate errors
const errors = new Set();

/**
 * Shows an error message in the UI
 * @param {string} message - The error message to display
 * @param {string|number} errorId - A unique ID for the error (defaults to current timestamp)
 */
function showError(message, errorId = Date.now()) {
    const errorsContainer = document.getElementById('errorsContainer');
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.id = `error-${errorId}`;

    const messageSpan = document.createElement('span');
    messageSpan.textContent = message;

    const dismissButton = document.createElement('button');
    dismissButton.className = 'error-dismiss';
    dismissButton.innerHTML = '×';
    dismissButton.onclick = () => {
        errorDiv.remove();
        errors.delete(errorId);
    };

    errorDiv.appendChild(messageSpan);
    errorDiv.appendChild(dismissButton);

    if (!errors.has(errorId)) {
        errors.add(errorId);
        errorsContainer.appendChild(errorDiv);
    }
}

/**
 * Updates the workflow instances table with the provided data
 * @param {Object} data - The workflow instances data
 * @param {Function} onInstanceClick - Callback function when an instance is clicked
 */
function updateWorkflowInstancesTable(data, onInstanceClick) {
    const tbody = document.getElementById('result');

    // Sort instances by instance_id in descending order
    const sortedElements = [...data.elements].sort((a, b) => {
        return b.workflow_instance_id - a.workflow_instance_id;
    });

    tbody.innerHTML = sortedElements.map(instance => `
        <tr class="instance-row" data-instance-id="${instance.workflow_instance_id}" data-status="${instance.status}">
            <td>${instance.workflow_instance_id}</td>
            <td class="status-${instance.status.toLowerCase()}">${instance.status}</td>
            <td>${formatDate(instance.create_time)}</td>
            <td>${formatDate(instance.start_time)}</td>
            <td>${formatDate(instance.end_time)}</td>
            <td>${instance.workflow_uuid}</td>
        </tr>
    `).join('');

    // Add click handlers to instance rows
    document.querySelectorAll('.instance-row').forEach(row => {
        row.addEventListener('click', function() {
            const status = this.getAttribute('data-status');
            if (status && status.toLowerCase() !== "created") {
                const instanceId = this.getAttribute('data-instance-id');
                onInstanceClick(instanceId);
            }
        });
    });
}

/**
 * Shows the instance details panel
 * @param {string} instanceId - The ID of the instance to show
 */
function showInstanceDetails(instanceId) {
    document.getElementById('instanceIdDisplay').textContent = instanceId;
    document.getElementById('instanceDetailsContainer').style.display = 'block';
}

/**
 * Populates the run selector dropdown
 * @param {Object} instance - The workflow instance
 * @param {Function} onRunChange - Callback function when a run is selected
 */
function populateRunSelector(instance, onRunChange) {
    const runSelector = document.getElementById('runSelector');
    runSelector.innerHTML = '';

    // Add the latest run
    const option = document.createElement('option');
    option.value = instance.workflow_run_id;
    option.textContent = `Run ${instance.workflow_run_id} (Latest)`;
    runSelector.appendChild(option);

    // Add event listener to run selector
    runSelector.addEventListener('change', function() {
        onRunChange(this.value);
    });

    return instance.workflow_run_id;
}

/**
 * Visualizes the DAG (Directed Acyclic Graph) of a workflow run
 * @param {Object} run - The workflow run data
 * @param {Function} onStepClick - Callback function when a step is clicked
 */
async function visualizeDAG(run, onStepClick) {
    const dagContainer = document.getElementById('dagContainer');
    dagContainer.innerHTML = '';

    // Hide step details
    document.getElementById('stepDetailsContainer').style.display = 'none';

    // Check if we have a runtime DAG
    if (!run.runtime_dag) {
        dagContainer.innerHTML = '<p>No DAG information available for this run.</p>';
        return;
    }

    // Get the steps from the runtime workflow
    const steps = run.runtime_workflow.steps;
    if (!steps || steps.length === 0) {
        dagContainer.innerHTML = '<p>No steps available for this run.</p>';
        return;
    }

    // Create nodes for each step
    const nodes = {};
    const horizontalSpacing = 200;
    const verticalSpacing = 100;

    // First, create a map of step ID to step
    const stepsMap = {};
    steps.forEach(stepObj => {
        const step = stepObj.step;
        stepsMap[step.id] = step;
    });

    // Create a map of step ID to level (depth in the DAG)
    const levels = {};
    const visited = new Set();

    // Find root nodes (steps with no predecessors)
    const rootNodes = [];
    for (const stepId in run.runtime_dag) {
        const transition = run.runtime_dag[stepId];
        if (!transition.predecessors || transition.predecessors.length === 0) {
            rootNodes.push(stepId);
        }
    }

    // Assign levels to nodes using BFS
    const queue = rootNodes.map(id => ({ id, level: 0 }));
    while (queue.length > 0) {
        const { id, level } = queue.shift();
        if (visited.has(id)) continue;

        visited.add(id);
        levels[id] = level;

        // Add successors to queue
        const transition = run.runtime_dag[id];
        if (transition && transition.successors) {
            for (const successorId in transition.successors) {
                queue.push({ id: successorId, level: level + 1 });
            }
        }
    }

    // Count nodes at each level
    const nodesAtLevel = {};
    for (const id in levels) {
        const level = levels[id];
        nodesAtLevel[level] = (nodesAtLevel[level] || 0) + 1;
    }

    // Calculate positions for each node
    const positions = {};
    for (const id in levels) {
        const level = levels[id];
        const levelNodes = Object.entries(levels)
            .filter(([_, l]) => l === level)
            .map(([id]) => id);

        const index = levelNodes.indexOf(id);
        const x = level * horizontalSpacing + 50;
        const y = (index + 0.5) * verticalSpacing + 50;

        positions[id] = { x, y };
    }

    // Create nodes
    for (const stepId in stepsMap) {
        const step = stepsMap[stepId];
        const position = positions[stepId] || { x: 50, y: 50 };

        // Get step status from step details
        const stepDetails = await onStepClick(stepId, true);
        let status = stepDetails.runtime_state.status;

        // Create node element
        const node = document.createElement('div');
        node.className = `step-node status-${status.toLowerCase()}`;
        node.id = `step-${stepId}`;
        node.setAttribute('data-step-id', stepId);
        node.style.left = `${position.x}px`;
        node.style.top = `${position.y}px`;
        node.innerHTML = `
            <div>${step.id}</div>
            <div class="status-${status.toLowerCase()}">${status}</div>
        `;

        // Add click handler
        node.addEventListener('click', function() {
            // Deselect all nodes
            document.querySelectorAll('.step-node').forEach(n => n.classList.remove('selected'));

            // Select this node
            this.classList.add('selected');

            // Show step details
            onStepClick(stepId);
        });

        dagContainer.appendChild(node);
        nodes[stepId] = node;
    }

    // Create edges
    for (const stepId in run.runtime_dag) {
        const transition = run.runtime_dag[stepId];
        if (transition && transition.successors) {
            for (const successorId in transition.successors) {
                createEdge(stepId, successorId, nodes);
            }
        }
    }
}

/**
 * Creates an edge between two nodes in the DAG
 * @param {string} fromId - The ID of the source node
 * @param {string} toId - The ID of the target node
 * @param {Object} nodes - Map of node IDs to node elements
 */
function createEdge(fromId, toId, nodes) {
    const fromNode = nodes[fromId];
    const toNode = nodes[toId];

    if (!fromNode || !toNode) return;

    const fromRect = fromNode.getBoundingClientRect();
    const toRect = toNode.getBoundingClientRect();

    const dagContainer = document.getElementById('dagContainer');
    const containerRect = dagContainer.getBoundingClientRect();

    const fromX = parseInt(fromNode.style.left) + 150; // Right side of from node
    const fromY = parseInt(fromNode.style.top) + 30; // Middle of from node
    const toX = parseInt(toNode.style.left); // Left side of to node
    const toY = parseInt(toNode.style.top) + 30; // Middle of to node

    const length = Math.sqrt(Math.pow(toX - fromX, 2) + Math.pow(toY - fromY, 2));
    const angle = Math.atan2(toY - fromY, toX - fromX);

    const edge = document.createElement('div');
    edge.className = 'step-edge';
    edge.style.width = `${length}px`;
    edge.style.left = `${fromX}px`;
    edge.style.top = `${fromY}px`;
    edge.style.transform = `rotate(${angle}rad)`;

    dagContainer.appendChild(edge);
}

/**
 * Populates the attempt selector dropdown
 * @param {Object} step - The step data
 * @param {Function} onAttemptChange - Callback function when an attempt is selected
 */
function populateAttemptSelector(step, onAttemptChange) {
    const attemptSelector = document.getElementById('attemptSelector');
    attemptSelector.innerHTML = '';

    // Add the latest attempt
    const option = document.createElement('option');
    option.value = step.step_attempt_id;
    option.textContent = `Attempt ${step.step_attempt_id} (Latest)`;
    attemptSelector.appendChild(option);

    // Add event listener to attempt selector
    attemptSelector.addEventListener('change', function() {
        onAttemptChange(step.step_id, this.value);
    });

    return step.step_attempt_id;
}

/**
 * Displays step information in the UI
 * @param {Object} step - The step data
 */
function displayStepInfo(step) {
    const stepInfo = document.getElementById('stepInfo');
    stepInfo.innerHTML = '';

    // Display step properties
    const properties = [
        { label: 'ID', value: step.step_id },
        { label: 'Status', value: step.runtime_state ? step.runtime_state.status : 'N/A', class: step.runtime_state ? `status-${step.runtime_state.status.toLowerCase()}` : '' },
        { label: 'Type', value: step?.definition?.step?.type || 'N/A' },
        { label: 'Start Time', value: formatDate(step.runtime_state ? step.runtime_state.start_time : null) },
        { label: 'End Time', value: formatDate(step.runtime_state ? step.runtime_state.end_time : null) },
        { label: 'Duration', value: calculateDuration(step.runtime_state ? step.runtime_state.start_time : null, step.runtime_state ? step.runtime_state.end_time : null) }
    ];

    properties.forEach(prop => {
        const item = document.createElement('div');
        item.className = 'step-info-item';
        item.innerHTML = `
            <span class="step-info-label">${prop.label}:</span>
            <span ${prop.class ? `class="${prop.class}"` : ''}>${prop.value}</span>
        `;
        stepInfo.appendChild(item);
    });
}

/**
 * Shows step details in the UI
 * @param {Object} step - The step data
 * @param {Function} onAttemptChange - Callback function when an attempt is selected
 */
function showStepDetails(step, onAttemptChange) {
    document.getElementById('stepIdDisplay').textContent = step.id;
    document.getElementById('stepDetailsContainer').style.display = 'block';

    const attemptId = populateAttemptSelector(step, onAttemptChange);
    displayStepInfo(step);

    return attemptId;
}

export {
    showError,
    updateWorkflowInstancesTable,
    showInstanceDetails,
    populateRunSelector,
    visualizeDAG,
    populateAttemptSelector,
    displayStepInfo,
    showStepDetails
};
