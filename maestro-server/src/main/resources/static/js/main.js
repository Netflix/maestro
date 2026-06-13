/**
 * Main application logic for the Maestro UI
 */
import * as api from './api.js';
import * as ui from './ui.js';

// Global state
let state = {
    workflowId: null,
    currentInstanceId: null,
    currentRunId: null,
    currentStepId: null,
    currentAttemptId: null,
    currentInstance: null,
    currentRun: null
};

/**
 * Initializes the application
 */
async function init() {
    // Parse the URL hash to get the workflow ID
    parseUrlHash();

    // Check if workflowId is blank and show error if it is
    if (!state.workflowId) {
        ui.showError('Workflow ID is blank. Please provide a valid workflow ID in the URL.');
        disableStartButton();
        return;
    }

    // Update page title with workflow ID
    document.getElementById('pageTitle').textContent = state.workflowId;

    // Set up event listeners
    document.getElementById('startButton').addEventListener('click', startWorkflow);

    // Check if workflow exists
    try {
        await api.checkWorkflowExists(state.workflowId);
        
        // Workflow exists, fetch instances
        await fetchAndUpdateInstances();
        
        // Set up periodic refresh
        setInterval(fetchAndUpdateInstances, 2000);
    } catch (error) {
        if (error.message.includes('NOT_FOUND')) {
            ui.showError(`Workflow [${state.workflowId}] has not been created yet or has been deleted.`);
            disableStartButton();
            document.getElementById('workflowTable').style.display = 'none';
        } else {
            ui.showError('Error checking workflow: ' + error.message);
        }
    }
}

/**
 * Parses the URL hash to extract the workflow ID
 */
function parseUrlHash() {
    // Check if we're at the root path and redirect to /#/workflows/{workflowId}/instances if needed
    if (!window.location.hash) {
        // We're at the root path, redirect to /#/workflows/sample-dag-test-1/instances
        window.location.href = '/#/workflows/sample-dag-test-1/instances';
        return;
    }

    // Get workflowId from URL hash: /#/workflows/{workflowId}/instances
    const hashParts = window.location.hash.substring(1).split('/').filter(part => part.length > 0);

    // Check if the URL has the correct format but workflowId is blank
    const hasWorkflowsPath = hashParts.length >= 1 && hashParts[0] === 'workflows';
    const isWorkflowIdBlank = hasWorkflowsPath && (hashParts.length === 1 || hashParts[1] === '');

    // Set workflowId based on URL hash
    state.workflowId = isWorkflowIdBlank ? null : hashParts[1];
}

/**
 * Disables the start button
 */
function disableStartButton() {
    const startButton = document.getElementById('startButton');
    startButton.disabled = true;
    startButton.style.backgroundColor = '#cccccc';
    startButton.style.cursor = 'not-allowed';
}

/**
 * Starts a new workflow instance
 */
async function startWorkflow() {
    try {
        await api.startWorkflowInstance(state.workflowId);
        await fetchAndUpdateInstances();
    } catch (error) {
        ui.showError('Error starting workflow: ' + error.message);
    }
}

/**
 * Fetches workflow instances and updates the UI
 */
async function fetchAndUpdateInstances() {
    try {
        const data = await api.fetchWorkflowInstances(state.workflowId);
        ui.updateWorkflowInstancesTable(data, showInstanceDetails);
    } catch (error) {
        ui.showError('Error fetching data: ' + error.message);
    }
}

/**
 * Shows instance details
 * @param {string} instanceId - The ID of the instance to show
 */
async function showInstanceDetails(instanceId) {
    state.currentInstanceId = instanceId;
    ui.showInstanceDetails(instanceId);

    try {
        // Fetch the instance details
        const instance = await api.fetchWorkflowInstanceDetails(state.workflowId, instanceId);
        state.currentInstance = instance;

        // Populate run selector and get current run ID
        state.currentRunId = ui.populateRunSelector(instance, handleRunChange);

        // Fetch the run details
        await fetchRunDetails(state.currentRunId);
    } catch (error) {
        ui.showError('Error fetching instance details: ' + error.message);
    }
}

/**
 * Handles run selection change
 * @param {string} runId - The ID of the selected run
 */
async function handleRunChange(runId) {
    state.currentRunId = runId;
    await fetchRunDetails(runId);
}

/**
 * Fetches run details and updates the UI
 * @param {string} runId - The ID of the run
 */
async function fetchRunDetails(runId) {
    try {
        const run = await api.fetchWorkflowRunDetails(state.workflowId, state.currentInstanceId, runId);
        state.currentRun = run;

        // Visualize the DAG
        await ui.visualizeDAG(run, handleStepClick);
    } catch (error) {
        ui.showError('Error fetching run details: ' + error.message);
    }
}

/**
 * Handles step click in the DAG
 * @param {string} stepId - The ID of the clicked step
 * @param {boolean} fetchOnly - If true, only fetch the step details without updating UI
 * @returns {Promise<Object>} - The step details
 */
async function handleStepClick(stepId, fetchOnly = false) {
    try {
        state.currentStepId = stepId;
        
        // Fetch step details
        const step = await api.fetchStepDetails(state.workflowId, state.currentInstanceId, stepId);
        
        if (!fetchOnly) {
            // Show step details in UI
            state.currentAttemptId = ui.showStepDetails(step, handleAttemptChange);
        }
        
        return step;
    } catch (error) {
        ui.showError('Error fetching step details: ' + error.message);
        throw error;
    }
}

/**
 * Handles attempt selection change
 * @param {string} stepId - The ID of the step
 * @param {string} attemptId - The ID of the selected attempt
 */
async function handleAttemptChange(stepId, attemptId) {
    try {
        state.currentAttemptId = attemptId;
        
        // Fetch step attempt details
        const attempt = await api.fetchStepAttemptDetails(
            state.workflowId, 
            state.currentInstanceId, 
            state.currentRunId, 
            stepId, 
            attemptId
        );
        
        // Display step attempt info
        ui.displayStepInfo(attempt);
    } catch (error) {
        ui.showError('Error fetching step attempt: ' + error.message);
    }
}

// Initialize the application when the DOM is fully loaded
document.addEventListener('DOMContentLoaded', init);