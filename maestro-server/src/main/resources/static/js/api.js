/**
 * API functions for the Maestro UI
 */
import { handleResponse } from './utils.js';

/**
 * Start a new workflow instance
 * @param {string} workflowId - The ID of the workflow to start
 * @returns {Promise<Object>} - The created workflow instance
 */
async function startWorkflowInstance(workflowId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/versions/latest/actions/start`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'user': 'tester'
            },
            body: JSON.stringify({
                initiator: {
                    type: 'manual'
                }
            })
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error starting workflow:', error);
        throw error;
    }
}

/**
 * Fetch workflow instances
 * @param {string} workflowId - The ID of the workflow
 * @param {number} limit - The maximum number of instances to fetch
 * @returns {Promise<Object>} - The workflow instances
 */
async function fetchWorkflowInstances(workflowId, limit = 100) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/instances?first=${limit}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error fetching workflow instances:', error);
        throw error;
    }
}

/**
 * Fetch workflow instance details
 * @param {string} workflowId - The ID of the workflow
 * @param {string} instanceId - The ID of the instance
 * @returns {Promise<Object>} - The workflow instance details
 */
async function fetchWorkflowInstanceDetails(workflowId, instanceId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/instances/${instanceId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error fetching instance details:', error);
        throw error;
    }
}

/**
 * Fetch workflow run details
 * @param {string} workflowId - The ID of the workflow
 * @param {string} instanceId - The ID of the instance
 * @param {string} runId - The ID of the run
 * @returns {Promise<Object>} - The workflow run details
 */
async function fetchWorkflowRunDetails(workflowId, instanceId, runId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/instances/${instanceId}/runs/${runId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error fetching run details:', error);
        throw error;
    }
}

/**
 * Fetch step details
 * @param {string} workflowId - The ID of the workflow
 * @param {string} instanceId - The ID of the instance
 * @param {string} stepId - The ID of the step
 * @returns {Promise<Object>} - The step details
 */
async function fetchStepDetails(workflowId, instanceId, stepId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/instances/${instanceId}/steps/${stepId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error fetching step details:', error);
        throw error;
    }
}

/**
 * Fetch step attempt details
 * @param {string} workflowId - The ID of the workflow
 * @param {string} instanceId - The ID of the instance
 * @param {string} runId - The ID of the run
 * @param {string} stepId - The ID of the step
 * @param {string} attemptId - The ID of the attempt
 * @returns {Promise<Object>} - The step attempt details
 */
async function fetchStepAttemptDetails(workflowId, instanceId, runId, stepId, attemptId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/instances/${instanceId}/runs/${runId}/steps/${stepId}/attempts/${attemptId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error fetching step attempt details:', error);
        throw error;
    }
}

/**
 * Check if a workflow exists
 * @param {string} workflowId - The ID of the workflow
 * @returns {Promise<Object>} - The workflow details if it exists
 */
async function checkWorkflowExists(workflowId) {
    try {
        const response = await fetch(`/api/v3/workflows/${workflowId}/versions/latest`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
        return await handleResponse(response);
    } catch (error) {
        console.error('Error checking workflow:', error);
        throw error;
    }
}

export {
    startWorkflowInstance,
    fetchWorkflowInstances,
    fetchWorkflowInstanceDetails,
    fetchWorkflowRunDetails,
    fetchStepDetails,
    fetchStepAttemptDetails,
    checkWorkflowExists
};