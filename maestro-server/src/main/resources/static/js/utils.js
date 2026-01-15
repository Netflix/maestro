/**
 * Utility functions for the Maestro UI
 */

/**
 * Formats a timestamp into a human-readable date string
 * @param {number} epoch - The timestamp to format
 * @returns {string} - The formatted date string
 */
function formatDate(epoch) {
    if (epoch === undefined) {
        return '-';
    }

    if (epoch.toString().length === 10) {
        epoch *= 1000;
    }

    const date = new Date(epoch);
    const pad = (n) => n.toString().padStart(2, '0');

    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    const seconds = pad(date.getSeconds());

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * Calculates the duration between two timestamps
 * @param {number} startTime - The start timestamp
 * @param {number} endTime - The end timestamp (optional, defaults to current time)
 * @returns {string} - The formatted duration string
 */
function calculateDuration(startTime, endTime) {
    if (!startTime) return 'N/A';

    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();

    const durationMs = end - start;
    const seconds = Math.floor(durationMs / 1000);

    if (seconds < 60) {
        return `${seconds} seconds`;
    } else if (seconds < 3600) {
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        return `${minutes} minutes, ${remainingSeconds} seconds`;
    } else {
        const hours = Math.floor(seconds / 3600);
        const remainingMinutes = Math.floor((seconds % 3600) / 60);
        return `${hours} hours, ${remainingMinutes} minutes`;
    }
}

/**
 * Handles API response and converts it to JSON or throws an error
 * @param {Response} response - The fetch API response
 * @returns {Promise<Object>} - The parsed JSON response
 * @throws {Error} - If the response is not ok
 */
function handleResponse(response) {
    if (!response.ok) {
        return response.text().then(text => {
            let error;
            try {
                error = JSON.parse(text);
            } catch (e) {
                error = undefined;
            }
            if (error) {
                throw new Error(error.message || `${response.status}: ${text}`);
            } else {
                throw new Error(`${response.status}: ${text}`);
            }
        });
    }
    return response.json();
}

// Export the utility functions
export { formatDate, calculateDuration, handleResponse };