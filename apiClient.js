// apiClient.js
const axios = require('axios');
const axiosRetry = require('axios-retry');

console.log("Type of axiosRetry:", typeof axiosRetry);

// Configure axios-retry with desired options
axiosRetry(axios, {
  retries: 3, // Number of retry attempts
  retryDelay: axiosRetry.exponentialDelay, // Exponential backoff delay
  retryCondition: (error) => {
    // Retry on network errors or if the response status is in the 5xx range
    return axiosRetry.isNetworkError(error) || axiosRetry.isRetryableError(error);
  }
});

/**
 * Fetch external data with a timeout of 5000ms (5 seconds)
 * @param {string} url - The URL to fetch data from.
 * @returns {Promise<any>} - The response data.
 */
async function fetchExternalData(url) {
  try {
    const response = await axios.get(url, { timeout: 5000 });
    return response.data;
  } catch (error) {
    console.error(`Error fetching external data from ${url}: ${error.message}`);
    throw error;
  }
}

module.exports = { fetchExternalData };
