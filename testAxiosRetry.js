const axios = require('axios');
const axiosRetry = require('axios-retry').default;

console.log("Type of axiosRetry:", typeof axiosRetry);

// Configure axios-retry with a simple retry logic
axiosRetry(axios, {
  retries: 2, // Try 2 times on failure
  retryDelay: axiosRetry.exponentialDelay, // Use exponential backoff
  retryCondition: (error) => {
    return axiosRetry.isNetworkError(error) || axiosRetry.isRetryableError(error);
  }
});

console.log("axios-retry is configured successfully.");

// Test an API call with axios
(async () => {
  try {
    const response = await axios.get('https://jsonplaceholder.typicode.com/todos/1', { timeout: 5000 });
    console.log("Response data:", response.data);
  } catch (error) {
    console.error("Error fetching data:", error.message);
  }
})();
