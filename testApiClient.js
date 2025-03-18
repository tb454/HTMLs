const { fetchExternalData } = require('./apiClient');

(async () => {
  try {
    const data = await fetchExternalData('https://jsonplaceholder.typicode.com/todos/1');
    console.log("Fetched Data:", data);
  } catch (error) {
    console.error("Test Error:", error);
  }
})();
