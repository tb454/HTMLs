// webpack.config.js
const path = require('path');

module.exports = {
  entry: './src/index.js', // Adjust if your entry file is elsewhere
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'bundle.js'
  },
  module: {
    rules: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: 'babel-loader' // Optional: if you need transpiling
      }
    ]
  },
  mode: process.env.NODE_ENV || 'development'
};
