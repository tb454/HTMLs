const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    // Customize the log message format
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    // Log error-level messages to a file
    new winston.transports.File({ filename: 'error.log', level: 'error' })
  ]
});

module.exports = logger;
