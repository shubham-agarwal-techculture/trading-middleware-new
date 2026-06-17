const SignalSource = require('./SignalSource');
const express = require('express');
const EventEmitter = require('events');
const fs = require('fs').promises;
const path = require('path');

class RESTSignalReceiver extends SignalSource {
    constructor(port = 5001, logFilePath = 'signals.json', app = null) {
        super();
        this.port = port;
        if (app) {
            this.app = app;
        } else {
            this.app = express();
        }
        this.app.use(express.json());
        this.events = new EventEmitter();
        this.logFilePath = logFilePath;
        this.setupRoutes();
        this.initializeLogFile();
    }

    async initializeLogFile() {
        try {
            // Check if file exists, if not create it with empty array
            await fs.access(this.logFilePath);
        } catch (error) {
            // File doesn't exist, create it with empty array
            await fs.writeFile(this.logFilePath, '[]', 'utf8');
            console.log(`Created new log file: ${this.logFilePath}`);
        }
    }

    setupRoutes() {
        this.app.post('/signal', (req, res) => {
            const { symbol, action, quantity, position } = req.body;

            if (!position || !action || !quantity) {
                console.warn('Invalid signal received:', req.body, req.query);
                return res.status(400).json({
                    error: 'Missing required fields: action, quantity, position',
                    received: { body: req.body, query: req.query }
                });
            }

            console.log(`Received signal: ${action} ${quantity} for ${symbol || 'NIFTY'} (Position: ${position || 'N/A'})`);

            const { orderType, limitPrice, productType, instrumentType } = req.body;
            
            // Create the signal object
            const signalData = {
                ...req.body,
                symbol: symbol,
                action: action.toUpperCase(),
                quantity: Number(quantity),
                position: position,
                orderType: (orderType || 'LIMIT').toUpperCase(),
                limitPrice: limitPrice ? Number(limitPrice) : undefined,
                productType: (productType || 'MIS').toUpperCase(),
                instrumentType: instrumentType ? instrumentType.toUpperCase() : undefined,
                timestamp: Date.now(),
                receivedAt: new Date().toISOString()
            };

            // Emit the signal
            this.events.emit('signal', signalData);

            // Log the signal asynchronously
            this.logSignal(signalData);

            res.json({ status: 'Signal received', symbol, action, quantity, position });
        });
    }

    async logSignal(signalData) {
        try {
            // Read existing data
            let existingData = [];
            try {
                const fileContent = await fs.readFile(this.logFilePath, 'utf8');
                existingData = JSON.parse(fileContent);
                // Ensure it's an array
                if (!Array.isArray(existingData)) {
                    existingData = [];
                }
            } catch (error) {
                // If file doesn't exist or is invalid, start with empty array
                existingData = [];
            }

            // Add new signal
            existingData.push(signalData);

            // Write back to file
            await fs.writeFile(this.logFilePath, JSON.stringify(existingData, null, 2), 'utf8');
            
            console.log(`Signal logged to ${this.logFilePath}`);
        } catch (error) {
            console.error('Error logging signal to file:', error);
        }
    }

    // Optional: Method to query logs
    async getLogs(options = {}) {
        try {
            const fileContent = await fs.readFile(this.logFilePath, 'utf8');
            let logs = JSON.parse(fileContent);
            
            // Filter by symbol if provided
            if (options.symbol) {
                logs = logs.filter(log => log.symbol === options.symbol);
            }
            
            // Filter by action if provided
            if (options.action) {
                logs = logs.filter(log => log.action === options.action.toUpperCase());
            }
            
            // Limit results
            if (options.limit && options.limit > 0) {
                logs = logs.slice(-options.limit);
            }
            
            return logs;
        } catch (error) {
            console.error('Error reading logs:', error);
            return [];
        }
    }

    start(server = null) {
        if (server) {
            this.server = server;
            console.log(`Signal Receiver attached to existing server on port ${this.port}`);
        } else {
            this.server = this.app.listen(this.port, () => {
                console.log(`Signal Receiver listening on port ${this.port}`);
                console.log(`Signals will be logged to: ${this.logFilePath}`);
            });
        }
    }

    stop() {
        if (this.server) this.server.close();
    }

    on(event, callback) {
        this.events.on(event, callback);
    }
}

module.exports = RESTSignalReceiver;
