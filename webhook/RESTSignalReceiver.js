const SignalSource = require('./SignalSource');
const express = require('express');
const EventEmitter = require('events');
const fs = require('fs').promises;
const { normalizeSignal } = require('./normalizeSignal');

/** Max retained signal log entries (newest kept). */
const MAX_SIGNAL_LOG = 1000;

class RESTSignalReceiver extends SignalSource {
    /**
     * @param {number} [port=5001]
     * @param {string} [logFilePath='signals.json']
     * @param {import('express').Express|null} [app=null]
     */
    constructor(port = 5001, logFilePath = 'signals.json', app = null) {
        super();
        this.port = port;
        this.app = app || express();
        this.app.use(express.json());
        this.events = new EventEmitter();
        this.logFilePath = logFilePath;
        this.setupRoutes();
        this.initializeLogFile();
    }

    async initializeLogFile() {
        try {
            await fs.access(this.logFilePath);
        } catch {
            await fs.writeFile(this.logFilePath, '[]', 'utf8');
            console.log(`Created new log file: ${this.logFilePath}`);
        }
    }

    setupRoutes() {
        this.app.post('/signal', (req, res) => {
            const result = normalizeSignal(req.body);
            if (!result.ok) {
                console.warn('Invalid signal received:', req.body);
                return res.status(400).json({
                    error: result.error,
                    received: result.received,
                });
            }

            const signalData = result.signal;
            console.log(
                `Received signal: ${signalData.action} ${signalData.quantity} for ${signalData.symbol || 'NIFTY'} (Position: ${signalData.position})`
            );

            this.events.emit('signal', signalData);
            this.logSignal(signalData);

            res.json({
                status: 'Signal received',
                symbol: signalData.symbol,
                action: signalData.action,
                quantity: signalData.quantity,
                position: signalData.position,
            });
        });
    }

    async logSignal(signalData) {
        try {
            let existingData = [];
            try {
                const fileContent = await fs.readFile(this.logFilePath, 'utf8');
                existingData = JSON.parse(fileContent);
                if (!Array.isArray(existingData)) existingData = [];
            } catch {
                existingData = [];
            }

            existingData.push(signalData);
            if (existingData.length > MAX_SIGNAL_LOG) {
                existingData = existingData.slice(-MAX_SIGNAL_LOG);
            }
            await fs.writeFile(
                this.logFilePath,
                JSON.stringify(existingData, null, 2),
                'utf8'
            );
            console.log(`Signal logged to ${this.logFilePath}`);
        } catch (error) {
            console.error('Error logging signal to file:', error);
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
