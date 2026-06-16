const SignalSource = require('./SignalSource');
const express = require('express');
const EventEmitter = require('events');

class RESTSignalReceiver extends SignalSource {
    constructor(port = 5001) {
        super();
        this.port = port;
        this.app = express();
        this.app.use(express.json());
        this.events = new EventEmitter();
        this.setupRoutes();
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
            this.events.emit('signal', {
                ...req.body,
                symbol: symbol,
                action: action.toUpperCase(),
                quantity: Number(quantity),
                position: position,
                orderType: (orderType || 'LIMIT').toUpperCase(),
                limitPrice: limitPrice ? Number(limitPrice) : undefined,
                productType: (productType || 'MIS').toUpperCase(),
                instrumentType: instrumentType ? instrumentType.toUpperCase() : undefined,
                timestamp: Date.now()
            });

            res.json({ status: 'Signal received', symbol, action, quantity, position });
        });
    }

    start() {
        this.server = this.app.listen(this.port, () => {
            console.log(`Signal Receiver listening on port ${this.port}`);
        });
    }

    stop() {
        if (this.server) this.server.close();
    }

    on(event, callback) {
        this.events.on(event, callback);
    }
}

module.exports = RESTSignalReceiver;
