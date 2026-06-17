require('dotenv').config();
const RESTSignalReceiver = require('./RESTSignalReceiver');
const http = require('http');
const express = require('express');
const path = require('path');

const port = process.env.SIGNAL_PORT || 5001;
const bridgePort = process.env.OMS_BRIDGE_PORT || 5002;
const app = express();

// Serve static files from public directory
app.use(express.static(path.join(__dirname, 'public')));

// Create receiver with our app
const receiver = new RESTSignalReceiver(port, 'signals.json', app);

// Create HTTP server
const server = http.createServer(app);

function forwardSignal(signal) {
    const data = JSON.stringify(signal);
    const options = {
        hostname: '127.0.0.1',
        port: bridgePort,
        path: '/signal',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data)
        }
    };

    const req = http.request(options, (res) => {
        let responseData = '';
        res.on('data', (chunk) => {
            responseData += chunk;
        });
        res.on('end', () => {
            console.log(`[index.js] Forward response (Status: ${res.statusCode}):`, responseData);
        });
    });

    req.on('error', (err) => {
        console.error('[index.js] Error forwarding signal to Python bridge:', err.message);
    });

    req.write(data);
    req.end();
}

// Register event handler for incoming signals
receiver.on('signal', (signal) => {
    console.log('[index.js] Webhook Signal Processed:', JSON.stringify(signal, null, 2));
    forwardSignal(signal);
});

// Start the server
server.listen(port, () => {
    console.log(`Webhook service and dashboard running on port ${port}`);
    console.log(`Dashboard: http://localhost:${port}`);
});

// Start the receiver with our server
receiver.start(server);
