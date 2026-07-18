/**
 * Webhook + dashboard entry point.
 *
 * Listens for TradingView (and other) HTTP signals on SIGNAL_PORT, normalizes
 * them, and forwards to the Python bridge. Also serves the trading dashboard.
 */
require('dotenv').config();
const http = require('http');
const express = require('express');
const path = require('path');
const RESTSignalReceiver = require('./RESTSignalReceiver');
const bridgeClient = require('./bridgeClient');

const port = Number(process.env.SIGNAL_PORT || 5001);
const bridgeHost = process.env.BRIDGE_HOST || '127.0.0.1';
const bridgePort = Number(
    process.env.BRIDGE_PORT || process.env.OMS_BRIDGE_PORT || 5002
);
const bridgeApiBase =
    process.env.BRIDGE_API_BASE || `http://${bridgeHost}:${bridgePort}`;

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// Dashboard runtime config (API_BASE for the Python bridge)
app.get('/runtime-config.js', (_req, res) => {
    res.type('application/javascript');
    res.send(`window.API_BASE = ${JSON.stringify(bridgeApiBase)};`);
});

const receiver = new RESTSignalReceiver(port, 'signals.json', app);
const server = http.createServer(app);

receiver.on('signal', async (signal) => {
    console.log('[server] Webhook Signal Processed:', JSON.stringify(signal, null, 2));
    try {
        const result = await bridgeClient.forward(signal, {
            host: bridgeHost,
            port: bridgePort,
        });
        console.log(
            `[server] Forward response (Status: ${result.statusCode}):`,
            result.body
        );
    } catch (err) {
        console.error('[server] Error forwarding signal to Python bridge:', err.message);
    }
});

server.listen(port, () => {
    console.log(`Webhook service and dashboard running on port ${port}`);
    console.log(`Dashboard: http://localhost:${port}`);
    console.log(`Bridge target: ${bridgeApiBase}`);
});

receiver.start(server);
