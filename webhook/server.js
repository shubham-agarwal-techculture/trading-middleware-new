/**
 * Webhook + dashboard entry point.
 *
 * Listens for TradingView (and other) HTTP signals on SIGNAL_PORT, normalizes
 * them, and forwards to the Python bridge. Also serves the trading dashboard.
 */
require('dotenv').config();
const fs = require('fs');
const http = require('http');
const path = require('path');
const express = require('express');
const RESTSignalReceiver = require('./RESTSignalReceiver');
const bridgeClient = require('./bridgeClient');

const PORT_RETRY_COUNT = 20;
const preferredPort = Number(process.env.SIGNAL_PORT || 5001);
const RUNTIME_PORT_FILE = path.join(__dirname, '..', '.webhook_http_port');
const bridgeHost = process.env.BRIDGE_HOST || '127.0.0.1';
const preferredBridgePort = Number(
    process.env.BRIDGE_PORT || process.env.OMS_BRIDGE_PORT || 5002
);

function resolveBridgePort() {
    return bridgeClient.resolveBridgePort(preferredBridgePort);
}

function bridgeApiBase() {
    if (process.env.BRIDGE_API_BASE) return process.env.BRIDGE_API_BASE;
    return `http://${bridgeHost}:${resolveBridgePort()}`;
}

function writeRuntimePort(port) {
    try {
        fs.writeFileSync(RUNTIME_PORT_FILE, String(port), 'utf8');
    } catch (err) {
        console.warn(`Could not write runtime port file ${RUNTIME_PORT_FILE}:`, err.message);
    }
}

/**
 * Bind server to preferredPort, or the next free port on EADDRINUSE.
 * @param {import('http').Server} server
 * @param {number} startPort
 * @param {number} [maxTries]
 * @returns {Promise<number>}
 */
function listenWithFallback(server, startPort, maxTries = PORT_RETRY_COUNT) {
    return new Promise((resolve, reject) => {
        let attempt = 0;

        const tryPort = () => {
            const port = startPort + attempt;
            const onError = (err) => {
                server.off('listening', onListening);
                if (err.code === 'EADDRINUSE' && attempt < maxTries - 1) {
                    console.warn(`Port ${port} in use, trying ${port + 1}...`);
                    attempt += 1;
                    // Ensure the failed listen attempt is cleaned up before retry.
                    server.close(() => setImmediate(tryPort));
                    return;
                }
                reject(err);
            };
            const onListening = () => {
                server.off('error', onError);
                resolve(port);
            };
            server.once('error', onError);
            server.once('listening', onListening);
            server.listen(port);
        };

        tryPort();
    });
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// Dashboard runtime config (API_BASE for the Python bridge).
// Re-read on each request so a late-started / re-bound bridge port is picked up.
app.get('/runtime-config.js', (_req, res) => {
    res.type('application/javascript');
    res.send(`window.API_BASE = ${JSON.stringify(bridgeApiBase())};`);
});

const receiver = new RESTSignalReceiver(preferredPort, 'signals.json', app);
const server = http.createServer(app);

receiver.on('signal', async (signal) => {
    console.log('[server] Webhook Signal Processed:', JSON.stringify(signal, null, 2));
    try {
        const port = resolveBridgePort();
        const result = await bridgeClient.forward(signal, {
            host: bridgeHost,
            port,
        });
        console.log(
            `[server] Forward response (Status: ${result.statusCode}):`,
            result.body
        );
    } catch (err) {
        console.error('[server] Error forwarding signal to Python bridge:', err.message);
    }
});

(async () => {
    try {
        try {
            if (fs.existsSync(RUNTIME_PORT_FILE)) {
                fs.unlinkSync(RUNTIME_PORT_FILE);
            }
        } catch {
            // Non-fatal; write after bind will overwrite anyway.
        }
        const port = await listenWithFallback(server, preferredPort);
        receiver.port = port;
        if (port !== preferredPort) {
            console.warn(
                `SIGNAL_PORT ${preferredPort} is in use; auto-selected port ${port} instead`
            );
        }
        writeRuntimePort(port);
        console.log(`Webhook service and dashboard running on port ${port}`);
        console.log(`Dashboard: http://localhost:${port}`);
        console.log(`Bridge target: ${bridgeApiBase()}`);
        receiver.start(server);
    } catch (err) {
        console.error(
            `Failed to bind webhook on ports ${preferredPort}-${preferredPort + PORT_RETRY_COUNT - 1}:`,
            err.message
        );
        process.exit(1);
    }
})();
