/**
 * HTTP client that forwards normalized signals to the Python bridge.
 */

const fs = require('fs');
const path = require('path');

const RUNTIME_PORT_FILE = path.join(__dirname, '..', '.bridge_http_port');

/**
 * Prefer the port written by the Python bridge when it auto-selected a free port.
 * @param {number|string} [fallback=5002]
 * @returns {number}
 */
function resolveBridgePort(fallback = 5002) {
    try {
        if (fs.existsSync(RUNTIME_PORT_FILE)) {
            const raw = fs.readFileSync(RUNTIME_PORT_FILE, 'utf8').trim();
            const port = Number(raw);
            if (Number.isFinite(port) && port > 0) return port;
        }
    } catch {
        // Fall through to configured / default port.
    }
    return Number(
        fallback ||
            process.env.BRIDGE_PORT ||
            process.env.OMS_BRIDGE_PORT ||
            5002
    );
}

/**
 * @param {object} signal - Normalized signal payload
 * @param {{ host?: string, port?: number|string, path?: string }} [opts]
 * @returns {Promise<{ statusCode: number, body: string }>}
 */
async function forward(signal, opts = {}) {
    const host = opts.host || process.env.BRIDGE_HOST || '127.0.0.1';
    const port =
        opts.port != null
            ? Number(opts.port)
            : resolveBridgePort(
                  process.env.BRIDGE_PORT || process.env.OMS_BRIDGE_PORT || 5002
              );
    const reqPath = opts.path || '/signal';
    const url = `http://${host}:${port}${reqPath}`;

    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(signal),
    });
    const body = await res.text();
    return { statusCode: res.status, body };
}

module.exports = { forward, resolveBridgePort };
