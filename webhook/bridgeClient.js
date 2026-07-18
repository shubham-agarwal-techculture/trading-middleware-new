/**
 * HTTP client that forwards normalized signals to the Python bridge.
 */

/**
 * @param {object} signal - Normalized signal payload
 * @param {{ host?: string, port?: number|string, path?: string }} [opts]
 * @returns {Promise<{ statusCode: number, body: string }>}
 */
async function forward(signal, opts = {}) {
    const host = opts.host || process.env.BRIDGE_HOST || '127.0.0.1';
    const port = opts.port || process.env.BRIDGE_PORT || process.env.OMS_BRIDGE_PORT || 5002;
    const path = opts.path || '/signal';
    const url = `http://${host}:${port}${path}`;

    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(signal),
    });
    const body = await res.text();
    return { statusCode: res.status, body };
}

module.exports = { forward };
