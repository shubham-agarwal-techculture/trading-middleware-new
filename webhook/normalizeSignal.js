/**
 * Normalize and validate an inbound webhook/dashboard signal body.
 *
 * @param {object} body - Raw request body
 * @returns {{ ok: true, signal: object } | { ok: false, error: string, received: object }}
 */
function normalizeSignal(body = {}) {
    const { symbol, action, quantity, position } = body;

    if (!position || !action || !quantity) {
        return {
            ok: false,
            error: 'Missing required fields: action, quantity, position',
            received: { body },
        };
    }

    const { orderType, limitPrice, productType, instrumentType } = body;

    return {
        ok: true,
        signal: {
            ...body,
            symbol,
            action: String(action).toUpperCase(),
            quantity: Number(quantity),
            position,
            orderType: String(orderType || 'LIMIT').toUpperCase(),
            limitPrice: limitPrice ? Number(limitPrice) : undefined,
            productType: String(productType || 'MIS').toUpperCase(),
            instrumentType: instrumentType
                ? String(instrumentType).toUpperCase()
                : undefined,
            timestamp: Date.now(),
            receivedAt: new Date().toISOString(),
        },
    };
}

module.exports = { normalizeSignal };
