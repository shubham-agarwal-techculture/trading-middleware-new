/**
 * Abstract base for signal sources.
 *
 * Concrete implementations must emit ``signal`` events with a normalized
 * payload (see ``normalizeSignal``):
 * ``{ symbol, action, quantity, position, orderType, productType, ... }``.
 */
class SignalSource {
    constructor() {
        if (this.constructor === SignalSource) {
            throw new Error('Abstract class cannot be instantiated');
        }
    }

    /** Start listening for signals. */
    start() {
        throw new Error("Method 'start()' must be implemented");
    }

    /** Stop listening for signals. */
    stop() {
        throw new Error("Method 'stop()' must be implemented");
    }
}

module.exports = SignalSource;
