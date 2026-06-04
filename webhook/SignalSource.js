/**
 * Interface for Signal Source.
 * Must emit 'signal' events with { symbol, action, quantity, priceType, metadata }
 */
class SignalSource {
    constructor() {
        if (this.constructor === SignalSource) {
            throw new Error("Abstract class cannot be instantiated");
        }
    }

    /**
     * Start listening for signals.
     */
    start() {
        throw new Error("Method 'start()' must be implemented");
    }

    /**
     * Stop listening for signals.
     */
    stop() {
        throw new Error("Method 'stop()' must be implemented");
    }
}

module.exports = SignalSource;
