"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TimeoutError = exports.SQSError = void 0;
class SQSError extends Error {
    constructor(message) {
        super(message);
        this.code = '';
        this.statusCode = 500;
        this.region = '';
        this.hostname = '';
        this.time = new Date();
        this.retryable = false;
        this.name = 'SQSError';
    }
}
exports.SQSError = SQSError;
class TimeoutError extends Error {
    constructor(message) {
        super(message || 'Operation timed out');
        this.name = 'TimeoutError';
    }
}
exports.TimeoutError = TimeoutError;
