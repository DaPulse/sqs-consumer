export declare class SQSError extends Error {
    code: string;
    statusCode: number;
    region: string;
    hostname: string;
    time: Date;
    retryable: boolean;
    constructor(message: string);
}
export declare class TimeoutError extends Error {
    constructor(message?: string);
}
