export class SQSError extends Error {
  code: string = '';
  statusCode: number = 500;
  region: string = '';
  hostname: string = '';
  time: Date = new Date();
  retryable: boolean = false;

  constructor(message: string) {
    super(message);
    this.name = 'SQSError';
  }
}

export class TimeoutError extends Error {
  constructor(message?: string) {
    super(message || 'Operation timed out');
    this.name = 'TimeoutError';
  }
}
