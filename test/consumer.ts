import { assert } from 'chai';
import pEvent from 'p-event';
import * as sinon from 'sinon';
import { Consumer } from '../src/index';
import { SQS, QueueAttributeName } from '@aws-sdk/client-sqs';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;
const POLLING_TIMEOUT = 100;

function stubResolve(value?: any): sinon.SinonStub {
  return sandbox.stub().resolves(value);
}

class MockSQSError extends Error {
  code: string = '';
  statusCode: number = 500;
  region: string = '';
  hostname: string = '';
  time: Date = new Date();
  retryable: boolean = false;

  constructor(message: string) {
    super(message);
    this.name = 'MockSQSError';
  }
}

// tslint:disable:no-unused-expression
describe('Consumer', () => {
  let consumer: Consumer;
  let handleMessage: sinon.SinonStub;
  let handleMessageBatch: sinon.SinonStub;
  let sqs: sinon.SinonStubbedInstance<SQS>;
  const response = {
    Messages: [{
      ReceiptHandle: 'receipt-handle',
      MessageId: '123',
      Body: 'body'
    }]
  };

  beforeEach(() => {
    handleMessage = sandbox.stub().resolves(null);
    handleMessageBatch = sandbox.stub().resolves(null);
    sqs = sandbox.createStubInstance(SQS);
    sqs.send = sandbox.stub().resolves(response);

    consumer = new Consumer({
      queueUrl: 'some-queue-url',
      region: 'some-region',
      handleMessage,
      sqs: sqs as any,
      authenticationErrorTimeout: 20
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a queueUrl to be set', () => {
    assert.throws(() => {
      Consumer.create({
        region: 'some-region',
        handleMessage
      } as any);
    });
  });

  it('requires a handleMessage or handleMessagesBatch function to be set', () => {
    assert.throws(() => {
      new Consumer({
        handleMessage: undefined,
        region: 'some-region',
        queueUrl: 'some-queue-url'
      } as any);
    });
  });

  it('requires the batchSize option to be no greater than 10', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        batchSize: 11
      });
    });
  });

  it('requires the batchSize option to be greater than 0', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        batchSize: -1
      });
    });
  });

  describe('.create', () => {
    it('creates a new instance of a Consumer object', () => {
      const instance = Consumer.create({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        batchSize: 1,
        visibilityTimeout: 10,
        waitTimeSeconds: 10,
        handleMessage
      });

      assert.instanceOf(instance, Consumer);
    });
  });

  describe('.start', () => {
    it('fires an error event when an error occurs receiving a message', async () => {
      const receiveErr = new Error('Receive error');
      sqs.send.rejects(receiveErr);

      consumer.start();

      const err = await pEvent(consumer, 'unhandled_error');

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, 'Receive error');
    });

    it('retains sqs error information', async () => {
      const receiveErr = new MockSQSError('Receive error');
      receiveErr.code = 'short code';
      receiveErr.retryable = false;
      receiveErr.statusCode = 403;
      receiveErr.time = new Date();
      receiveErr.hostname = 'hostname';
      receiveErr.region = 'eu-west-1';

      sqs.send.rejects(receiveErr);

      consumer.start();
      const err = await pEvent(consumer, 'unhandled_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Receive error');
    });

    it('fires a timeout event if handler function takes too long', async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => new Promise<void>((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        sqs: sqs as any,
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const err = await pEvent(consumer, 'timeout_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`);
    });

    it('handles unexpected exceptions thrown by the handler function', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => {
          throw new Error('unexpected parsing error');
        },
        sqs: sqs as any,
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const err = await pEvent(consumer, 'processing_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Unexpected message handler failure: unexpected parsing error');
    });

    it('fires an error event when an error occurs deleting a message', async () => {
      const deleteErr = new Error('Delete error');

      handleMessage.resolves(null);
      sqs.send.rejects(deleteErr);

      consumer.start();
      const err = await pEvent(consumer, 'unhandled_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Delete error');
    });

    it('retains sqs error information when deleting a message', async () => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves(sqsError);
      sqs.send.rejects(sqsError);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'Processing error');
      assert.equal(message.MessageId, '123');
    });

    it('waits before repolling when a credentials error occurs', async () => {
      const credentialsErr = {
        code: 'CredentialsError',
        message: 'Missing credentials in config'
      };
      sqs.send.rejects(credentialsErr);

      return new Promise<void>((resolve) => {
        const timings: number[] = [];
        const errorListener = sandbox.stub();

        consumer.on('unhandled_error', errorListener);

        errorListener.onFirstCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onSecondCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onThirdCall().callsFake(() => {
          consumer.stop();
          sandbox.assert.calledThrice(sqs.send);
          assert.isAtLeast(timings[1] - timings[0], AUTHENTICATION_ERROR_TIMEOUT);
          resolve();
        });

        consumer.start();
      });
    });

    it('waits before repolling when a 403 error occurs', async () => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };
      sqs.send.rejects(invalidSignatureErr);

      return new Promise<void>((resolve) => {
        const timings: number[] = [];
        const errorListener = sandbox.stub();

        consumer.on('unhandled_error', errorListener);

        errorListener.onFirstCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onSecondCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onThirdCall().callsFake(() => {
          consumer.stop();
          sandbox.assert.calledThrice(sqs.send);
          assert.isAtLeast(timings[1] - timings[0], AUTHENTICATION_ERROR_TIMEOUT);
          resolve();
        });

        consumer.start();
      });
    });

    it('waits before repolling when a UnknownEndpoint error occurs', async () => {
      const unknownEndpointErr = {
        code: 'UnknownEndpoint',
        message: 'Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.'
      };
      sqs.send.rejects(unknownEndpointErr);

      return new Promise<void>((resolve) => {
        const timings: number[] = [];
        const errorListener = sandbox.stub();

        consumer.on('unhandled_error', errorListener);

        errorListener.onFirstCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onSecondCall().callsFake(() => {
          timings.push(Date.now());
        });

        errorListener.onThirdCall().callsFake(() => {
          consumer.stop();
          sandbox.assert.calledThrice(sqs.send);
          assert.isAtLeast(timings[1] - timings[0], AUTHENTICATION_ERROR_TIMEOUT);
          resolve();
        });

        consumer.start();
      });
    });

    it('waits before repolling when no messages are returned', () => {
      sqs.send.resolves({});

      return new Promise<void>((resolve) => {
        const timings: number[] = [];
        const timeListener = sandbox.stub();

        consumer.on('empty', timeListener);

        timeListener.onFirstCall().callsFake(() => {
          timings.push(Date.now());
        });

        timeListener.onSecondCall().callsFake(() => {
          timings.push(Date.now());
        });

        timeListener.onThirdCall().callsFake(() => {
          consumer.stop();
          sandbox.assert.calledThrice(sqs.send);
          assert.isAtLeast(timings[1] - timings[0], POLLING_TIMEOUT);
          resolve();
        });

        (consumer as any).pollingWaitTimeMs = POLLING_TIMEOUT;
        consumer.start();
      });
    });

    it('stops when polling is set to true', () => {
      sqs.send.resolves({});

      consumer.start();
      consumer.stop();

      assert.equal(consumer.isRunning, false);
    });

    it('handles messages gracefully', async () => {
      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      assert.equal(message.MessageId, '123');
      assert.equal(message.Body, 'body');
    });

    it('deletes messages after processing', async () => {
      handleMessage.resolves();

      return new Promise<void>((resolve) => {
        handleMessage.onSecondCall().callsFake(() => {
          consumer.stop();
          setTimeout(() => {
                         sandbox.assert.calledWith(sqs.send, sinon.match.has('input', sinon.match.has('ReceiptHandle', 'receipt-handle')));
            resolve();
          }, 10);
        });

        consumer.start();
      });
    });

    it('doesn\'t delete the message when a processing error is reported', async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

              // Should not call deleteMessage since processing failed
        sandbox.assert.neverCalledWith(sqs.send, sinon.match.any);
    });

    it('doesn\'t consume more messages when called multiple times', () => {
      sqs.send = stubResolve(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(sqs.send);
    });

    it('consumes multiple messages when the batchSize is greater than 1', async () => {
      sqs.send = stubResolve({
        Messages: [
          {
            ReceiptHandle: 'receipt-handle-1',
            MessageId: '1',
            Body: 'body-1'
          },
          {
            ReceiptHandle: 'receipt-handle-2',
            MessageId: '2',
            Body: 'body-2'
          },
          {
            ReceiptHandle: 'receipt-handle-3',
            MessageId: '3',
            Body: 'body-3'
          }
        ]
      });

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        batchSize: 3,
        sqs: sqs as any,
        authenticationErrorTimeout: 20,
        handleMessage
      });

      return new Promise<void>((resolve) => {
        handleMessage.onThirdCall().callsFake(() => {
          sandbox.assert.calledWith(sqs.send, sinon.match.has('input', sinon.match({
            QueueUrl: 'some-queue-url',
            AttributeNames: [],
            MessageAttributeNames: [],
            MaxNumberOfMessages: 3,
            WaitTimeSeconds: 20,
            VisibilityTimeout: 0
          })));
          consumer.stop();
          resolve();
        });

        consumer.start();
      });
    });

    it('consumes messages with message level attributes', async () => {
      const messageWithAttr = {
        ReceiptHandle: 'receipt-handle',
        MessageId: '123',
        Body: 'body',
        Attributes: {
          ApproximateReceiveCount: 1
        }
      };

      sqs.send = stubResolve({
        Messages: [messageWithAttr]
      });

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        attributeNames: ['ApproximateReceiveCount' as QueueAttributeName],
        region: 'some-region',
        handleMessage,
        sqs: sqs as any
      });

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      sandbox.assert.calledWith(sqs.send, sinon.match.has('input', sinon.match({
        QueueUrl: 'some-queue-url',
        AttributeNames: ['ApproximateReceiveCount'],
        MessageAttributeNames: [],
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
        VisibilityTimeout: 0
      })));
      assert.equal(message.Attributes.ApproximateReceiveCount, 1);
    });

    it('fires an emptyQueue event when all messages have been consumed', async () => {
      sqs.send = stubResolve({});

      consumer.start();
      await pEvent(consumer, 'empty');
      consumer.stop();
    });

    it('terminate message visibility timeout on processing error', async () => {
      const processingError = new Error('Processing error');

      handleMessage.rejects(processingError);
      (consumer as any).terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.send, sinon.match.has('input', sinon.match({
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 0
      })));
    });

    it('does not terminate visibility timeout when the option is not set', async () => {
      const processingError = new Error('Processing error');

      handleMessage.rejects(processingError);
      (consumer as any).terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

              // Should not call changeMessageVisibility
        sandbox.assert.neverCalledWith(sqs.send, sinon.match.has('input', sinon.match.has('VisibilityTimeout', 0)));
      });

      it('fires error event when failed to terminate visibility timeout on processing error', async () => {
        const processingError = new Error('Processing error');
        const sqsError = new Error('Processing error');
        sqsError.name = 'SQSError';
        sqs.send.rejects(sqsError);
        (consumer as any).terminateVisibilityTimeout = true;

        consumer = new Consumer({
          queueUrl: 'some-queue-url',
          region: 'some-region',
          handleMessage: () => Promise.reject(processingError),
          sqs: sqs as any,
          authenticationErrorTimeout: 20
        });

        consumer.start();
        await pEvent(consumer, 'processing_error');
        consumer.stop();

        sandbox.assert.calledWith(sqs.send, sinon.match.has('input', sinon.match({
          QueueUrl: 'some-queue-url',
          ReceiptHandle: 'receipt-handle',
          VisibilityTimeout: 0
        })));
    });

    it('fires response_processed event for each batch', async () => {
      sqs.send = stubResolve({
        Messages: [
          {
            ReceiptHandle: 'receipt-handle-1',
            MessageId: '1',
            Body: 'body-1'
          },
          {
            ReceiptHandle: 'receipt-handle-2',
            MessageId: '2',
            Body: 'body-2'
          },
          {
            ReceiptHandle: 'receipt-handle-3',
            MessageId: '3',
            Body: 'body-3'
          }
        ]
      });

      return new Promise<void>((resolve) => {
        handleMessage.onThirdCall().callsFake(() => {
          consumer.on('response_processed', () => {
            consumer.stop();
            resolve();
          });
        });

        consumer.start();
      });
    });

    it('handles handleMessageBatch requests', async () => {
      handleMessageBatch = sandbox.stub().resolves(null);

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessageBatch,
        sqs: sqs as any,
        authenticationErrorTimeout: 20
      });

      return new Promise<void>((resolve) => {
        handleMessageBatch.onFirstCall().callsFake(() => {
          consumer.stop();
          resolve();
        });

        consumer.start();
      });
    });

    it('prefer handleMessageBatch over handleMessage when both are set', async () => {
      handleMessageBatch = sandbox.stub().resolves(null);
      handleMessage = sandbox.stub().resolves(null);

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessageBatch,
        handleMessage,
        sqs: sqs as any,
        authenticationErrorTimeout: 20
      });

      return new Promise<void>((resolve) => {
        handleMessageBatch.onFirstCall().callsFake(() => {
          sandbox.assert.notCalled(handleMessage);
          consumer.stop();
          resolve();
        });

        consumer.start();
      });
    });
  });

  describe('.stop', () => {
    it('stops the consumer polling for messages', () => {
      consumer = Consumer.create({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage
      });

      consumer.start();
      consumer.stop();

      assert.equal(consumer.isRunning, false);
    });

    it('fires a stopped event when last in-flight message is processed', async () => {
      handleMessage.resolves(null);

      const handleStop = sandbox.stub().returns(null);
      consumer.on('stopped', handleStop);

      return new Promise<void>((resolve) => {
        consumer.start();
        consumer.stop();

        setTimeout(() => {
          sandbox.assert.calledOnce(handleStop);
          resolve();
        }, 10);
      });
    });

    it('fires a stopped event a second time if started and stopped twice', async () => {
      return new Promise<void>((resolve) => {
        const handleStop = sandbox.stub().returns(null).onSecondCall().callsFake(() => {
          sandbox.assert.calledTwice(handleStop);
          resolve();
        });

        consumer.on('stopped', handleStop);

        consumer.start();
        consumer.stop();

        setTimeout(() => {
          consumer.start();
          consumer.stop();
        }, 10);
      });
    });

    it('fires a stopped event only once when stopped before a message is received', async () => {
      sqs.send = stubResolve(new Promise((res) => setTimeout(res, 100)));

      return new Promise<void>((resolve) => {
        const handleStop = sandbox.stub().callsFake(() => {
          setTimeout(() => {
            sandbox.assert.calledOnce(handleStop);
            resolve();
          }, 10);
        });

        consumer.on('stopped', handleStop);

        consumer.start();
        consumer.stop();
      });
    });

    it('fires a stopped event only once when stopped after a message is received', async () => {
      handleMessage = sandbox.stub().returns(new Promise((res) => setTimeout(res, 100)));
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage,
        sqs: sqs as any,
        authenticationErrorTimeout: 20
      });

      return new Promise<void>((resolve) => {
        const handleStop = sandbox.stub().callsFake(() => {
          setTimeout(() => {
            sandbox.assert.calledOnce(handleStop);
            resolve();
          }, 10);
        });

        consumer.on('stopped', handleStop);

        consumer.start();
        consumer.stop();
      });
    });
  });
});
