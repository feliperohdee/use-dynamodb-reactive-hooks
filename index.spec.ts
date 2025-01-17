import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import z, { date } from 'zod';

import Hooks, { SUBTASK_TTL_IN_MS, TaskException, taskShape } from './index';

// @ts-expect-error
global.fetch = vi.fn(async url => {
	return {
		headers: new Headers({
			'content-type': 'application/json'
		}),
		json: async () => {
			return { success: true, url };
		},
		ok: true,
		text: async () => {
			return JSON.stringify({ success: true, url });
		},
		status: 200
	};
});

const createTestTask = (scheduleDelay: number = 0, options?: Partial<Hooks.TaskInput>): Hooks.Task => {
	return taskShape({
		namespace: 'spec',
		repeatInterval: 30,
		repeatUnit: 'minutes',
		requestBody: { a: 1 },
		requestHeaders: { a: '1' },
		requestMethod: 'POST',
		requestUrl: 'https://httpbin.org/anything',
		scheduledDate: new Date(_.now() + scheduleDelay).toISOString(),
		...options
	});
};

const createTestSubTaskInput = (task: Hooks.Task, options?: Partial<Hooks.Task>): Hooks.Task => {
	return {
		...task,
		eventDelayDebounce: false,
		eventDelayUnit: 'minutes',
		eventDelayValue: 1,
		requestMethod: 'POST',
		requestUrl: 'https://httpbin.org/subtask',
		...options,
		requestBody: {
			...options?.requestBody,
			subTask: 1
		},
		requestHeaders: {
			...options?.requestHeaders,
			subTask: '1'
		}
	};
};

describe('/index.ts', () => {
	let hooks: Hooks;

	beforeAll(() => {
		hooks = new Hooks({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			logsTableName: 'use-dynamodb-reactive-hooks-logs-spec',
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tasksTableName: 'use-dynamodb-reactive-hooks-tasks-spec'
		});
	});

	beforeEach(() => {
		hooks = new Hooks({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			logsTableName: 'use-dynamodb-reactive-hooks-logs-spec',
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tasksTableName: 'use-dynamodb-reactive-hooks-tasks-spec'
		});
	});

	afterAll(async () => {
		await Promise.all([hooks.clearTasks('spec'), hooks.clearLogs('spec')]);
	});

	describe('calculateNextSchedule', () => {
		it('should calculates next time by minutes', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, {
				unit: 'minutes',
				value: 30
			});

			expect(res).toEqual(new Date('2024-03-18T10:30:00.000Z').toISOString());
		});

		it('should calculates next time by hours', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, {
				unit: 'hours',
				value: 2
			});

			expect(res).toEqual(new Date('2024-03-18T12:00:00.000Z').toISOString());
		});

		it('should calculates next time by days', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, {
				unit: 'days',
				value: 1
			});

			expect(res).toEqual(new Date('2024-03-19T10:00:00.000Z').toISOString());
		});

		it('should handle fractional intervals', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, {
				unit: 'hours',
				value: 1.5
			});

			expect(res).toEqual(new Date('2024-03-18T11:30:00.000Z').toISOString());
		});
	});

	describe('checkExecuteTask', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			// @ts-expect-error
			vi.spyOn(hooks, 'getTaskInternal');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should return task', async () => {
			// @ts-expect-error
			const res = await hooks.checkExecuteTask({
				date: new Date(),
				task
			});

			expect(hooks.getTaskInternal).toHaveBeenCalledWith({
				id: task.id,
				namespace: 'spec'
			});

			expect(res).toEqual(task);
		});

		it('should throw error if task is not found', async () => {
			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(),
					task: {
						...task,
						id: 'inexistent-id'
					}
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task not found');
			}
		});

		it('should throw error if task was modified', async () => {
			await hooks.db.tasks.update({
				filter: {
					item: { id: task.id, namespace: 'spec' }
				}
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task was modified');
			}
		});

		it('should throw error if task execution is after the noAfter date', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#noAfter': 'noAfter' },
				attributeValues: { ':noAfter': new Date().toISOString() },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #noAfter = :noAfter'
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(Date.now() + 5000),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must not be executed after the noAfter date');
			}
		});

		it('should throw error if task execution is before the noBefore date', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#noBefore': 'noBefore' },
				attributeValues: { ':noBefore': new Date().toISOString() },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #noBefore = :noBefore'
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(Date.now() - 5000),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must not be executed before the noBefore date');
			}
		});

		it('should throw error if task is already running', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#pid': 'pid' },
				attributeValues: { ':pid': '123' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #pid = :pid'
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is already running');
			}
		});

		it('should throw error if task has reached the repeat max', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#repeatMax': 'repeatMax',
					'#totalExecutions': 'totalExecutions'
				},
				attributeValues: {
					':repeatMax': 1,
					':totalExecutions': 1
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #repeatMax = :repeatMax, #totalExecutions = :totalExecutions'
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task has reached the repeat max');
			}
		});

		it('should throw error if task is not in a valid state', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: { ':status': 'DISABLED' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #status = :status'
			});

			try {
				// @ts-expect-error
				await hooks.checkExecuteTask({
					date: new Date(),
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is not in a valid state');
			}
		});
	});

	describe('callWebhook', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(
				createTestTask(0, {
					conditionFilter: {
						operator: 'EQUALS',
						path: ['key'],
						type: 'STRING',
						value: 'value'
					}
				})
			);

			// @ts-expect-error
			vi.spyOn(hooks, 'checkExecuteTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'getParentTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'getTaskInternal');
			// @ts-expect-error
			vi.spyOn(hooks, 'registerForkTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'registerScheduledSubTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskError');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskLock');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskSuccess');
			vi.spyOn(hooks.webhooks, 'trigger');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should works', async () => {
			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task
			});
			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				pid: expect.any(String),
				task
			});
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				metadata: {
					executionType: 'EVENT',
					taskId: task.id,
					taskType: 'PRIMARY'
				},
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});
			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
				expect.objectContaining({
					executionType: 'EVENT',
					log: expect.objectContaining({
						requestBody: { a: 1 },
						requestHeaders: { a: '1' },
						requestMethod: 'POST',
						requestUrl: 'https://httpbin.org/anything'
					}),
					pid: expect.any(String),
					task: {
						...task,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						pid: expect.any(String),
						status: 'PROCESSING'
					}
				})
			);

			expect(res).toEqual([
				{
					...task,
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'EVENT',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should works with [matched conditionData, requestBody, requestHeaders, requestMethod, requestUrl]', async () => {
			const res = await hooks.callWebhook({
				conditionData: { key: 'value' },
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: { b: 2 },
				requestHeaders: { b: '2' },
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/custom'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task
			});
			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				pid: expect.any(String),
				task
			});
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				metadata: {
					executionType: 'EVENT',
					taskId: task.id,
					taskType: 'PRIMARY'
				},
				namespace: 'spec',
				requestBody: { a: 1, b: 2 },
				requestHeaders: { a: '1', b: '2' },
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/custom',
				retryLimit: 3
			});
			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
				expect.objectContaining({
					executionType: 'EVENT',
					log: expect.objectContaining({
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom?a=1&b=2'
					}),
					pid: expect.any(String),
					task: {
						...task,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						pid: expect.any(String),
						status: 'PROCESSING'
					}
				})
			);

			expect(res).toEqual([
				{
					...task,
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'EVENT',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should works with [unmatched conditionData, requestBody, requestHeaders, requestMethod, requestUrl]', async () => {
			const res = await hooks.callWebhook({
				conditionData: { key: 'no-value' },
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskLock).not.toHaveBeenCalled();
			expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

			expect(res).toEqual([]);
		});

		it('should works with task.concurrency = true', async () => {
			task = taskShape(
				await hooks.db.tasks.update({
					attributeNames: { '#concurrency': 'concurrency' },
					attributeValues: { ':concurrency': true },
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: 'SET #concurrency = :concurrency'
				})
			);

			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task
			});
			// @ts-expect-error
			expect(hooks.setTaskLock).not.toHaveBeenCalled();
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				metadata: {
					executionType: 'EVENT',
					taskId: task.id,
					taskType: 'PRIMARY'
				},
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});
			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
				executionType: 'EVENT',
				log: expect.objectContaining({
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				}),
				pid: expect.any(String),
				task
			});

			expect(res).toEqual([
				{
					...task,
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'EVENT',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should handle inexistent task', async () => {
			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: 'inexistent-id', namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: 'inexistent-id', namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskLock).not.toHaveBeenCalled();
			expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

			expect(res).toEqual([]);
		});

		it('should handle ConditionalCheckFailedException', async () => {
			// @ts-expect-error
			vi.mocked(hooks.setTaskLock).mockRejectedValue(
				new ConditionalCheckFailedException({
					message: 'ConditionalCheckFailedException',
					$metadata: {}
				})
			);

			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task
			});
			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskError).not.toHaveBeenCalled();

			expect(res).toEqual([]);
		});

		it('should handle TaskException', async () => {
			// @ts-expect-error
			vi.mocked(hooks.setTaskLock).mockRejectedValue(new TaskException('Task is not in a valid state'));

			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskError).not.toHaveBeenCalled();

			expect(res).toEqual([]);
		});

		it('should handle exceptions', async () => {
			vi.mocked(hooks.webhooks.trigger).mockRejectedValue(new Error('test'));

			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: task.id, namespace: 'spec' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getParentTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task
			});
			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				pid: expect.any(String),
				task
			});
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				metadata: {
					executionType: 'EVENT',
					taskId: task.id,
					taskType: 'PRIMARY'
				},
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});
			// @ts-expect-error
			expect(hooks.setTaskError).toHaveBeenCalledWith({
				error: new Error('test'),
				executionType: 'EVENT',
				pid: expect.any(String),
				task: {
					...task,
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					pid: expect.any(String),
					status: 'PROCESSING'
				}
			});

			expect(res).toEqual([
				{
					...task,
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					firstErrorDate: expect.any(String),
					lastError: 'test',
					lastErrorDate: expect.any(String),
					lastErrorExecutionType: 'EVENT',
					totalErrors: 1
				}
			]);
		});

		describe('register forks', () => {
			it('should not register if executionType = SCHEDULED', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: 'fork-id',
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([]);
			});

			it('should register', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: 'fork-id',
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					parentTask: task
				});

				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: {
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						type: 'FORK'
					}
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: {
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						type: 'FORK'
					}
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						taskId: `${task.id}#fork-id`,
						taskType: 'FORK'
					},
					namespace: 'spec#FORK',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3
				});
				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
					expect.objectContaining({
						executionType: 'EVENT',
						log: expect.objectContaining({
							requestBody: { a: 1 },
							requestHeaders: { a: '1' },
							requestMethod: 'POST',
							requestUrl: 'https://httpbin.org/anything'
						}),
						pid: expect.any(String),
						task: {
							...task,
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							id: `${task.id}#fork-id`,
							namespace: 'spec#FORK',
							parentId: task.id,
							parentNamespace: 'spec',
							pid: expect.any(String),
							status: 'PROCESSING',
							type: 'FORK'
						}
					})
				);

				expect(res).toEqual([
					{
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						firstExecutionDate: expect.any(String),
						id: `${task.id}#fork-id`,
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'EVENT',
						lastResponseBody: expect.any(String),
						lastResponseHeaders: expect.any(Object),
						lastResponseStatus: 200,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						scheduledDate: expect.any(String),
						totalExecutions: 1,
						totalSuccessfulExecutions: 1,
						type: 'FORK'
					}
				]);
			});

			it('should register with forkOnly = true', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: 'fork-id',
					forkOnly: true,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					parentTask: task
				});

				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([]);
			});

			it('should register with [eventDelayValue, eventDelayUnit, eventDelayDebounce, requestBody, requestHeaders, requestMethod, requestUrl]', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					executionType: 'EVENT',
					forkId: 'fork-id',
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: { b: 2 },
					requestHeaders: { b: '2' },
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/custom'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					parentTask: {
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 0,
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom'
					}
				});

				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: {
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom',
						type: 'FORK'
					}
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: {
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom',
						type: 'FORK'
					}
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						taskId: `${task.id}#fork-id`,
						taskType: 'FORK'
					},
					namespace: 'spec#FORK',
					requestBody: { a: 1, b: 2 },
					requestHeaders: { a: '1', b: '2' },
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/custom',
					retryLimit: 3
				});
				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
					expect.objectContaining({
						executionType: 'EVENT',
						log: expect.objectContaining({
							requestBody: { a: 1, b: 2 },
							requestHeaders: { a: '1', b: '2' },
							requestMethod: 'GET',
							requestUrl: 'https://httpbin.org/custom?a=1&b=2'
						}),
						pid: expect.any(String),
						task: {
							...task,
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							id: `${task.id}#fork-id`,
							namespace: 'spec#FORK',
							parentId: task.id,
							parentNamespace: 'spec',
							requestBody: { a: 1, b: 2 },
							requestHeaders: { a: '1', b: '2' },
							requestMethod: 'GET',
							requestUrl: 'https://httpbin.org/custom',
							pid: expect.any(String),
							status: 'PROCESSING',
							type: 'FORK'
						}
					})
				);

				expect(res).toEqual([
					{
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						firstExecutionDate: expect.any(String),
						id: `${task.id}#fork-id`,
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'EVENT',
						lastResponseBody: expect.any(String),
						lastResponseHeaders: expect.any(Object),
						lastResponseStatus: 200,
						namespace: 'spec#FORK',
						parentId: task.id,
						parentNamespace: 'spec',
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom',
						scheduledDate: expect.any(String),
						totalExecutions: 1,
						totalSuccessfulExecutions: 1,
						type: 'FORK'
					}
				]);
			});
		});

		describe('register subTasks', () => {
			beforeEach(async () => {
				task = await hooks.registerTask(
					createTestTask(0, {
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				);
			});

			it('should not register if executionType = SCHEDULED', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([]);
			});

			it('should register', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).toHaveBeenCalledWith({
					...task,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([
					{
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						conditionFilter: null,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 0,
						firstScheduledDate: expect.any(String),
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]`),
						namespace: 'spec#SUBTASK',
						parentId: task.id,
						parentNamespace: 'spec',
						repeatInterval: 0,
						scheduledDate: expect.any(String),
						ttl: expect.any(Number),
						type: 'SUBTASK'
					}
				]);
			});

			it('should register with [eventDelayValue, eventDelayUnit, eventDelayDebounce, requestBody, requestHeaders, requestMethod, requestUrl]', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 10,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [{ id: task.id, namespace: 'spec' }],
					requestBody: { b: 2 },
					requestHeaders: { b: '2' },
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/custom'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).toHaveBeenCalledWith({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 10,
					requestBody: { a: 1, b: 2 },
					requestHeaders: { a: '1', b: '2' },
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/custom'
				});
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([
					{
						...task,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						conditionFilter: null,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 0,
						firstScheduledDate: expect.any(String),
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						parentId: task.id,
						parentNamespace: 'spec',
						repeatInterval: 0,
						requestBody: { a: 1, b: 2 },
						requestHeaders: { a: '1', b: '2' },
						requestMethod: 'GET',
						requestUrl: 'https://httpbin.org/custom',
						scheduledDate: expect.any(String),
						ttl: expect.any(Number),
						type: 'SUBTASK'
					}
				]);
			});
		});

		describe('execute subTasks', () => {
			let task: Hooks.Task;
			let subTask: Hooks.Task;

			beforeEach(async () => {
				task = await hooks.registerTask(createTestTask());

				// @ts-expect-error
				subTask = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				// @ts-expect-error
				vi.mocked(hooks.registerScheduledSubTask).mockClear();
			});

			it('should not execute if executionType = EVENT', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [{ id: subTask.id, namespace: 'spec' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: subTask.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([]);
			});

			it('should execute', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: [{ id: subTask.id, namespace: subTask.namespace }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerScheduledSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getParentTask).toHaveBeenCalledWith(subTask);
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'SCHEDULED',
						taskId: task.id,
						taskType: 'SUBTASK'
					},
					namespace: 'spec',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3
				});
				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
					expect.objectContaining({
						executionType: 'SCHEDULED',
						log: expect.objectContaining({
							requestBody: { a: 1 },
							requestHeaders: { a: '1' },
							requestMethod: 'POST',
							requestUrl: 'https://httpbin.org/anything'
						}),
						pid: expect.any(String),
						task: {
							...task,
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pid: expect.any(String),
							status: 'PROCESSING'
						}
					})
				);

				expect(res).toEqual([
					{
						...task,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						firstExecutionDate: expect.any(String),
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'SCHEDULED',
						lastResponseBody: expect.any(String),
						lastResponseHeaders: expect.any(Object),
						lastResponseStatus: 200,
						scheduledDate: expect.any(String),
						totalExecutions: 1,
						totalSuccessfulExecutions: 1
					}
				]);
			});
		});
	});

	describe('clearLogs', () => {
		beforeEach(() => {
			vi.spyOn(hooks.webhooks, 'clearLogs');
		});

		it('should clear namespace', async () => {
			const res = await hooks.clearLogs('spec');

			expect(res.count).toEqual(0);
		});
	});

	describe('clearTasks', () => {
		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'clear');
		});

		it('should clear namespace', async () => {
			await Promise.all([hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask())]);

			const res = await hooks.clearTasks('spec');
			expect(res.count).toEqual(3);

			expect(hooks.db.tasks.clear).toHaveBeenCalledWith('spec');
			expect(hooks.db.tasks.clear).toHaveBeenCalledWith('spec#FORK');
			expect(hooks.db.tasks.clear).toHaveBeenCalledWith('spec#SUBTASK');
			expect(hooks.db.tasks.clear).toHaveBeenCalledWith('spec#FORK#SUBTASK');

			const remaining = await hooks.db.tasks.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('deleteTask', () => {
		let task: Hooks.Task;
		let forkTask: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());
			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				parentTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(task, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(forkTask)),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(forkTask, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);

			vi.spyOn(hooks.db.tasks, 'delete');
			vi.spyOn(hooks.db.tasks, 'deleteMany');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should delete', async () => {
			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace'
				},
				attributeValues: {
					':id': task.id,
					':namespace': 'spec'
				},
				filterExpression: 'begins_with(#namespace, :namespace) AND begins_with(#id, :id)',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 6,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#fork-id#DELAY`),
						namespace: 'spec#FORK#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#fork-id#DELAY-DEBOUNCE`,
						namespace: 'spec#FORK#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					}
				]),
				lastEvaluatedKey: null
			});

			await hooks.deleteTask({
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledTimes(3);
			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec#FORK'
				},
				prefix: true
			});
			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec#FORK#SUBTASK'
				},
				prefix: true
			});
			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec#SUBTASK'
				},
				prefix: true
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace'
				},
				attributeValues: {
					':id': task.id,
					':namespace': 'spec'
				},
				filterExpression: 'begins_with(#namespace, :namespace) AND begins_with(#id, :id)',
				select: ['id', 'namespace']
			});

			expect(retrieved).toEqual({
				count: 0,
				items: [],
				lastEvaluatedKey: null
			});
		});

		it('should throw if task is not primary', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'FORK' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #type = :type'
			});

			try {
				await hooks.deleteTask({
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must be a primary task');
			}
		});

		it('should return null if inexistent', async () => {
			const res = await hooks.deleteTask({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(res).toBeNull();
		});
	});

	describe.todo('fetchLogs', () => {
		beforeAll(async () => {
			await Promise.all([hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask())]);
		});

		afterAll(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should fetch logs', async () => {
			await hooks.trigger();

			const res = await hooks.fetchLogs({
				namespace: 'spec'
			});

			expect(res).toEqual({
				count: 3,
				items: expect.any(Array),
				lastEvaluatedKey: null
			});
		});
	});

	describe.todo('fetchTasks', () => {
		let tasks: Hooks.Task[];
		let subTasks: Hooks.Task[];

		beforeAll(async () => {
			tasks = await Promise.all([
				hooks.registerTask(
					createTestTask(0, {
						eventPattern: 'event-pattern-1'
					})
				),
				hooks.registerTask(
					createTestTask(1000, {
						eventPattern: 'event-pattern-2'
					})
				),
				hooks.registerTask(
					createTestTask(2000, {
						eventPattern: 'event-pattern-3'
					})
				)
			]);

			subTasks = await Promise.all([
				// @ts-expect-error
				hooks.registerForkTask({
					parentTask: tasks[0]
				}),
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(tasks[0])),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(tasks[0], {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);
		});

		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'query');
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should fetch by [namespace]', async () => {
			const res = await hooks.fetchTasks({
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: expect.arrayContaining(tasks),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] with delayed subTasks', async () => {
			const res = await hooks.fetchTasks({
				namespace: 'spec',
				type: 'DELAY'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#SUBTASK-DELAY' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: expect.arrayContaining(_.filter(subTasks, { type: 'DELAY' })),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] with debounced subTasks', async () => {
			const res = await hooks.fetchTasks({
				namespace: 'spec',
				type: 'DELAY-DEBOUNCE'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#SUBTASK-DELAY-DEBOUNCE' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 2,
				items: expect.arrayContaining(_.filter(subTasks, { type: 'DELAY-DEBOUNCE' })),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] with identified debounced subTasks', async () => {
			const res = await hooks.fetchTasks({
				delayDebounceId: 'debounce-',
				id: tasks[0].id,
				idPrefix: true,
				namespace: 'spec',
				type: 'DELAY-DEBOUNCE'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#namespace': 'namespace',
					'#id': 'id'
				},
				attributeValues: {
					':namespace': 'spec#SUBTASK-DELAY-DEBOUNCE',
					':id': `${tasks[0].id}#debounce-`
				},
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace AND begins_with(#id, :id)',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: expect.arrayContaining(
					_.filter(subTasks, {
						namespace: 'spec#SUBTASK-DELAY-DEBOUNCE#debounce-id',
						type: 'DELAY-DEBOUNCE'
					})
				),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, desc, chunkLimit, onChunk, status]', async () => {
			const res = await hooks.fetchTasks({
				chunkLimit: 1,
				desc: true,
				namespace: 'spec',
				onChunk: async () => {},
				status: 'ACTIVE'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#namespace': 'namespace',
					'#status': 'status'
				},
				attributeValues: {
					':namespace': 'spec',
					':status': 'ACTIVE'
				},
				chunkLimit: 1,
				filterExpression: '#status = :status',
				limit: 100,
				onChunk: expect.any(Function),
				queryExpression: '#namespace = :namespace',
				scanIndexForward: false,
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: expect.arrayContaining(tasks),
				lastEvaluatedKey: null
			});
		});

		describe('query by [id]', () => {
			it('should fetch by [namespace, id]', async () => {
				const res = await hooks.fetchTasks({
					id: tasks[0].id.slice(0, 8),
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace'
					},
					attributeValues: {
						':id': tasks[0].id.slice(0, 8),
						':namespace': 'spec'
					},
					filterExpression: '',
					limit: 100,
					queryExpression: '#id = :id AND #namespace = :namespace',
					scanIndexForward: true,
					select: ['id', 'namespace'],
					startKey: null,
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id] with idPrefix = true', async () => {
				const res = await hooks.fetchTasks({
					id: tasks[0].id.slice(0, 8),
					idPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace'
					},
					attributeValues: {
						':id': tasks[0].id.slice(0, 8),
						':namespace': 'spec'
					},
					filterExpression: '',
					limit: 100,
					queryExpression: '#namespace = :namespace AND begins_with(#id, :id)',
					scanIndexForward: true,
					select: ['id', 'namespace'],
					startKey: null,
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[0]],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id, eventPattern, scheduledDate, status]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					id: tasks[0].id.slice(0, 8),
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#eventPattern': 'eventPattern',
						'#id': 'id',
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':eventPattern': 'event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':id': tasks[0].id.slice(0, 8),
						':namespace': 'spec',
						':status': 'ACTIVE',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression:
						'#eventPattern = :eventPattern AND #scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate AND #status = :status',
					limit: 100,
					queryExpression: '#id = :id AND #namespace = :namespace',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by [eventPattern]', () => {
			it('should fetch by [namespace, eventPattern]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#eventPattern': 'eventPattern'
					},
					attributeValues: {
						':namespace': 'spec',
						':eventPattern': 'event-pattern-'
					},
					filterExpression: '',
					index: 'namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #eventPattern = :eventPattern',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern] with eventPatternPrefix = true', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#eventPattern': 'eventPattern'
					},
					attributeValues: {
						':namespace': 'spec',
						':eventPattern': 'event-pattern-'
					},
					filterExpression: '',
					index: 'namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND begins_with(#eventPattern, :eventPattern)',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 3,
					items: expect.arrayContaining(tasks),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern, scheduledDate]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#eventPattern': 'eventPattern',
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate'
					},
					attributeValues: {
						':eventPattern': 'event-pattern-',
						':namespace': 'spec',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression: '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					index: 'namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #eventPattern = :eventPattern',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by [eventPattern, status]', () => {
			it('should fetch by [namespace, eventPattern, status]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': 'namespace__eventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__eventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'trigger-status-namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace__eventPattern = :namespace__eventPattern AND #status = :status',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern, status] with eventPatternPrefix = true', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': 'namespace__eventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__eventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'trigger-status-namespace-event-pattern',
					limit: 100,
					queryExpression: 'begins_with(#namespace__eventPattern, :namespace__eventPattern) AND #status = :status',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 3,
					items: expect.arrayContaining(tasks),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern, fromScheduledDate, toScheduledDate, status]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': 'namespace__eventPattern',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__eventPattern': 'spec#event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':toScheduledDate': '2024-03-18T10:00:00.000Z',
						':status': 'ACTIVE'
					},
					filterExpression: '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					index: 'trigger-status-namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace__eventPattern = :namespace__eventPattern AND #status = :status',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by [scheduledDate]', () => {
			it('should fetch by [namespace, scheduledDate]', async () => {
				const res = await hooks.fetchTasks({
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate'
					},
					attributeValues: {
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':namespace': 'spec',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression: '',
					index: 'namespace-scheduled-date',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, scheduledDate, status]', async () => {
				const res = await hooks.fetchTasks({
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':namespace': 'spec',
						':toScheduledDate': '2024-03-18T10:00:00.000Z',
						':status': 'ACTIVE'
					},
					filterExpression: '#status = :status',
					index: 'namespace-scheduled-date',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
		});
	});

	describe('getParentTask', () => {
		let task: Hooks.Task;
		let subTask: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask({
				namespace: 'spec',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			subTask = await hooks.registerScheduledSubTask(createTestSubTaskInput(task));
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should return parent task', async () => {
			// @ts-expect-error
			const res = await hooks.getParentTask(subTask);

			expect(res).toEqual({
				...task,
				__ts: res.__ts,
				__updatedAt: res.__updatedAt
			});
		});

		it('should throw if no parentId neither parentNamespace', async () => {
			try {
				// @ts-expect-error
				await hooks.getParentTask({
					...subTask,
					parentId: '',
					parentNamespace: ''
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Input is not a subtask');
			}
		});

		it('should throw if parent task is not found', async () => {
			try {
				// @ts-expect-error
				await hooks.getParentTask({
					...subTask,
					parentNamespace: 'spec',
					parentId: 'non-existent-id'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Parent task not found');
			}
		});

		it('should throw if parent task is not a primary or fork task', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'SUBTASK-DELAY' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #type = :type'
			});

			try {
				// @ts-expect-error
				await hooks.getParentTask({
					...subTask,
					parentNamespace: 'spec',
					parentId: task.id
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Parent task must be a primary or fork task');
			}
		});
	});

	describe('getTask', () => {
		let task: Hooks.Task;
		let forkTask: Hooks.Task;
		let subTasks: Hooks.Task[];

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				parentTask: task
			});
			subTasks = await Promise.all([
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(task, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);
		});

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'get');
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should get', async () => {
			const res = await hooks.getTask({
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: expect.any(String),
					namespace: 'spec'
				},
				prefix: false
			});

			expect(res).toEqual(task);
		});

		it.only('should get forked task', async () => {
			const res = await hooks.getTask({
				forkId: 'fork-id',
				id: forkTask.parentId,
				namespace: forkTask.parentNamespace
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: `${forkTask.parentId}#fork-id`,
					namespace: `${forkTask.parentNamespace}#FORK`
				},
				prefix: true
			});

			expect(res).toEqual(forkTask);
		});

		it('should get delayed subTask', async () => {
			const res = await hooks.getTask({
				id: subTasks[0].parentId,
				namespace: subTasks[0].parentNamespace,
				type: 'SUBTASK-DELAY'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: `${subTasks[0].parentId}#DELAY`,
					namespace: `${subTasks[0].parentNamespace}#SUBTASK`
				},
				prefix: true
			});

			expect(res).toEqual(subTasks[0]);
		});

		it('should get debounced subTask', async () => {
			const res = await hooks.getTask({
				id: subTasks[1].parentId,
				namespace: subTasks[1].parentNamespace,
				type: 'SUBTASK-DELAY-DEBOUNCE'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: `${subTasks[1].parentId}#DELAY-DEBOUNCE`,
					namespace: `${subTasks[1].parentNamespace}#SUBTASK`
				},
				prefix: false
			});

			expect(res).toEqual(subTasks[1]);
		});

		it('should return null if inexistent', async () => {
			const res = await hooks.getTask({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: 'non-existent-id',
					namespace: 'spec'
				},
				prefix: false
			});

			expect(res).toBeNull();
		});
	});

	describe('getTaskInternal', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should get', async () => {
			// @ts-expect-error
			const res = await hooks.getTaskInternal({
				id: task.id,
				namespace: 'spec'
			});

			expect(res).toEqual(task);
		});
	});

	describe('queryActiveTasks', () => {
		let tasks: Hooks.Task[];

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'query');
		});

		describe('edge cases', () => {
			beforeEach(async () => {
				tasks = await Promise.all([hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask())]);
			});

			afterEach(async () => {
				await hooks.clearTasks('spec');
			});

			it('should no return tasks with pid', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#pid': 'pid' },
					attributeValues: { ':pid': 'test' },
					filter: {
						item: { id: tasks[0].id, namespace: 'spec' }
					},
					updateExpression: 'SET #pid = :pid'
				});

				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':now': expect.any(String),
						':startOfTimes': '0000-00-00T00:00:00.000Z',
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});

			it('should no return tasks with totalExecutions > repeatMax', async () => {
				await Promise.all([
					hooks.db.tasks.update({
						attributeNames: {
							'#repeatMax': 'repeatMax',
							'#totalExecutions': 'totalExecutions'
						},
						attributeValues: {
							':repeatMax': 1,
							':totalExecutions': 1
						},
						filter: {
							item: { id: tasks[0].id, namespace: 'spec' }
						},
						updateExpression: 'SET #totalExecutions = :totalExecutions, #repeatMax = :repeatMax'
					}),
					hooks.db.tasks.update({
						attributeNames: {
							'#totalExecutions': 'totalExecutions'
						},
						attributeValues: {
							':totalExecutions': 1000
						},
						filter: {
							item: { id: tasks[1].id, namespace: 'spec' }
						},
						updateExpression: 'SET #totalExecutions = :totalExecutions'
					})
				]);

				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':now': expect.any(String),
						':startOfTimes': '0000-00-00T00:00:00.000Z',
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});

			it('should no return tasks with now < noBefore', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#noBefore': 'noBefore' },
					attributeValues: { ':noBefore': '3000-01-01T00:00:00.000Z' },
					filter: {
						item: { id: tasks[0].id, namespace: 'spec' }
					},
					updateExpression: 'SET #noBefore = :noBefore'
				});

				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':now': expect.any(String),
						':startOfTimes': '0000-00-00T00:00:00.000Z',
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});

			it('should no return tasks with now > noAfter', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#noAfter': 'noAfter' },
					attributeValues: { ':noAfter': '1999-01-01T00:00:00.000Z' },
					filter: {
						item: { id: tasks[0].id, namespace: 'spec' }
					},
					updateExpression: 'SET #noAfter = :noAfter'
				});

				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':now': expect.any(String),
						':startOfTimes': '0000-00-00T00:00:00.000Z',
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});
		});

		describe('by eventPattern', () => {
			beforeAll(async () => {
				tasks = await Promise.all([
					hooks.registerTask(
						createTestTask(0, {
							eventPattern: ''
						})
					),
					hooks.registerTask(
						createTestTask(0, {
							eventPattern: 'event-pattern-1'
						})
					),
					hooks.registerTask(
						createTestTask(0, {
							eventPattern: 'event-pattern-2'
						})
					)
				]);
			});

			afterAll(async () => {
				await hooks.clearTasks('spec');
			});

			it('should fetch by [namespace, eventPattern]', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					eventPattern: 'event-pattern-1',
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': 'namespace__eventPattern',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':namespace__eventPattern': 'spec#event-pattern-1',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-namespace-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#namespace__eventPattern = :namespace__eventPattern AND #status = :active',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern] with eventPatternPrefix = true', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': 'namespace__eventPattern',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':namespace__eventPattern': 'spec#event-pattern-',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-namespace-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: 'begins_with(#namespace__eventPattern, :namespace__eventPattern) AND #status = :active',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 2,
					items: expect.arrayContaining([
						{
							id: tasks[1].id,
							namespace: 'spec'
						},
						{
							id: tasks[2].id,
							namespace: 'spec'
						}
					]),
					lastEvaluatedKey: null
				});
			});
		});

		describe('by id', () => {
			beforeAll(async () => {
				tasks = await Promise.all([hooks.registerTask(createTestTask()), hooks.registerTask(createTestTask())]);
			});

			afterAll(async () => {
				await hooks.clearTasks('spec');
			});

			it('should fetch by [namespace, id]', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					id: tasks[0].id,
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':id': tasks[0].id,
						':namespace': 'spec',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)',
						'#status = :active'
					].join(' AND '),
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#id = :id AND #namespace = :namespace',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[0].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});
		});

		describe('by scheduledDate', () => {
			beforeAll(async () => {
				tasks = await Promise.all([
					hooks.registerTask(createTestTask(0, { scheduledDate: '' })),
					hooks.registerTask(createTestTask(0)),
					hooks.registerTask(createTestTask(5000))
				]);
			});

			afterAll(async () => {
				await hooks.clearTasks('spec');
			});

			it('should fetch', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeatMax': 'repeatMax',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':now': expect.any(String),
						':startOfTimes': '0000-00-00T00:00:00.000Z',
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'#pid = :empty',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'trigger-status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now',
					select: ['id', 'namespace'],
					strictChunkLimit: true
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							id: tasks[1].id,
							namespace: 'spec'
						}
					],
					lastEvaluatedKey: null
				});
			});
		});
	});

	describe('registerForkTask', () => {
		let currentYear = new Date().getFullYear();
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask({
				conditionFilter: {
					operator: 'EQUALS',
					path: ['key'],
					type: 'STRING',
					value: 'value'
				},
				description: 'description',
				eventDelayDebounce: true,
				eventDelayUnit: 'minutes',
				eventDelayValue: 30,
				eventPattern: 'event-pattern',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				repeatInterval: 1,
				repeatMax: 1,
				repeatUnit: 'hours',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				scheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`
			});

			task = await hooks.db.tasks.update({
				filter: {
					item: {
						id: task.id,
						namespace: 'spec'
					}
				},
				updateFunction: item => {
					return {
						...item,
						firstErrorDate: '2025-01-12T00:00:00.000Z',
						firstExecutionDate: '2025-01-12T00:00:00.000Z',
						firstScheduledDate: '2025-01-12T00:00:00.000Z',
						lastError: 'last-error',
						lastErrorDate: '2025-01-12T00:00:00.000Z',
						lastErrorExecutionType: 'EVENT',
						lastExecutionDate: '2025-01-12T00:00:00.000Z',
						lastExecutionType: 'EVENT',
						lastResponseBody: 'last-response-body',
						lastResponseHeaders: {
							'last-response-header': 'last-response-header'
						},
						lastResponseStatus: 200,
						totalErrors: 1,
						totalExecutions: 1,
						totalFailedExecutions: 1,
						totalSuccessfulExecutions: 1,
						ttl: 1
					};
				}
			});

			vi.spyOn(hooks.db.tasks, 'put');
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should create fork', async () => {
			// @ts-expect-error
			const res = await hooks.registerForkTask({
				forkId: 'fork-id',
				parentTask: task
			});

			expect(hooks.db.tasks.put).toHaveBeenCalledWith({
				__createdAt: '',
				__ts: 0,
				__updatedAt: '',
				concurrency: false,
				conditionFilter: {
					defaultValue: '',
					normalize: true,
					operator: 'EQUALS',
					path: ['key'],
					type: 'STRING',
					value: 'value'
				},
				description: 'description',
				eventDelayDebounce: true,
				eventDelayUnit: 'minutes',
				eventDelayValue: 30,
				eventPattern: 'event-pattern',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: `${task.id}#fork-id`,
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec#FORK',
				namespace__eventPattern: 'spec#event-pattern',
				noAfter: `${currentYear + 1}-01-01T03:00:00.000Z`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				parentId: task.id,
				parentNamespace: 'spec',
				pid: '',
				repeatInterval: 1,
				repeatMax: 1,
				repeatUnit: 'hours',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'FORK'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				description: 'description',
				conditionFilter: {
					defaultValue: '',
					normalize: true,
					operator: 'EQUALS',
					path: ['key'],
					type: 'STRING',
					value: 'value'
				},
				eventDelayDebounce: true,
				eventDelayUnit: 'minutes',
				eventDelayValue: 30,
				eventPattern: 'event-pattern',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
				id: `${task.id}#fork-id`,
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec#FORK',
				namespace__eventPattern: 'spec#event-pattern',
				noAfter: `${currentYear + 1}-01-01T03:00:00.000Z`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				parentId: task.id,
				parentNamespace: 'spec',
				pid: '',
				repeatInterval: 1,
				repeatMax: 1,
				repeatUnit: 'hours',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'FORK'
			});
		});

		it('should throw if parent task is disabled', async () => {
			try {
				// @ts-expect-error
				await hooks.registerForkTask({
					forkId: 'fork-id',
					parentTask: {
						...task,
						status: 'DISABLED'
					}
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Parent task is disabled');
			}
		});

		it('should throw if parent task is not a primary task', async () => {
			try {
				// @ts-expect-error
				await hooks.registerForkTask({
					forkId: 'fork-id',
					parentTask: {
						...task,
						type: 'FORK'
					}
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Parent task must be a primary task');
			}
		});

		it('should throw if fork task already exists', async () => {
			// @ts-expect-error
			await hooks.registerForkTask({
				forkId: 'fork-id',
				parentTask: task
			});

			try {
				// @ts-expect-error
				await hooks.registerForkTask({
					forkId: 'fork-id',
					parentTask: task
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Fork task already exists');
			}
		});
	});

	describe('registerScheduledSubTask', () => {
		let task: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask({
				namespace: 'spec',
				requestUrl: 'https://httpbin.org/anything'
			});
		});

		beforeEach(() => {
			// @ts-expect-error
			vi.spyOn(hooks, 'countSubTasks').mockResolvedValue(1);
			vi.spyOn(hooks.db.tasks, 'put');
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		describe('delay subTask', () => {
			it('should create', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						parentId: task.id,
						parentNamespace: 'spec',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: task.id,
					parentNamespace: 'spec',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});

			it('should create by fork', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1,
					id: `${task.id}#fork-id`,
					namespace: 'spec#FORK',
					type: 'FORK'
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#fork-id#DELAY#[0-9]+`),
						namespace: 'spec#FORK#SUBTASK',
						namespace__eventPattern: '-',
						parentId: `${task.id}#fork-id`,
						parentNamespace: 'spec#FORK',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#fork-id#DELAY#[0-9]+`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#FORK#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: `${task.id}#fork-id`,
					parentNamespace: 'spec#FORK',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});

			it('should throw if parent task is disabled', async () => {
				try {
					// @ts-expect-error
					await hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						status: 'DISABLED'
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Parent task is disabled');
				}
			});

			it('should throw if parent task is not a primary or fork task', async () => {
				try {
					// @ts-expect-error
					await hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						type: 'SUBTASK'
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Parent task must be a primary or fork task');
				}
			});

			it('should throw if parent task has reached the repeat max by totalExecutions', async () => {
				try {
					// @ts-expect-error
					await hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						repeatMax: 1,
						totalExecutions: 1
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Parent task has reached the repeat max by totalExecutions');
				}
			});

			it('should throw if parent task has reached the repeat max by countSubTasks', async () => {
				try {
					// @ts-expect-error
					await hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						repeatMax: 1
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Parent task has reached the repeat max by totalSubTasks');
				}
			});
		});

		describe('debounce delay subTask', () => {
			it('should create', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#DELAY-DEBOUNCE`),
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						parentId: task.id,
						parentNamespace: 'spec',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#DELAY-DEBOUNCE`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: task.id,
					parentNamespace: 'spec',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});

			it('should create by fork', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1,
					id: `${task.id}#fork-id`,
					namespace: 'spec#FORK',
					type: 'FORK'
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#fork-id#DELAY-DEBOUNCE`),
						namespace: 'spec#FORK#SUBTASK',
						namespace__eventPattern: '-',
						parentId: `${task.id}#fork-id`,
						parentNamespace: 'spec#FORK',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#fork-id#DELAY-DEBOUNCE`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#FORK#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: `${task.id}#fork-id`,
					parentNamespace: 'spec#FORK',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});
		});

		describe('existing debounce delay subTask', () => {
			beforeEach(async () => {
				await Promise.all([
					// @ts-expect-error
					hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					}),
					// @ts-expect-error
					hooks.registerScheduledSubTask({
						...task,
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						type: 'FORK'
					})
				]);
			});

			it('should update', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#DELAY-DEBOUNCE`),
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						parentId: task.id,
						parentNamespace: 'spec',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).not.toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#DELAY-DEBOUNCE`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: task.id,
					parentNamespace: 'spec',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});

			it('should update by fork', async () => {
				// @ts-expect-error
				const res = await hooks.registerScheduledSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1,
					id: `${task.id}#fork-id`,
					namespace: 'spec#FORK',
					type: 'FORK'
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: expect.stringMatching(`${task.id}#fork-id#DELAY-DEBOUNCE`),
						namespace: 'spec#FORK#SUBTASK',
						namespace__eventPattern: '-',
						parentId: `${task.id}#fork-id`,
						parentNamespace: 'spec#FORK',
						type: 'SUBTASK'
					}),
					{
						overwrite: true,
						useCurrentCreatedAtIfExists: true
					}
				);

				const scheduledDateDiff = new Date(res!.scheduledDate).getTime() - Date.now();

				expect(scheduledDateDiff).toBeGreaterThan(55 * 1000); // 55 seconds
				expect(scheduledDateDiff).toBeLessThan(60 * 1000); // 60 seconds

				expect(res!.__createdAt).not.toEqual(res!.__updatedAt);
				expect(res!.ttl).toBeGreaterThan((Date.now() + SUBTASK_TTL_IN_MS) / 1000);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					id: expect.stringMatching(`${task.id}#fork-id#DELAY-DEBOUNCE`),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: null,
					lastResponseStatus: 0,
					namespace: 'spec#FORK#SUBTASK',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: `${task.id}#fork-id`,
					parentNamespace: 'spec#FORK',
					pid: '',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});
		});
	});

	describe('registerTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'put');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should validate args', async () => {
			const invalidInput = {
				namespace: 'spec'
			};

			try {
				await hooks.registerTask(invalidInput as any);

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.put).not.toHaveBeenCalled();
				expect(err).toBeInstanceOf(Error);
			}
		});

		it('should validate noAfter > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.registerTask({
					namespace: 'spec',
					noAfter: `${currentYear}-01-01T00:00:00Z`,
					noBefore: `${currentYear + 1}-01-01T00:00:00-03:00`,
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.put).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noAfter cannot be in the past');
			}
		});

		it('should validate noBefore > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.registerTask({
					namespace: 'spec',
					noAfter: `${currentYear + 1}-01-01T00:00:00Z`, // 2026-01-01T00:00:00.000Z
					noBefore: `${currentYear}-01-01T00:00:00-03:00`, // 2025-01-01T03:00:00.000Z
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.put).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noBefore cannot be in the past');
			}
		});

		it('should validate noBefore > noAfter', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.registerTask({
					namespace: 'spec',
					noAfter: `${currentYear + 1}-01-01T00:00:00Z`, // 2026-01-01T00:00:00.000Z
					noBefore: `${currentYear + 1}-01-01T00:00:00-03:00`, // 2026-01-01T03:00:00.000Z
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.put).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noAfter must be after noBefore');
			}
		});

		it('should validate scheduledDate > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.registerTask({
					namespace: 'spec',
					scheduledDate: `${currentYear}-01-01T00:00:00-03:00`,
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.put).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('scheduledDate cannot be in the past');
			}
		});

		it('should create task', async () => {
			const res = await hooks.registerTask({
				namespace: 'spec',
				requestUrl: 'https://httpbin.org/anything'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: '',
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: '-',
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'PRIMARY',
				ttl: 0
			});
		});

		it('should create task by [eventPattern, noAfter, noBefore, scheduledDate]', async () => {
			const currentYear = new Date().getFullYear();
			const res = await hooks.registerTask({
				eventPattern: 'event-pattern',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				requestUrl: 'https://httpbin.org/anything',
				scheduledDate: `${currentYear + 1}-01-01T00:00:00-03:00`
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: 'event-pattern',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec',
				namespace__eventPattern: 'spec#event-pattern',
				noAfter: `${currentYear + 1}-01-01T03:00:00.000Z`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'PRIMARY',
				ttl: 0
			});
		});
	});

	describe('setTaskActive', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'deleteMany');
			vi.spyOn(hooks.db.tasks, 'query');
			vi.spyOn(hooks.db.tasks, 'transaction');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should disable tasks and forks and delete subtasks', async () => {
			// @ts-expect-error
			const forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				parentTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(task, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerScheduledSubTask(createTestSubTaskInput(forkTask)),
				// @ts-expect-error
				hooks.registerScheduledSubTask(
					createTestSubTaskInput(forkTask, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);

			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace'
				},
				attributeValues: {
					':id': task.id,
					':namespace': 'spec'
				},
				filterExpression: 'begins_with(#namespace, :namespace) AND begins_with(#id, :id)',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 6,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#fork-id#DELAY#[0-9]`),
						namespace: 'spec#FORK#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#fork-id#DELAY-DEBOUNCE`,
						namespace: 'spec#FORK#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					}
				]),
				lastEvaluatedKey: null
			});

			await hooks.setTaskActive({
				active: false,
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace'
				},
				attributeValues: {
					':id': task.id,
					':namespace': 'spec#FORK'
				},
				limit: Infinity,
				queryExpression: '#namespace = :namespace AND begins_with(#id, :id)',
				select: ['id', 'namespace']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Update: {
							ExpressionAttributeNames: { '#status': 'status' },
							ExpressionAttributeValues: { ':status': 'DISABLED' },
							Key: {
								id: task.id,
								namespace: 'spec'
							},
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status'
						}
					},
					{
						Update: {
							ExpressionAttributeNames: { '#status': 'status' },
							ExpressionAttributeValues: { ':status': 'DISABLED' },
							Key: {
								id: `${task.id}#fork-id`,
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status'
						}
					}
				]
			});

			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledTimes(2);
			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec#FORK#SUBTASK'
				},
				prefix: true
			});
			expect(hooks.db.tasks.deleteMany).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec#SUBTASK'
				},
				prefix: true
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace'
				},
				attributeValues: {
					':id': task.id,
					':namespace': 'spec'
				},
				filterExpression: 'begins_with(#namespace, :namespace) AND begins_with(#id, :id)',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 2,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'DISABLED'
					},
					{
						id: `${task.id}#fork-id`,
						namespace: 'spec#FORK',
						status: 'DISABLED'
					}
				]),
				lastEvaluatedKey: null
			});
		});

		it('should throw if task is not primary', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'FORK' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #type = :type'
			});

			try {
				await hooks.setTaskActive({
					active: false,
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must be a primary task');
			}
		});

		it('should throw if task is not active', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: { ':status': 'DISABLED' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #status = :status'
			});

			try {
				await hooks.setTaskActive({
					active: false,
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is already DISABLED');
			}
		});

		it('should throw if task is processing', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: { ':status': 'PROCESSING' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #status = :status'
			});

			try {
				await hooks.setTaskActive({
					active: false,
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is not in a valid state');
			}
		});
	});

	describe('setTaskError', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should set', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'EVENT',
				pid: 'test',
				task,
				error: new Error('test')
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstErrorDate': 'firstErrorDate',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastErrorExecutionType': 'lastErrorExecutionType',
					'#pid': 'pid',
					'#status': 'status',
					'#totalErrors': 'totalErrors'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':error': 'test',
					':executionType': 'EVENT',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#pid = :empty',
						'#firstErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', ')
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'EVENT',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set with task.concurrency = true', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#concurrency': 'concurrency' },
				attributeValues: { ':concurrency': true },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #concurrency = :concurrency'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'EVENT',
				pid: '',
				task,
				error: new Error('test')
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstErrorDate': 'firstErrorDate',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastErrorExecutionType': 'lastErrorExecutionType',
					'#pid': 'pid',
					'#status': 'status',
					'#totalErrors': 'totalErrors'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':error': 'test',
					':executionType': 'EVENT',
					':now': expect.any(String),
					':one': 1
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#pid = :empty',
						'#firstErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', ')
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: true,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'EVENT',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set task status = MAX-ERRORS-REACHED if max errors reached', async () => {
			hooks.maxErrors = 1;

			task = await hooks.db.tasks.update({
				attributeNames: {
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'EVENT',
				pid: 'test',
				task,
				error: new Error('test')
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstErrorDate': 'firstErrorDate',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastErrorExecutionType': 'lastErrorExecutionType',
					'#pid': 'pid',
					'#status': 'status',
					'#totalErrors': 'totalErrors'
				},
				attributeValues: {
					':empty': '',
					':error': 'test',
					':executionType': 'EVENT',
					':maxErrorsReached': 'MAX-ERRORS-REACHED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#pid = :empty',
						'#firstErrorDate = :now',
						'#status = :maxErrorsReached'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', ')
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'EVENT',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'MAX-ERRORS-REACHED',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'PRIMARY'
			});
		});
	});

	describe('setTaskLock', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should set task status = PROCESSING and set pid', async () => {
			// @ts-expect-error
			const res = await hooks.setTaskLock({
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#pid': 'pid',
					'#repeatMax': 'repeatMax',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#ts': '__ts'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':pid': 'test',
					':processing': 'PROCESSING',
					':ts': expect.any(Number),
					':zero': 0
				},
				// in case of other process already picked the task while it was being processed
				conditionExpression: [
					'#pid = :empty',
					'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
					'#status = :active',
					'#ts = :ts'
				].join(' AND '),
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			expect(res.status).toEqual('PROCESSING');
			expect(res.pid).toEqual('test');
		});
	});

	describe('setTaskSuccess', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should set', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskSuccess({
				executionType: 'SCHEDULED',
				log: {
					responseBody: 'test',
					responseHeaders: {},
					responseOk: true,
					responseStatus: 200
				},
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastExecutionType': 'lastExecutionType',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':executionType': 'SCHEDULED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING',
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 200,
					':scheduledDate': expect.any(String)
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#pid = :empty',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set with task.concurrency = true', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#concurrency': 'concurrency' },
				attributeValues: { ':concurrency': true },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #concurrency = :concurrency'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskSuccess({
				executionType: 'SCHEDULED',
				log: {
					responseBody: 'test',
					responseHeaders: {},
					responseOk: true,
					responseStatus: 200
				},
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastExecutionType': 'lastExecutionType',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':executionType': 'SCHEDULED',
					':now': expect.any(String),
					':one': 1,
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 200,
					':scheduledDate': expect.any(String)
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#pid = :empty',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: true,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set with response.ok = false', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskSuccess({
				executionType: 'SCHEDULED',
				log: {
					responseBody: 'test',
					responseHeaders: {},
					responseOk: false,
					responseStatus: 400
				},
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastExecutionType': 'lastExecutionType',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#totalSuccessfulOrFailed': 'totalFailedExecutions'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':executionType': 'SCHEDULED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING',
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 400,
					':scheduledDate': expect.any(String)
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#pid = :empty',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 400,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 1,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set with task.repeat.interval = 0', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#repeatInterval': 'repeatInterval',
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':interval': 0,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #repeatInterval = :interval, #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskSuccess({
				executionType: 'SCHEDULED',
				log: {
					responseBody: 'test',
					responseHeaders: {},
					responseOk: true,
					responseStatus: 200
				},
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastExecutionType': 'lastExecutionType',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':empty': '',
					':executionType': 'SCHEDULED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING',
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 200
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#pid = :empty',
						'#firstExecutionDate = :now',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(0);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should set with task.repeat.max = 1', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: {
					'#repeatMax': 'repeatMax',
					'#status': 'status',
					'#pid': 'pid'
				},
				attributeValues: {
					':max': 1,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #repeatMax = :max, #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskSuccess({
				executionType: 'SCHEDULED',
				log: {
					responseBody: 'test',
					responseHeaders: {},
					responseOk: true,
					responseStatus: 200
				},
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastExecutionType': 'lastExecutionType',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions',
					'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
				},
				attributeValues: {
					':empty': '',
					':executionType': 'SCHEDULED',
					':maxRepeatReached': 'MAX-REPEAT-REACHED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING',
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 200
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#pid = :empty',
						'#firstExecutionDate = :now',
						'#status = :maxRepeatReached'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(0);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				conditionFilter: null,
				description: '',
				eventDelayDebounce: false,
				eventDelayUnit: 'minutes',
				eventDelayValue: 0,
				eventPattern: '-',
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				namespace: 'spec',
				namespace__eventPattern: '-',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				pid: '',
				repeatInterval: 30,
				repeatMax: 1,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'MAX-REPEAT-REACHED',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		describe('executionType = EVENT', () => {
			it('should set', async () => {
				task = await hooks.db.tasks.update({
					attributeNames: {
						'#status': 'status',
						'#pid': 'pid'
					},
					attributeValues: {
						':pid': 'test',
						':processing': 'PROCESSING'
					},
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: 'SET #pid = :pid, #status = :processing'
				});
				vi.mocked(hooks.db.tasks.update).mockClear();

				// @ts-expect-error
				const res = await hooks.setTaskSuccess({
					executionType: 'EVENT',
					pid: 'test',
					task,
					log: {
						responseBody: 'test',
						responseHeaders: {},
						responseOk: true,
						responseStatus: 200
					}
				});

				expect(hooks.db.tasks.update).toHaveBeenCalledWith({
					attributeNames: {
						'#firstExecutionDate': 'firstExecutionDate',
						'#lastExecutionDate': 'lastExecutionDate',
						'#lastExecutionType': 'lastExecutionType',
						'#lastResponseBody': 'lastResponseBody',
						'#lastResponseHeaders': 'lastResponseHeaders',
						'#lastResponseStatus': 'lastResponseStatus',
						'#pid': 'pid',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions',
						'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':executionType': 'EVENT',
						':now': expect.any(String),
						':one': 1,
						':pid': 'test',
						':processing': 'PROCESSING',
						':responseBody': 'test',
						':responseHeaders': {},
						':responseStatus': 200,
						':scheduledDate': expect.any(String)
					},
					conditionExpression: '#status = :processing AND #pid = :pid',
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: [
						`SET ${[
							'#lastExecutionDate = :now',
							'#lastExecutionType = :executionType',
							'#lastResponseBody = :responseBody',
							'#lastResponseHeaders = :responseHeaders',
							'#lastResponseStatus = :responseStatus',
							'#pid = :empty',
							'#firstExecutionDate = :now',
							'#scheduledDate = :scheduledDate',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
					].join(' ')
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: expect.any(String),
					firstScheduledDate: expect.any(String),
					id: expect.any(String),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'EVENT',
					lastResponseBody: 'test',
					lastResponseHeaders: {},
					lastResponseStatus: 200,
					namespace: 'spec',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: '',
					parentNamespace: '',
					pid: '',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 1,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 1,
					ttl: 0,
					type: 'PRIMARY'
				});
			});

			it('should set with task.rescheduleOnEvent = false', async () => {
				task = await hooks.db.tasks.update({
					attributeNames: {
						'#rescheduleOnEvent': 'rescheduleOnEvent',
						'#status': 'status',
						'#pid': 'pid'
					},
					attributeValues: {
						':rescheduleOnEvent': false,
						':pid': 'test',
						':processing': 'PROCESSING'
					},
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: 'SET #pid = :pid, #rescheduleOnEvent = :rescheduleOnEvent, #status = :processing'
				});
				vi.mocked(hooks.db.tasks.update).mockClear();

				// @ts-expect-error
				const res = await hooks.setTaskSuccess({
					executionType: 'EVENT',
					pid: 'test',
					task,
					log: {
						responseBody: 'test',
						responseHeaders: {},
						responseOk: true,
						responseStatus: 200
					}
				});

				expect(hooks.db.tasks.update).toHaveBeenCalledWith({
					attributeNames: {
						'#firstExecutionDate': 'firstExecutionDate',
						'#lastExecutionDate': 'lastExecutionDate',
						'#lastExecutionType': 'lastExecutionType',
						'#lastResponseBody': 'lastResponseBody',
						'#lastResponseHeaders': 'lastResponseHeaders',
						'#lastResponseStatus': 'lastResponseStatus',
						'#pid': 'pid',
						'#status': 'status',
						'#totalExecutions': 'totalExecutions',
						'#totalSuccessfulOrFailed': 'totalSuccessfulExecutions'
					},
					attributeValues: {
						':active': 'ACTIVE',
						':empty': '',
						':executionType': 'EVENT',
						':now': expect.any(String),
						':one': 1,
						':pid': 'test',
						':processing': 'PROCESSING',
						':responseBody': 'test',
						':responseHeaders': {},
						':responseStatus': 200
					},
					conditionExpression: '#status = :processing AND #pid = :pid',
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: [
						`SET ${[
							'#lastExecutionDate = :now',
							'#lastExecutionType = :executionType',
							'#lastResponseBody = :responseBody',
							'#lastResponseHeaders = :responseHeaders',
							'#lastResponseStatus = :responseStatus',
							'#pid = :empty',
							'#firstExecutionDate = :now',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`
					].join(' ')
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(0);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__ts: expect.any(Number),
					__updatedAt: expect.any(String),
					concurrency: false,
					conditionFilter: null,
					description: '',
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 0,
					eventPattern: '-',
					firstErrorDate: '',
					firstExecutionDate: expect.any(String),
					firstScheduledDate: expect.any(String),
					id: expect.any(String),
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'EVENT',
					lastResponseBody: 'test',
					lastResponseHeaders: {},
					lastResponseStatus: 200,
					namespace: 'spec',
					namespace__eventPattern: '-',
					noAfter: '',
					noBefore: '',
					parentId: '',
					parentNamespace: '',
					pid: '',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: false,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 1,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 1,
					ttl: 0,
					type: 'PRIMARY'
				});
			});
		});
	});

	describe.todo('trigger', () => {
		beforeEach(async () => {
			await Promise.all(
				_.map(
					[
						createTestTask(0, {
							eventPattern: 'event-pattern-1',
							requestMethod: 'GET'
						}),
						createTestTask(0, {
							eventPattern: 'event-pattern-2',
							requestMethod: 'GET'
						}),
						createTestTask(1000)
					],
					task => {
						return hooks.registerTask(task);
					}
				)
			);

			vi.spyOn(hooks, 'callWebhook');
			// @ts-expect-error
			vi.spyOn(hooks, 'queryActiveTasks');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should works SCHEDULED', async () => {
			const res = await hooks.trigger();

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				delayDebounceId: '',
				executionType: 'SCHEDULED',
				tasks: expect.any(Array)
			});

			const { tasks } = vi.mocked(hooks.callWebhook).mock.calls[0][0];

			expect(
				_.every(tasks, task => {
					return (
						_.isEqual(task.requestBody, { a: 1 }) &&
						_.isEqual(task.requestHeaders, { a: '1' }) &&
						task.requestMethod === 'GET' &&
						task.requestUrl === 'https://httpbin.org/anything'
					);
				})
			).toBe(true);

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});
		});

		it('should works EVENT by eventPattern', async () => {
			const res = await hooks.trigger({
				eventPattern: 'event-pattern-',
				eventPatternPrefix: true,
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				eventPattern: 'event-pattern-',
				eventPatternPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				delayDebounceId: '',
				executionType: 'EVENT',
				tasks: expect.any(Array)
			});

			const { tasks } = vi.mocked(hooks.callWebhook).mock.calls[0][0];

			expect(
				_.every(tasks, task => {
					return (
						_.isEqual(task.requestBody, { a: 1 }) &&
						_.isEqual(task.requestHeaders, { a: '1' }) &&
						task.requestMethod === 'GET' &&
						task.requestUrl === 'https://httpbin.org/anything'
					);
				})
			).toBe(true);

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});
		});

		it('should works EVENT by eventPattern with [body, headers, method, url]', async () => {
			const res = await hooks.trigger({
				eventPattern: 'event-pattern-',
				eventPatternPrefix: true,
				namespace: 'spec',
				requestBody: { a: 2, b: 3 },
				requestHeaders: { a: '2', b: '3' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything-2'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				eventPattern: 'event-pattern-',
				eventPatternPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				delayDebounceId: '',
				executionType: 'EVENT',
				tasks: expect.any(Array)
			});

			const { tasks } = vi.mocked(hooks.callWebhook).mock.calls[0][0];

			expect(
				_.every(tasks, task => {
					return (
						_.isEqual(task.requestBody, { a: 2, b: 3 }) &&
						_.isEqual(task.requestHeaders, { a: '2', b: '3' }) &&
						task.requestMethod === 'POST' &&
						task.requestUrl === 'https://httpbin.org/anything-2'
					);
				})
			).toBe(true);

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});
		});

		it('should works EVENT with conditionFilter', async () => {
			const res = await hooks.trigger({
				conditionData: {
					a: 'test'
				},
				conditionFilter: {
					type: 'STRING',
					path: ['key'],
					operator: 'EQUALS',
					value: 'test-1'
				},
				eventPattern: 'event-pattern-',
				eventPatternPrefix: true,
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).not.toHaveBeenCalled();
			expect(hooks.callWebhook).not.toHaveBeenCalled();
			expect(res).toEqual({
				processed: 0,
				errors: 0
			});
		});

		it('should works EVENT by id', async () => {
			const res = await hooks.trigger({
				id: 'id-prefix-',
				idPrefix: true,
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				id: 'id-prefix-',
				idPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				delayDebounceId: '',
				executionType: 'EVENT',
				tasks: expect.any(Array)
			});

			const { tasks } = vi.mocked(hooks.callWebhook).mock.calls[0][0];

			expect(
				_.every(tasks, task => {
					return (
						_.isEqual(task.requestBody, { a: 1 }) &&
						_.isEqual(task.requestHeaders, { a: '1' }) &&
						task.requestMethod === 'GET' &&
						task.requestUrl === 'https://httpbin.org/anything'
					);
				})
			).toBe(true);

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});
		});

		it('should works EVENT by id with [body, delayDebounceId, headers, method, url]', async () => {
			const res = await hooks.trigger({
				delayDebounceId: 'debounce-id',
				id: 'id-prefix-',
				idPrefix: true,
				namespace: 'spec',
				requestBody: { a: 2, b: 3 },
				requestHeaders: { a: '2', b: '3' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything-2'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				id: 'id-prefix-',
				idPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				delayDebounceId: 'debounce-id',
				executionType: 'EVENT',
				tasks: expect.any(Array)
			});

			const { tasks } = vi.mocked(hooks.callWebhook).mock.calls[0][0];

			expect(
				_.every(tasks, task => {
					return (
						_.isEqual(task.requestBody, { a: 2, b: 3 }) &&
						_.isEqual(task.requestHeaders, { a: '2', b: '3' }) &&
						task.requestMethod === 'POST' &&
						task.requestUrl === 'https://httpbin.org/anything-2'
					);
				})
			).toBe(true);

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});
		});
	});

	describe('uuid', () => {
		it('should generate a UUID', () => {
			// @ts-expect-error
			const uuid = hooks.uuid();

			expect(uuid).toMatch(/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});
	});
});
