import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import FilterCriteria from 'use-filter-criteria';
import z from 'zod';

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

			// @ts-expect-error
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
						// @ts-expect-error
						id: await hooks.uuidFromString('inexistent-id')
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
		let forkTask: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});

			// @ts-expect-error
			vi.spyOn(hooks, 'checkExecuteTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'getSubTaskParent');
			// @ts-expect-error
			vi.spyOn(hooks, 'getTaskInternal');
			// @ts-expect-error
			vi.spyOn(hooks, 'registerForkTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'registerDelayedSubTask');
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
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					forkId: '',
					ruleId: '',
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
						responseBody: expect.any(String),
						responseHeaders: expect.any(Object),
						responseOk: true,
						responseStatus: 200
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

		it('should works with fork', async () => {
			const res = await hooks.callWebhook({
				conditionData: null,
				date: new Date(),
				eventDelayDebounce: null,
				eventDelayUnit: null,
				eventDelayValue: null,
				executionType: 'EVENT',
				forkId: null,
				forkOnly: false,
				keys: [{ id: forkTask.id, namespace: 'spec#FORK' }],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: forkTask.id, namespace: 'spec#FORK' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
				date: expect.any(Date),
				task: forkTask
			});
			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				pid: expect.any(String),
				task: forkTask
			});
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				metadata: {
					executionType: 'EVENT',
					forkId: 'fork-id',
					ruleId: '',
					taskId: task.id,
					taskType: 'FORK'
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
						responseBody: expect.any(String),
						responseHeaders: expect.any(Object),
						responseOk: true,
						responseStatus: 200
					}),
					pid: expect.any(String),
					task: {
						...forkTask,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						pid: expect.any(String),
						status: 'PROCESSING'
					}
				})
			);

			expect(res).toEqual([
				{
					...forkTask,
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
			task = await hooks.registerTask(
				createTestTask(0, {
					conditionFilter: FilterCriteria.criteria({
						matchValue: 'value',
						operator: 'EQUALS',
						type: 'STRING',
						valuePath: ['key']
					})
				})
			);

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
				requestUrl: 'https://httpbin.org/custom',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					forkId: '',
					ruleId: '',
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
						responseBody: expect.any(String),
						responseHeaders: expect.any(Object),
						responseOk: true,
						responseStatus: 200
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
			task = await hooks.registerTask(
				createTestTask(0, {
					conditionFilter: FilterCriteria.criteria({
						matchValue: 'value',
						operator: 'EQUALS',
						type: 'STRING',
						valuePath: ['key']
					})
				})
			);

			const res = await Promise.all([
				hooks.callWebhook({
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				}),
				hooks.callWebhook({
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				})
			]);

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskLock).not.toHaveBeenCalled();
			expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

			expect(res[0]).toEqual([]);
			expect(res[1]).toEqual([]);
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
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					forkId: '',
					ruleId: '',
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
					responseBody: expect.any(String),
					responseHeaders: expect.any(Object),
					responseOk: true,
					responseStatus: 200
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
				keys: [
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('inexistent-id'),
						namespace: 'spec'
					}
				],
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({
				// @ts-expect-error
				id: await hooks.uuidFromString('inexistent-id'),
				namespace: 'spec'
			});
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
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
				requestUrl: 'https://httpbin.org/anything',
				ruleId: null
			});

			// @ts-expect-error
			expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
			// @ts-expect-error
			expect(hooks.registerForkTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					forkId: '',
					ruleId: '',
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					primaryTask: task
				});

				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: {
						...forkTask,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String)
					}
				});

				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: {
						...forkTask,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String)
					}
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						forkId: 'fork-id',
						ruleId: '',
						taskId: task.id,
						taskType: 'FORK'
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
							responseBody: expect.any(String),
							responseHeaders: expect.any(Object),
							responseOk: true,
							responseStatus: 200
						}),
						pid: expect.any(String),
						task: {
							...forkTask,
							__createdAt: expect.any(String),
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pid: expect.any(String),
							status: 'PROCESSING'
						}
					})
				);

				expect(res).toEqual([
					{
						...forkTask,
						__createdAt: expect.any(String),
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					primaryTask: task
				});

				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					requestUrl: 'https://httpbin.org/custom',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).toHaveBeenCalledWith({
					forkId: 'fork-id',
					primaryTask: task
				});

				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: {
						...forkTask,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String)
					}
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: {
						...forkTask,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String)
					}
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						forkId: 'fork-id',
						ruleId: '',
						taskId: task.id,
						taskType: 'FORK'
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
							responseBody: expect.any(String),
							responseHeaders: expect.any(Object),
							responseOk: true,
							responseStatus: 200
						}),
						pid: expect.any(String),
						task: {
							...forkTask,
							__createdAt: expect.any(String),
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
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						firstExecutionDate: expect.any(String),
						forkId: 'fork-id',
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id'),
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'EVENT',
						lastResponseBody: expect.any(String),
						lastResponseHeaders: expect.any(Object),
						lastResponseStatus: 200,
						namespace: 'spec#FORK',
						primaryId: task.id,
						primaryNamespace: 'spec',
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

				// @ts-expect-error
				forkTask = await hooks.registerForkTask(
					{
						forkId: 'fork-id',
						primaryTask: task
					},
					true
				);

				// @ts-expect-error
				vi.mocked(hooks.registerForkTask).mockClear();
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).toHaveBeenCalledWith({
					...task,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
						forkId: '',
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						primaryId: task.id,
						primaryNamespace: 'spec',
						repeatInterval: 0,
						scheduledDate: expect.any(String),
						ttl: expect.any(Number),
						type: 'SUBTASK'
					}
				]);
			});

			it('should register fork', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [{ id: forkTask.id, namespace: 'spec#FORK' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: forkTask.id, namespace: 'spec#FORK' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).toHaveBeenCalledWith({
					...forkTask,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res).toEqual([
					{
						...forkTask,
						__createdAt: expect.any(String),
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						eventDelayValue: 0,
						firstScheduledDate: expect.any(String),
						id: expect.stringMatching(`${forkTask.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
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
					requestUrl: 'https://httpbin.org/custom',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).toHaveBeenCalledWith({
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
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
						forkId: '',
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						primaryId: task.id,
						primaryNamespace: 'spec',
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
			let subTasks: Hooks.Task[];

			beforeEach(async () => {
				subTasks = await Promise.all([
					// @ts-expect-error
					hooks.registerDelayedSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					}),
					// @ts-expect-error
					hooks.registerDelayedSubTask({
						...forkTask,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				]);

				// @ts-expect-error
				vi.mocked(hooks.registerDelayedSubTask).mockClear();
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
					keys: [{ id: subTasks[0].id, namespace: 'spec#SUBTASK' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: subTasks[0].id, namespace: 'spec#SUBTASK' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
					keys: [{ id: subTasks[0].id, namespace: 'spec#SUBTASK' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: subTasks[0].id, namespace: 'spec#SUBTASK' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).toHaveBeenCalledWith(subTasks[0]);
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
						forkId: '',
						ruleId: '',
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
							responseBody: expect.any(String),
							responseHeaders: expect.any(Object),
							responseOk: true,
							responseStatus: 200
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

			it('should execute forked', async () => {
				const res = await hooks.callWebhook({
					conditionData: null,
					date: new Date(),
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: [{ id: subTasks[1].id, namespace: 'spec#SUBTASK' }],
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: subTasks[1].id, namespace: 'spec#SUBTASK' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).toHaveBeenCalledWith(subTasks[1]);
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: forkTask
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: forkTask
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'SCHEDULED',
						forkId: 'fork-id',
						ruleId: '',
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
							responseBody: expect.any(String),
							responseHeaders: expect.any(Object),
							responseOk: true,
							responseStatus: 200
						}),
						pid: expect.any(String),
						task: {
							...forkTask,
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pid: expect.any(String),
							status: 'PROCESSING'
						}
					})
				);

				expect(res).toEqual([
					{
						...forkTask,
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

		describe('execute rules', () => {
			let rule1: Mock;
			let rule2: Mock;

			beforeEach(() => {
				// @ts-expect-error
				vi.mocked(global.fetch).mockResolvedValueOnce({
					ok: false,
					status: 400
				});

				rule1 = vi.fn(async () => {
					return _.times(5, i => {
						return {
							requestBody: { a: i },
							requestHeaders: { a: `a-${i}` },
							requestMethod: 'GET',
							requestUrl: 'https://httpbin.org/rule'
						};
					});
				});

				rule2 = vi.fn(async () => {
					return _.times(5, i => {
						return {
							requestBody: null,
							requestHeaders: null,
							requestMethod: null,
							requestUrl: null
						};
					});
				});

				hooks.registerRule('rule-id-1', rule1);
				hooks.registerRule('rule-id-2', rule2);
			});

			it('should execute', async () => {
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: 'rule-id-1'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
				expect(hooks.webhooks.trigger).toHaveBeenCalledTimes(5);
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						forkId: '',
						ruleId: 'rule-id-1',
						taskId: task.id,
						taskType: 'PRIMARY'
					},
					namespace: 'spec',
					requestBody: { a: 4 },
					requestHeaders: { a: 'a-4' },
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/rule',
					retryLimit: 3
				});

				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
					expect.objectContaining({
						executionType: 'EVENT',
						log: expect.objectContaining({
							responseBody: JSON.stringify({
								totalExecutions: 5,
								totalFailedExecutions: 1,
								totalSuccessfulExecutions: 4
							}),
							responseHeaders: {},
							responseOk: true,
							responseStatus: 200
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

			it('should execute with task.ruleId', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#ruleId': 'ruleId' },
					attributeValues: { ':ruleId': 'rule-id-2' },
					filter: {
						item: { id: task.id, namespace: 'spec' }
					},
					updateExpression: 'SET #ruleId = :ruleId'
				});

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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: null
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.checkExecuteTask).toHaveBeenCalledWith({
					date: expect.any(Date),
					task: {
						...task,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						ruleId: 'rule-id-2'
					}
				});
				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					pid: expect.any(String),
					task: {
						...task,
						__ts: expect.any(Number),
						__updatedAt: expect.any(String),
						ruleId: 'rule-id-2'
					}
				});
				expect(hooks.webhooks.trigger).toHaveBeenCalledTimes(5);
				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					metadata: {
						executionType: 'EVENT',
						forkId: '',
						ruleId: 'rule-id-2',
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
							responseBody: JSON.stringify({
								totalExecutions: 5,
								totalFailedExecutions: 1,
								totalSuccessfulExecutions: 4
							}),
							responseHeaders: {},
							responseOk: true,
							responseStatus: 200
						}),
						pid: expect.any(String),
						task: {
							...task,
							__ts: expect.any(Number),
							__updatedAt: expect.any(String),
							pid: expect.any(String),
							ruleId: 'rule-id-2',
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
						ruleId: 'rule-id-2',
						scheduledDate: expect.any(String),
						totalExecutions: 1,
						totalSuccessfulExecutions: 1
					}
				]);
			});

			it('should handle inexistent rule', async () => {
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
					requestUrl: 'https://httpbin.org/anything',
					ruleId: 'inexistent-rule-id'
				});

				// @ts-expect-error
				expect(hooks.getTaskInternal).toHaveBeenCalledWith({ id: task.id, namespace: 'spec' });
				// @ts-expect-error
				expect(hooks.registerForkTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.registerDelayedSubTask).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.getSubTaskParent).not.toHaveBeenCalled();
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
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();
				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith(
					expect.objectContaining({
						executionType: 'EVENT',
						log: expect.objectContaining({
							responseBody: JSON.stringify({
								totalExecutions: 0,
								totalFailedExecutions: 0,
								totalSuccessfulExecutions: 0
							}),
							responseHeaders: {},
							responseOk: true,
							responseStatus: 200
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

			const remaining = await hooks.db.tasks.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('countSubTasks', () => {
		let task: Hooks.Task;
		let forkTask: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask, { eventDelayDebounce: true }))
			]);
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should count subTasks', async () => {
			// @ts-expect-error
			const res = await hooks.countSubTasks(task);

			expect(res).toEqual(2);
		});

		it('should count forked subTasks', async () => {
			// @ts-expect-error
			const res = await hooks.countSubTasks(forkTask);

			expect(res).toEqual(2);
		});
	});

	describe('debugCondition', () => {
		let task: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask(
				createTestTask(0, {
					conditionFilter: FilterCriteria.criteria({
						matchValue: 'value',
						operator: 'EQUALS',
						type: 'STRING',
						valuePath: ['key']
					})
				})
			);
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should debug condition', async () => {
			const res = await hooks.debugCondition({
				conditionData: { key: 'value' },
				id: task.id,
				namespace: 'spec'
			});

			expect(res).toEqual({
				matchValue: 'value',
				passed: true,
				reason: 'STRING criteria "EQUALS" check PASSED',
				value: 'value'
			});
		});

		it('should debug condition with no conditionData', async () => {
			const res = await hooks.debugCondition({
				conditionData: null,
				id: task.id,
				namespace: 'spec'
			});

			expect(res).toEqual({
				matchValue: 'value',
				passed: false,
				reason: 'STRING criteria "EQUALS" check FAILED',
				value: ''
			});
		});

		it('should throw if task not found', async () => {
			try {
				await hooks.debugCondition({
					conditionData: { key: 'value' },
					// @ts-expect-error
					id: await hooks.uuidFromString('inexistent-id'),
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task not found');
			}
		});

		it('should throw if task has no condition filter', async () => {
			const task = await hooks.registerTask(createTestTask());

			try {
				await hooks.debugCondition({
					conditionData: { key: 'value' },
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task has no condition filter');
			}
		});
	});

	describe('deleteTask', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'query');
			vi.spyOn(hooks.db.tasks, 'transaction');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should throw if task is not found', async () => {
			try {
				await hooks.deleteTask({
					// @ts-expect-error
					id: await hooks.uuidFromString('inexistent-id'),
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task not found');
			}
		});

		it('should delete', async () => {
			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2, { eventDelayDebounce: true }))
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace']
			});

			expect(retrieved).toEqual({
				count: 9,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					}
				]),
				lastEvaluatedKey: null
			});

			await hooks.deleteTask({
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledOnce();
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					primaryId: task.id,
					primaryNamespace: 'spec'
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: expect.arrayContaining([
					{
						Delete: {
							Key: { id: task.id, namespace: 'spec' },
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								id: `${task.id}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-1'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-2'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					}
				])
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace']
			});

			expect(retrieved).toEqual({
				count: 0,
				items: [],
				lastEvaluatedKey: null
			});
		});

		it('should delete by fork', async () => {
			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2, { eventDelayDebounce: true }))
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace']
			});

			expect(retrieved).toEqual({
				count: 9,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					}
				]),
				lastEvaluatedKey: null
			});

			await hooks.deleteTask({
				fork: true,
				id: 'fork-id-1',
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledTimes(2);
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id-1'),
					namespace: 'spec#FORK'
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id-1'),
					namespace: 'spec#SUBTASK'
				},
				limit: Infinity,
				prefix: true,
				select: ['id', 'namespace']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: expect.arrayContaining([
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-1'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					}
				])
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace']
			});

			expect(retrieved).toEqual({
				count: 6,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK'
					}
				]),
				lastEvaluatedKey: null
			});
		});

		it('should throw if task is not primary or fork', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'SUBTASK' },
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
				expect(err.message).toEqual('Task must be a primary or fork task');
			}
		});
	});

	describe('executeRule', () => {
		let rule: Mock;
		let task: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
		});

		beforeEach(() => {
			rule = vi.fn(async () => {
				return _.times(5, i => {
					return {
						requestBody: { a: i },
						requestHeaders: null,
						requestMethod: null,
						requestUrl: null
					};
				});
			});

			hooks.registerRule('rule-id', rule);
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should execute rule', async () => {
			// @ts-expect-error
			const res = await hooks.executeRule('rule-id', task);

			expect(rule).toHaveBeenCalledWith({ task });
			expect(res).toEqual(
				_.times(5, i => {
					return {
						requestBody: { a: i },
						requestHeaders: null,
						requestMethod: null,
						requestUrl: null
					};
				})
			);
		});

		it('should handle inexistent rule', async () => {
			// @ts-expect-error
			const res = await hooks.executeRule('inexistent-rule-id', task);

			expect(rule).not.toHaveBeenCalled();
			expect(res).toEqual([]);
		});
	});

	describe('fetchLogs', () => {
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

	describe('fetchTasks', () => {
		let tasks: Hooks.Task[];
		let forkTask: Hooks.Task;
		let subTasks: Hooks.Task[];

		const pick = (tasks: Partial<Hooks.Task>[]) => {
			return _.map(tasks, task => {
				return _.pick(task, ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title']);
			});
		};

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

			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: tasks[0]
			});

			subTasks = await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(tasks[0])),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(tasks[0], { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask, { eventDelayDebounce: true }))
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
				select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: expect.arrayContaining(pick(tasks)),
				lastEvaluatedKey: null
			});
		});

		it('should fetch forks by [namespace]', async () => {
			const res = await hooks.fetchTasks({
				fork: true,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#FORK' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: expect.arrayContaining(pick([forkTask])),
				lastEvaluatedKey: null
			});
		});

		it('should fetch subtasks by [namespace]', async () => {
			const res = await hooks.fetchTasks({
				namespace: 'spec',
				subTask: true
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#SUBTASK' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
				startKey: null
			});

			expect(res).toEqual({
				count: 4,
				items: expect.arrayContaining(pick(subTasks)),
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
				select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: expect.arrayContaining(pick(tasks)),
				lastEvaluatedKey: null
			});
		});

		describe('query by [id]', () => {
			it('should fetch by [namespace, id]', async () => {
				const res = await hooks.fetchTasks({
					id: tasks[0].id,
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace'
					},
					attributeValues: {
						':id': tasks[0].id,
						':namespace': 'spec'
					},
					filterExpression: '',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #id = :id',
					scanIndexForward: true,
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
					startKey: null
				});

				expect(res).toEqual({
					count: 1,
					items: expect.arrayContaining(pick([tasks[0]])),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id (prefix)]', async () => {
				const res = await hooks.fetchTasks({
					id: tasks[0].id,
					idPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace'
					},
					attributeValues: {
						':id': tasks[0].id,
						':namespace': 'spec'
					},
					filterExpression: '',
					limit: 100,
					queryExpression: '#namespace = :namespace AND begins_with(#id, :id)',
					scanIndexForward: true,
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
					startKey: null
				});

				expect(res).toEqual({
					count: 1,
					items: expect.arrayContaining(pick([tasks[0]])),
					lastEvaluatedKey: null
				});
			});

			it('should fetch forks by [namespace, id]', async () => {
				const res = await hooks.fetchTasks({
					fork: true,
					id: 'fork-id',
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#id': 'id',
						'#namespace': 'namespace'
					},
					attributeValues: {
						':id': forkTask.id,
						':namespace': 'spec#FORK'
					},
					filterExpression: '',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #id = :id',
					scanIndexForward: true,
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
					startKey: null
				});

				expect(res).toEqual({
					count: 1,
					items: expect.arrayContaining(pick([forkTask])),
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
					queryExpression: '#namespace = :namespace AND #id = :id',
					scanIndexForward: true,
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
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
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
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
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
					startKey: null
				});

				expect(res).toEqual({
					count: 3,
					items: expect.arrayContaining(pick(tasks)),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, eventPattern, scheduledDate, status]', async () => {
				const res = await hooks.fetchTasks({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#eventPattern': 'eventPattern',
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':eventPattern': 'event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':namespace': 'spec',
						':status': 'ACTIVE',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression: '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate AND #status = :status',
					index: 'namespace-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #eventPattern = :eventPattern',
					scanIndexForward: true,
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
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
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
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
					select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
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

	describe('getSubTaskParent', () => {
		let task: Hooks.Task;
		let forkTask: Hooks.Task;
		let subTasks: Hooks.Task[];

		beforeAll(async () => {
			task = await hooks.registerTask({
				namespace: 'spec',
				requestUrl: 'https://httpbin.org/anything'
			});

			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});

			subTasks = await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask, { eventDelayDebounce: true }))
			]);
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should return parent task', async () => {
			const res = await Promise.all([
				// @ts-expect-error
				hooks.getSubTaskParent(subTasks[0]),
				// @ts-expect-error
				hooks.getSubTaskParent(subTasks[1])
			]);

			expect(res[0]).toEqual(task);
			expect(res[1]).toEqual(task);
		});

		it('should return parent fork task', async () => {
			const res = await Promise.all([
				// @ts-expect-error
				hooks.getSubTaskParent(subTasks[2]),
				// @ts-expect-error
				hooks.getSubTaskParent(subTasks[3])
			]);

			expect(res[0]).toEqual(forkTask);
			expect(res[1]).toEqual(forkTask);
		});

		it('should throw if task is not a subtask', async () => {
			try {
				// @ts-expect-error
				await hooks.getSubTaskParent(task);

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must be a subtask');
			}
		});

		it('should throw if task is not found', async () => {
			try {
				// @ts-expect-error
				await hooks.getSubTaskParent({
					...subTasks[0],
					// @ts-expect-error
					id: await hooks.uuidFromString('inexistent-id')
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Parent task not found');
			}
		});

		it('should throw if task is not a primary or fork task', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'SUBTASK' },
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #type = :type'
			});

			try {
				// @ts-expect-error
				await hooks.getSubTaskParent(subTasks[0]);

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

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});
		});

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'get');
		});

		afterAll(async () => {
			await hooks.clearTasks('spec');
		});

		it('should throw if task is not found', async () => {
			try {
				await hooks.getTask({
					// @ts-expect-error
					id: await hooks.uuidFromString('inexistent-id'),
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task not found');
			}
		});

		it('should get', async () => {
			const res = await hooks.getTask({
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: task.id,
					namespace: 'spec'
				}
			});

			expect(res).toEqual(task);
		});

		it('should get forked task', async () => {
			const res = await hooks.getTask({
				fork: true,
				id: 'fork-id',
				namespace: 'spec'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					id: forkTask.id,
					namespace: 'spec#FORK'
				}
			});

			expect(res).toEqual(forkTask);
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

	describe('registerDelayedSubTask', () => {
		let task: Hooks.Task;
		let forkTask: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask({
				namespace: 'spec',
				requestUrl: 'https://httpbin.org/anything',
				ruleId: 'rule-id'
			});

			// @ts-expect-error
			forkTask = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
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
				const res = await hooks.registerDelayedSubTask({
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
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: '',
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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
				const res = await hooks.registerDelayedSubTask({
					...forkTask,
					eventDelayDebounce: false,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						forkId: 'fork-id',
						id: expect.stringMatching(`${forkTask.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: 'fork-id',
					id: expect.stringMatching(`${forkTask.id}#DELAY#[0-9]+`),
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: expect.any(Number),
					type: 'SUBTASK'
				});
			});

			it('should throw if task has reached the repeat max by totalExecutions', async () => {
				try {
					// @ts-expect-error
					await hooks.registerDelayedSubTask({
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
					expect(err.message).toEqual('Primary task has reached the repeat max by totalExecutions');
				}
			});

			it('should throw if task is disabled', async () => {
				try {
					// @ts-expect-error
					await hooks.registerDelayedSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						status: 'DISABLED'
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Primary task is disabled');
				}
			});

			it('should throw if task is not a primary or fork task', async () => {
				try {
					// @ts-expect-error
					await hooks.registerDelayedSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						type: 'SUBTASK'
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Task must be a primary or fork task');
				}
			});

			it('should throw if task has reached the repeat max by countSubTasks', async () => {
				try {
					// @ts-expect-error
					await hooks.registerDelayedSubTask({
						...task,
						eventDelayDebounce: false,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1,
						repeatMax: 1
					});

					throw new Error('expected to throw');
				} catch (err) {
					expect(err).toBeInstanceOf(TaskException);
					expect(err.message).toEqual('Primary task has reached the repeat max by totalSubTasks');
				}
			});
		});

		describe('debounce delay subTask', () => {
			it('should create', async () => {
				// @ts-expect-error
				const res = await hooks.registerDelayedSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: '',
					id: `${task.id}#DELAY-DEBOUNCE`,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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
				const res = await hooks.registerDelayedSubTask({
					...forkTask,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						forkId: 'fork-id',
						id: `${forkTask.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: 'fork-id',
					id: `${forkTask.id}#DELAY-DEBOUNCE`,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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
					hooks.registerDelayedSubTask({
						...task,
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					}),
					// @ts-expect-error
					hooks.registerDelayedSubTask({
						...forkTask,
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				]);

				vi.mocked(hooks.db.tasks.put).mockClear();
			});

			it('should update', async () => {
				// @ts-expect-error
				const res = await hooks.registerDelayedSubTask({
					...task,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: '',
					id: `${task.id}#DELAY-DEBOUNCE`,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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
				const res = await hooks.registerDelayedSubTask({
					...forkTask,
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 1
				});

				expect(hooks.db.tasks.put).toHaveBeenCalledWith(
					expect.objectContaining({
						eventPattern: '-',
						forkId: 'fork-id',
						id: `${forkTask.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						namespace__eventPattern: '-',
						primaryId: task.id,
						primaryNamespace: 'spec',
						ruleId: 'rule-id',
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
					forkId: 'fork-id',
					id: `${forkTask.id}#DELAY-DEBOUNCE`,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 0,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'GET',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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

	describe('registerForkTask', () => {
		let currentYear = new Date().getFullYear();
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask({
				conditionFilter: FilterCriteria.criteria({
					matchValue: 'value',
					operator: 'EQUALS',
					type: 'STRING',
					valuePath: ['key']
				}),
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
				ruleId: 'rule-id',
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
				primaryTask: task
			});

			expect(hooks.db.tasks.put).toHaveBeenCalledWith(
				{
					__createdAt: '',
					__ts: 0,
					__updatedAt: '',
					concurrency: false,
					conditionFilter: FilterCriteria.criteria({
						defaultValue: '',
						matchValue: 'value',
						normalize: true,
						operator: 'EQUALS',
						type: 'STRING',
						valuePath: ['key']
					}),
					description: 'description',
					eventDelayDebounce: true,
					eventDelayUnit: 'minutes',
					eventDelayValue: 30,
					eventPattern: 'event-pattern',
					firstErrorDate: '',
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					forkId: 'fork-id',
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id'),
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 1,
					repeatMax: 1,
					repeatUnit: 'hours',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: 'rule-id',
					scheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
					status: 'ACTIVE',
					title: '',
					totalErrors: 0,
					totalExecutions: 0,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 0,
					ttl: 0,
					type: 'FORK'
				},
				{
					overwrite: false
				}
			);

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				concurrency: false,
				description: 'description',
				conditionFilter: FilterCriteria.criteria({
					defaultValue: '',
					matchValue: 'value',
					normalize: true,
					operator: 'EQUALS',
					type: 'STRING',
					valuePath: ['key']
				}),
				eventDelayDebounce: true,
				eventDelayUnit: 'minutes',
				eventDelayValue: 30,
				eventPattern: 'event-pattern',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
				forkId: 'fork-id',
				// @ts-expect-error
				id: await hooks.uuidFromString('fork-id'),
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 1,
				repeatMax: 1,
				repeatUnit: 'hours',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: 'rule-id',
				scheduledDate: `${currentYear + 1}-01-12T00:00:00.000Z`,
				status: 'ACTIVE',
				title: '',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'FORK'
			});
		});

		it('should overwrite fork if overwrite = true', async () => {
			// @ts-expect-error
			const res = await hooks.registerForkTask(
				{
					forkId: 'fork-id',
					primaryTask: task
				},
				true
			);

			expect(hooks.db.tasks.put).toHaveBeenCalledWith(expect.any(Object), {
				overwrite: true
			});

			// @ts-expect-error
			expect(res.id).toEqual(await hooks.uuidFromString('fork-id'));
		});

		it('should return fork task if it already exists', async () => {
			// @ts-expect-error
			const res1 = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});

			// @ts-expect-error
			const res2 = await hooks.registerForkTask({
				forkId: 'fork-id',
				primaryTask: task
			});

			expect(hooks.db.tasks.put).toHaveBeenCalledOnce();
			expect(res2).toEqual(res1);
		});

		it('should throw if task is disabled', async () => {
			try {
				// @ts-expect-error
				await hooks.registerForkTask({
					forkId: 'fork-id',
					primaryTask: {
						...task,
						status: 'DISABLED'
					}
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Primary task is disabled');
			}
		});

		it('should throw if task is not a primary task', async () => {
			try {
				// @ts-expect-error
				await hooks.registerForkTask({
					forkId: 'fork-id',
					primaryTask: {
						...task,
						type: 'FORK'
					}
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task must be a primary task');
			}
		});
	});

	describe('registerRule', () => {
		it('should create rule', () => {
			const fn = vi.fn();
			hooks.registerRule('rule-id', fn);

			expect(hooks.rules.get('rule-id')).toEqual(fn);
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
				forkId: '',
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
				pid: '',
				primaryId: expect.any(String),
				primaryNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: '-',
				status: 'ACTIVE',
				title: '',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'PRIMARY',
				ttl: 0
			});
		});

		it('should create task with id', async () => {
			const res = await hooks.registerTask({
				id: 'abc#12345678-1234-1234-1234-123456789012',
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
				forkId: '',
				id: 'abc#12345678-1234-1234-1234-123456789012',
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
				pid: '',
				primaryId: expect.any(String),
				primaryNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: '-',
				status: 'ACTIVE',
				title: '',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'PRIMARY',
				ttl: 0
			});
		});

		it('should create task by [eventPattern, noAfter, noBefore, ruleId, scheduledDate]', async () => {
			const currentYear = new Date().getFullYear();
			const res = await hooks.registerTask({
				eventPattern: 'event-pattern',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				requestUrl: 'https://httpbin.org/anything',
				ruleId: 'rule-id',
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
				forkId: '',
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
				pid: '',
				primaryId: expect.any(String),
				primaryNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: 'rule-id',
				scheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				status: 'ACTIVE',
				title: '',
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

		it('should throw if task is not found', async () => {
			try {
				await hooks.setTaskActive({
					active: false,
					// @ts-expect-error
					id: await hooks.uuidFromString('inexistent-id'),
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task not found');
			}
		});

		it('should disable', async () => {
			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(task, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(forkTask1, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(forkTask2, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 9,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					}
				]),
				lastEvaluatedKey: null
			});

			const res = await hooks.setTaskActive({
				active: false,
				id: task.id,
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledOnce();
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					primaryId: task.id,
					primaryNamespace: 'spec'
				},
				limit: Infinity,
				select: ['id', 'namespace', 'type']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: expect.arrayContaining([
					{
						Update: {
							ExpressionAttributeNames: {
								'#status': 'status',
								'#ts': '__ts',
								'#updatedAt': '__updatedAt'
							},
							ExpressionAttributeValues: {
								':status': 'DISABLED',
								':ts': expect.any(Number),
								':updatedAt': expect.any(String)
							},
							Key: { id: task.id, namespace: 'spec' },
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status, #ts = :ts, #updatedAt = :updatedAt'
						}
					},
					{
						Delete: {
							Key: {
								id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								id: `${task.id}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Update: {
							ExpressionAttributeNames: {
								'#status': 'status',
								'#ts': '__ts',
								'#updatedAt': '__updatedAt'
							},
							ExpressionAttributeValues: {
								':status': 'DISABLED',
								':ts': expect.any(Number),
								':updatedAt': expect.any(String)
							},
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-1'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status, #ts = :ts, #updatedAt = :updatedAt'
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Update: {
							ExpressionAttributeNames: {
								'#status': 'status',
								'#ts': '__ts',
								'#updatedAt': '__updatedAt'
							},
							ExpressionAttributeValues: {
								':status': 'DISABLED',
								':ts': expect.any(Number),
								':updatedAt': expect.any(String)
							},
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-2'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status, #ts = :ts, #updatedAt = :updatedAt'
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					}
				])
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryId': 'primaryId',
					'#primaryNamespace': 'primaryNamespace'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 3,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'DISABLED'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK',
						status: 'DISABLED'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK',
						status: 'DISABLED'
					}
				]),
				lastEvaluatedKey: null
			});

			expect(res?.__updatedAt).not.toEqual(task.__updatedAt);
			expect(res?.status).toEqual('DISABLED');
		});

		it('should disable by fork', async () => {
			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(task, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(forkTask1, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(
					createTestSubTaskInput(forkTask2, {
						eventDelayDebounce: true,
						eventDelayUnit: 'minutes',
						eventDelayValue: 1
					})
				)
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			let retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryNamespace': 'primaryNamespace',
					'#primaryId': 'primaryId'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 9,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					}
				]),
				lastEvaluatedKey: null
			});

			const res = await hooks.setTaskActive({
				active: false,
				fork: true,
				id: 'fork-id-1',
				namespace: 'spec'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledTimes(2);
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id-1'),
					namespace: 'spec#FORK'
				},
				limit: Infinity,
				select: ['id', 'namespace', 'type']
			});
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id-1'),
					namespace: 'spec#SUBTASK'
				},
				limit: Infinity,
				prefix: true,
				select: ['id', 'namespace', 'type']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: expect.arrayContaining([
					{
						Update: {
							ExpressionAttributeNames: {
								'#status': 'status',
								'#ts': '__ts',
								'#updatedAt': '__updatedAt'
							},
							ExpressionAttributeValues: {
								':status': 'DISABLED',
								':ts': expect.any(Number),
								':updatedAt': expect.any(String)
							},
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-1'),
								namespace: 'spec#FORK'
							},
							TableName: hooks.db.tasks.table,
							UpdateExpression: 'SET #status = :status, #ts = :ts, #updatedAt = :updatedAt'
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-1')}#DELAY#[0-9]+`),
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					},
					{
						Delete: {
							Key: {
								// @ts-expect-error
								id: `${await hooks.uuidFromString('fork-id-1')}#DELAY-DEBOUNCE`,
								namespace: 'spec#SUBTASK'
							},
							TableName: hooks.db.tasks.table
						}
					}
				])
			});

			retrieved = await hooks.db.tasks.scan({
				attributeNames: {
					'#primaryId': 'primaryId',
					'#primaryNamespace': 'primaryNamespace'
				},
				attributeValues: {
					':primaryId': task.id,
					':primaryNamespace': 'spec'
				},
				filterExpression: 'begins_with(#primaryNamespace, :primaryNamespace) AND #primaryId = :primaryId',
				select: ['id', 'namespace', 'status']
			});

			expect(retrieved).toEqual({
				count: 7,
				items: expect.arrayContaining([
					{
						id: task.id,
						namespace: 'spec',
						status: 'ACTIVE'
					},
					{
						id: expect.stringMatching(`${task.id}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						id: `${task.id}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-1'),
						namespace: 'spec#FORK',
						status: 'DISABLED'
					},
					{
						// @ts-expect-error
						id: await hooks.uuidFromString('fork-id-2'),
						namespace: 'spec#FORK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: expect.stringMatching(`${await hooks.uuidFromString('fork-id-2')}#DELAY#[0-9]+`),
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					},
					{
						// @ts-expect-error
						id: `${await hooks.uuidFromString('fork-id-2')}#DELAY-DEBOUNCE`,
						namespace: 'spec#SUBTASK',
						status: 'ACTIVE'
					}
				]),
				lastEvaluatedKey: null
			});

			expect(res?.__updatedAt).not.toEqual(task.__updatedAt);
			expect(res?.status).toEqual('DISABLED');
		});

		it('should throw if task is not primary or fork', async () => {
			await hooks.db.tasks.update({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':type': 'SUBTASK' },
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
				expect(err.message).toEqual('Task must be a primary or fork task');
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

		it('should throw try to enable and task have reached repeat max', async () => {
			await hooks.db.tasks.update({
				attributeNames: {
					'#repeatMax': 'repeatMax',
					'#status': 'status',
					'#totalExecutions': 'totalExecutions'
				},
				attributeValues: {
					':repeatMax': 1,
					':status': 'DISABLED',
					':totalExecutions': 1
				},
				filter: {
					item: { id: task.id, namespace: 'spec' }
				},
				updateExpression: 'SET #repeatMax = :repeatMax, #status = :status, #totalExecutions = :totalExecutions'
			});

			try {
				await hooks.setTaskActive({
					active: true,
					id: task.id,
					namespace: 'spec'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task has reached the repeat max');
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'MAX-ERRORS-REACHED',
				title: '',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should throw is task.status !== PROCESSING', async () => {
			try {
				// @ts-expect-error
				await hooks.setTaskError({
					executionType: 'EVENT',
					pid: 'test',
					task,
					error: new Error('test')
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is not in a valid state');
			}
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

		it('should throw is task.status !== ACTIVE', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: { ':status': 'PROCESSING' },
				filter: {
					item: {
						id: task.id,
						namespace: 'spec'
					}
				},
				updateExpression: 'SET #status = :status'
			});

			try {
				// @ts-expect-error
				await hooks.setTaskLock({
					pid: 'test',
					task
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is not in a valid state');
			}
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				title: '',
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
				forkId: '',
				id: task.id,
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
				pid: '',
				primaryId: task.id,
				primaryNamespace: 'spec',
				repeatInterval: 30,
				repeatMax: 1,
				repeatUnit: 'minutes',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				rescheduleOnEvent: true,
				retryLimit: 3,
				ruleId: '',
				scheduledDate: expect.any(String),
				status: 'MAX-REPEAT-REACHED',
				title: '',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				ttl: 0,
				type: 'PRIMARY'
			});
		});

		it('should throw is task.status !== PROCESSING', async () => {
			try {
				// @ts-expect-error
				await hooks.setTaskSuccess({
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

				throw new Error('Expected to throw');
			} catch (err) {
				expect(err).toBeInstanceOf(TaskException);
				expect(err.message).toEqual('Task is not in a valid state');
			}
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
					forkId: '',
					id: task.id,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: true,
					retryLimit: 3,
					ruleId: '',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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
					forkId: '',
					id: task.id,
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
					pid: '',
					primaryId: task.id,
					primaryNamespace: 'spec',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					rescheduleOnEvent: false,
					retryLimit: 3,
					ruleId: '',
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					title: '',
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

	describe('trigger', () => {
		let tasks: Hooks.Task[];

		beforeEach(async () => {
			tasks = await Promise.all([
				hooks.registerTask(
					createTestTask(0, {
						eventPattern: 'event-pattern-1',
						requestMethod: 'GET'
					})
				),
				hooks.registerTask(
					createTestTask(0, {
						eventPattern: 'event-pattern-2',
						requestMethod: 'GET'
					})
				),
				hooks.registerTask(createTestTask(60000))
			]);

			vi.spyOn(hooks, 'callWebhook');
			// @ts-expect-error
			vi.spyOn(hooks, 'queryActiveTasks');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		describe('EVENT', () => {
			it('should works by eventPattern', async () => {
				const res = await hooks.trigger({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.callWebhook).toHaveBeenCalledOnce();
				expect(hooks.callWebhook).toHaveBeenCalledWith({
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: expect.arrayContaining(
						_.map(tasks.slice(0, 2), task => {
							return {
								id: task.id,
								namespace: task.namespace
							};
						})
					),
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(res).toEqual({
					processed: 2,
					errors: 0
				});
			});

			it('should works by eventPattern with [requestBody, requestHeaders, requestMethod, requestUrl]', async () => {
				const res = await hooks.trigger({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec',
					requestBody: { a: 1, b: 2 },
					requestHeaders: { a: '1', b: '2' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything-2'
				});

				expect(hooks.callWebhook).toHaveBeenCalledOnce();
				expect(hooks.callWebhook).toHaveBeenCalledWith({
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: expect.arrayContaining(
						_.map(tasks.slice(0, 2), task => {
							return {
								id: task.id,
								namespace: task.namespace
							};
						})
					),
					requestBody: { a: 1, b: 2 },
					requestHeaders: { a: '1', b: '2' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything-2',
					ruleId: null
				});

				expect(res).toEqual({
					processed: 2,
					errors: 0
				});
			});

			it('should works by id', async () => {
				const res = await hooks.trigger({
					// @ts-expect-error
					id: await hooks.uuidFromString('id'),
					namespace: 'spec'
				});

				// @ts-expect-error
				expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
					date: expect.any(Date),
					// @ts-expect-error
					id: await hooks.uuidFromString('id'),
					namespace: 'spec',
					onChunk: expect.any(Function)
				});

				expect(hooks.callWebhook).not.toHaveBeenCalled();
				expect(res).toEqual({
					processed: 0,
					errors: 0
				});
			});

			it('should process in chunks when webhookChunkSize is set', async () => {
				hooks.webhookChunkSize = 1;

				const res = await hooks.trigger({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.callWebhook).toHaveBeenCalledTimes(2);
				expect(hooks.callWebhook).toHaveBeenNthCalledWith(1, {
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [
						{
							id: tasks[0].id,
							namespace: tasks[0].namespace
						}
					],
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(hooks.callWebhook).toHaveBeenNthCalledWith(2, {
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'EVENT',
					forkId: null,
					forkOnly: false,
					keys: [
						{
							id: tasks[1].id,
							namespace: tasks[1].namespace
						}
					],
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(res).toEqual({
					processed: 2,
					errors: 0
				});
			});
		});

		describe('SCHEDULED', () => {
			it('should works', async () => {
				const res = await hooks.trigger();

				// @ts-expect-error
				expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
					date: expect.any(Date),
					onChunk: expect.any(Function)
				});

				expect(hooks.callWebhook).toHaveBeenCalledOnce();
				expect(hooks.callWebhook).toHaveBeenCalledWith({
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: expect.arrayContaining(
						_.map(tasks.slice(0, 2), task => {
							return {
								id: task.id,
								namespace: task.namespace
							};
						})
					),
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(res).toEqual({
					processed: 2,
					errors: 0
				});
			});

			it('should process in chunks when webhookChunkSize is set', async () => {
				// Set webhookChunkSize
				hooks.webhookChunkSize = 1;

				const res = await hooks.trigger();

				// @ts-expect-error
				expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
					date: expect.any(Date),
					onChunk: expect.any(Function)
				});

				expect(hooks.callWebhook).toHaveBeenCalledTimes(2);
				expect(hooks.callWebhook).toHaveBeenCalledWith({
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: [
						{
							id: tasks[0].id,
							namespace: tasks[0].namespace
						}
					],
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(hooks.callWebhook).toHaveBeenCalledWith({
					date: expect.any(Date),
					conditionData: null,
					eventDelayDebounce: null,
					eventDelayUnit: null,
					eventDelayValue: null,
					executionType: 'SCHEDULED',
					forkId: null,
					forkOnly: false,
					keys: [
						{
							id: tasks[1].id,
							namespace: tasks[1].namespace
						}
					],
					requestBody: null,
					requestHeaders: null,
					requestMethod: null,
					requestUrl: null,
					ruleId: null
				});

				expect(res).toEqual({
					processed: 2,
					errors: 0
				});
			});
		});
	});

	describe('updateTask', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			task = await hooks.registerTask(createTestTask());

			vi.spyOn(hooks.db.tasks, 'query');
			vi.spyOn(hooks.db.tasks, 'transaction');
		});

		afterEach(async () => {
			await hooks.clearTasks('spec');
		});

		it('should validate args', async () => {
			const invalidInput = {
				namespace: 'spec'
			};

			try {
				await hooks.updateTask(invalidInput as any);

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.transaction).not.toHaveBeenCalled();
				expect(err).toBeInstanceOf(Error);
			}
		});

		it('should validate noAfter > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.updateTask({
					id: task.id,
					namespace: 'spec',
					noAfter: `${currentYear}-01-01T00:00:00Z`,
					noBefore: `${currentYear + 1}-01-01T00:00:00-03:00`,
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.transaction).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noAfter cannot be in the past');
			}
		});

		it('should validate noBefore > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.updateTask({
					id: task.id,
					namespace: 'spec',
					noAfter: `${currentYear + 1}-01-01T00:00:00Z`, // 2026-01-01T00:00:00.000Z
					noBefore: `${currentYear}-01-01T00:00:00-03:00`, // 2025-01-01T03:00:00.000Z
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.transaction).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noBefore cannot be in the past');
			}
		});

		it('should validate noBefore > noAfter', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.updateTask({
					id: task.id,
					namespace: 'spec',
					noAfter: `${currentYear + 1}-01-01T00:00:00Z`, // 2026-01-01T00:00:00.000Z
					noBefore: `${currentYear + 1}-01-01T00:00:00-03:00`, // 2026-01-01T03:00:00.000Z
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.transaction).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('noAfter must be after noBefore');
			}
		});

		it('should validate scheduledDate > now', async () => {
			try {
				const currentYear = new Date().getFullYear();

				await hooks.updateTask({
					id: task.id,
					namespace: 'spec',
					scheduledDate: `${currentYear}-01-01T00:00:00-03:00`,
					requestUrl: 'https://httpbin.org/anything'
				});

				throw new Error('Expected to throw');
			} catch (err) {
				expect(hooks.db.tasks.transaction).not.toHaveBeenCalled();
				expect((err as z.ZodError).errors[0].message).toEqual('scheduledDate cannot be in the past');
			}
		});

		it('should update', async () => {
			const currentYear = new Date().getFullYear();

			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2, { eventDelayDebounce: true }))
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			const res = await hooks.updateTask({
				concurrency: !task.concurrency,
				conditionFilter: FilterCriteria.criteria({
					matchValue: 'value',
					operator: 'EQUALS',
					type: 'STRING',
					valuePath: ['key']
				}),
				description: 'updated',
				eventDelayDebounce: !task.eventDelayDebounce,
				eventDelayUnit: task.eventDelayUnit === 'minutes' ? 'hours' : 'minutes',
				eventDelayValue: task.eventDelayValue + 1,
				eventPattern: 'event-pattern-',
				id: task.id,
				namespace: task.namespace,
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				repeatInterval: task.repeatInterval + 1,
				repeatMax: task.repeatMax + 1,
				repeatUnit: task.repeatUnit === 'minutes' ? 'hours' : 'minutes',
				requestBody: { a: 1, b: 2 },
				requestHeaders: { a: '1', b: '2' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything-2',
				rescheduleOnEvent: !task.rescheduleOnEvent,
				retryLimit: task.retryLimit + 1,
				ruleId: 'rule-id',
				scheduledDate: `${currentYear + 1}-01-01T00:00:00-03:00`,
				title: 'updated'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledOnce();
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#type': 'type' },
				attributeValues: {
					':fork': 'FORK',
					':primary': 'PRIMARY'
				},
				filterExpression: '#type = :fork OR #type = :primary',
				item: {
					primaryId: task.id,
					primaryNamespace: 'spec'
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});

			const transactItems = vi.mocked(hooks.db.tasks.transaction).mock.calls[0][0].TransactItems;

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: expect.any(Array)
			});

			expect(transactItems).toHaveLength(3);
			expect(
				_.every(transactItems, item => {
					return _.includes(item.Update!.Key!.namespace, 'FORK');
				})
			).toBe(false);

			expect(
				_.every(transactItems, item => {
					return (
						_.isEqual(item.Update!.ExpressionAttributeNames, {
							'#concurrency': 'concurrency',
							'#conditionFilter': 'conditionFilter',
							'#description': 'description',
							'#eventDelayDebounce': 'eventDelayDebounce',
							'#eventDelayUnit': 'eventDelayUnit',
							'#eventDelayValue': 'eventDelayValue',
							'#eventPattern': 'eventPattern',
							'#noAfter': 'noAfter',
							'#namespace__eventPattern': 'namespace__eventPattern',
							'#noBefore': 'noBefore',
							'#repeatInterval': 'repeatInterval',
							'#repeatMax': 'repeatMax',
							'#repeatUnit': 'repeatUnit',
							'#requestBody': 'requestBody',
							'#requestHeaders': 'requestHeaders',
							'#requestMethod': 'requestMethod',
							'#requestUrl': 'requestUrl',
							'#rescheduleOnEvent': 'rescheduleOnEvent',
							'#retryLimit': 'retryLimit',
							'#ruleId': 'ruleId',
							'#scheduledDate': 'scheduledDate',
							'#ts': '__ts',
							'#title': 'title',
							'#updatedAt': '__updatedAt'
						}) &&
						_.isEqual(item.Update!.ExpressionAttributeValues, {
							':concurrency': true,
							':conditionFilter': {
								alias: '',
								criteriaMapper: null,
								defaultValue: '',
								matchInArray: true,
								matchValue: 'value',
								normalize: true,
								operator: 'EQUALS',
								type: 'STRING',
								valuePath: ['key'],
								valueMapper: null
							},
							':description': 'updated',
							':eventDelayDebounce': true,
							':eventDelayUnit': 'hours',
							':eventDelayValue': 1,
							':eventPattern': 'event-pattern-',
							':noAfter': `${currentYear + 1}-01-01T03:00:00.000Z`,
							':noBefore': `${currentYear + 1}-01-01T00:00:00.000Z`,
							':namespace__eventPattern': 'spec#event-pattern-',
							':repeatInterval': 31,
							':repeatMax': 1,
							':repeatUnit': 'hours',
							':requestBody': { a: 1, b: 2 },
							':requestHeaders': { a: '1', b: '2' },
							':requestMethod': 'POST',
							':requestUrl': 'https://httpbin.org/anything-2',
							':rescheduleOnEvent': false,
							':retryLimit': 4,
							':ruleId': 'rule-id',
							':scheduledDate': `${currentYear + 1}-01-01T03:00:00.000Z`,
							':title': 'updated',
							':ts': item.Update!.ExpressionAttributeValues![':ts'],
							':updatedAt': item.Update!.ExpressionAttributeValues![':updatedAt']
						}) &&
						item.Update?.UpdateExpression ===
							`SET ${[
								'#ts = :ts',
								'#updatedAt = :updatedAt',
								'#concurrency = :concurrency',
								'#conditionFilter = :conditionFilter',
								'#description = :description',
								'#eventDelayDebounce = :eventDelayDebounce',
								'#eventDelayUnit = :eventDelayUnit',
								'#eventDelayValue = :eventDelayValue',
								'#eventPattern = :eventPattern',
								'#namespace__eventPattern = :namespace__eventPattern',
								'#noAfter = :noAfter',
								'#noBefore = :noBefore',
								'#repeatInterval = :repeatInterval',
								'#repeatMax = :repeatMax',
								'#repeatUnit = :repeatUnit',
								'#requestBody = :requestBody',
								'#requestHeaders = :requestHeaders',
								'#requestMethod = :requestMethod',
								'#requestUrl = :requestUrl',
								'#rescheduleOnEvent = :rescheduleOnEvent',
								'#retryLimit = :retryLimit',
								'#ruleId = :ruleId',
								'#scheduledDate = :scheduledDate',
								'#title = :title'
							].join(', ')}`
					);
				})
			).toBe(true);

			expect(res.__updatedAt).not.toEqual(task.__updatedAt);
		});

		it('should update by fork', async () => {
			const currentYear = new Date().getFullYear();

			// @ts-expect-error
			const forkTask1 = await hooks.registerForkTask({
				forkId: 'fork-id-1',
				primaryTask: task
			});

			// @ts-expect-error
			const forkTask2 = await hooks.registerForkTask({
				forkId: 'fork-id-2',
				primaryTask: task
			});

			await Promise.all([
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(task, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask1, { eventDelayDebounce: true })),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2)),
				// @ts-expect-error
				hooks.registerDelayedSubTask(createTestSubTaskInput(forkTask2, { eventDelayDebounce: true }))
			]);
			vi.mocked(hooks.db.tasks.query).mockClear();

			const res = await hooks.updateTask({
				concurrency: !task.concurrency,
				conditionFilter: FilterCriteria.criteria({
					matchValue: 'value',
					operator: 'EQUALS',
					type: 'STRING',
					valuePath: ['key']
				}),
				description: 'updated',
				eventDelayDebounce: !task.eventDelayDebounce,
				eventDelayUnit: task.eventDelayUnit === 'minutes' ? 'hours' : 'minutes',
				eventDelayValue: task.eventDelayValue + 1,
				eventPattern: 'event-pattern-',
				fork: true,
				id: 'fork-id-1',
				namespace: task.namespace,
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				repeatInterval: task.repeatInterval + 1,
				repeatMax: task.repeatMax + 1,
				repeatUnit: task.repeatUnit === 'minutes' ? 'hours' : 'minutes',
				requestBody: { a: 1, b: 2 },
				requestHeaders: { a: '1', b: '2' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything-2',
				rescheduleOnEvent: !task.rescheduleOnEvent,
				retryLimit: task.retryLimit + 1,
				ruleId: 'rule-id',
				scheduledDate: `${currentYear + 1}-01-01T00:00:00-03:00`,
				title: 'updated'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledOnce();
			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				item: {
					// @ts-expect-error
					id: await hooks.uuidFromString('fork-id-1'),
					namespace: 'spec#FORK'
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});

			expect(hooks.db.tasks.transaction).toHaveBeenCalledWith({
				TransactItems: [
					{
						Update: {
							ExpressionAttributeNames: {
								'#ts': '__ts',
								'#updatedAt': '__updatedAt',
								'#concurrency': 'concurrency',
								'#conditionFilter': 'conditionFilter',
								'#description': 'description',
								'#eventDelayDebounce': 'eventDelayDebounce',
								'#eventDelayUnit': 'eventDelayUnit',
								'#eventDelayValue': 'eventDelayValue',
								'#eventPattern': 'eventPattern',
								'#namespace__eventPattern': 'namespace__eventPattern',
								'#noAfter': 'noAfter',
								'#noBefore': 'noBefore',
								'#repeatInterval': 'repeatInterval',
								'#repeatMax': 'repeatMax',
								'#repeatUnit': 'repeatUnit',
								'#requestBody': 'requestBody',
								'#requestHeaders': 'requestHeaders',
								'#requestMethod': 'requestMethod',
								'#requestUrl': 'requestUrl',
								'#rescheduleOnEvent': 'rescheduleOnEvent',
								'#retryLimit': 'retryLimit',
								'#ruleId': 'ruleId',
								'#scheduledDate': 'scheduledDate',
								'#title': 'title'
							},
							ExpressionAttributeValues: {
								':ts': expect.any(Number),
								':updatedAt': expect.any(String),
								':concurrency': true,
								':conditionFilter': {
									alias: '',
									criteriaMapper: null,
									defaultValue: '',
									matchInArray: true,
									matchValue: 'value',
									normalize: true,
									operator: 'EQUALS',
									type: 'STRING',
									valueMapper: null,
									valuePath: ['key']
								},
								':description': 'updated',
								':eventDelayDebounce': true,
								':eventDelayUnit': 'hours',
								':eventDelayValue': 1,
								':eventPattern': 'event-pattern-',
								':namespace__eventPattern': 'spec#event-pattern-',
								':noAfter': `${currentYear + 1}-01-01T03:00:00.000Z`,
								':noBefore': `${currentYear + 1}-01-01T00:00:00.000Z`,
								':repeatInterval': 31,
								':repeatMax': 1,
								':repeatUnit': 'hours',
								':requestBody': {
									a: 1,
									b: 2
								},
								':requestHeaders': {
									a: '1',
									b: '2'
								},
								':requestMethod': 'POST',
								':requestUrl': 'https://httpbin.org/anything-2',
								':rescheduleOnEvent': false,
								':retryLimit': 4,
								':ruleId': 'rule-id',
								':scheduledDate': `${currentYear + 1}-01-01T03:00:00.000Z`,
								':title': 'updated'
							},
							Key: {
								// @ts-expect-error
								id: await hooks.uuidFromString('fork-id-1'),
								namespace: 'spec#FORK'
							},
							TableName: 'use-dynamodb-reactive-hooks-tasks-spec',
							UpdateExpression: `SET ${[
								'#ts = :ts',
								'#updatedAt = :updatedAt',
								'#concurrency = :concurrency',
								'#conditionFilter = :conditionFilter',
								'#description = :description',
								'#eventDelayDebounce = :eventDelayDebounce',
								'#eventDelayUnit = :eventDelayUnit',
								'#eventDelayValue = :eventDelayValue',
								'#eventPattern = :eventPattern',
								'#namespace__eventPattern = :namespace__eventPattern',
								'#noAfter = :noAfter',
								'#noBefore = :noBefore',
								'#repeatInterval = :repeatInterval',
								'#repeatMax = :repeatMax',
								'#repeatUnit = :repeatUnit',
								'#requestBody = :requestBody',
								'#requestHeaders = :requestHeaders',
								'#requestMethod = :requestMethod',
								'#requestUrl = :requestUrl',
								'#rescheduleOnEvent = :rescheduleOnEvent',
								'#retryLimit = :retryLimit',
								'#ruleId = :ruleId',
								'#scheduledDate = :scheduledDate',
								'#title = :title'
							].join(', ')}`
						}
					}
				]
			});

			expect(res.__updatedAt).not.toEqual(task.__updatedAt);
		});
	});

	describe('uuid', () => {
		it('should generate a UUID', () => {
			// @ts-expect-error
			const uuid = hooks.uuid();

			expect(uuid).toMatch(/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});
	});

	describe('uuidFromString', () => {
		it('should generate a UUID from a string', async () => {
			// @ts-expect-error
			const uuid = await hooks.uuidFromString('test');
			// @ts-expect-error
			const uuid2 = await hooks.uuidFromString('test');
			// @ts-expect-error
			const uuid3 = await hooks.uuidFromString('test-2');

			expect(uuid).toMatch(/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
			expect(uuid).toEqual(uuid2);
			expect(uuid2).not.toEqual(uuid3);
		});
	});
});
