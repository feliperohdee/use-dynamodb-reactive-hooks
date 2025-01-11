import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import z from 'zod';

import Hooks, { taskShape } from './index';

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

const createTestTask = (scheduleDelay: number = 0, options?: Partial<Hooks.Task>): Hooks.Task => {
	return taskShape({
		namespace: 'spec',
		repeatInterval: 30,
		repeatUnit: 'minutes',
		requestMethod: 'POST',
		requestUrl: 'https://httpbin.org/anything',
		scheduledDate: new Date(_.now() + scheduleDelay).toISOString(),
		...options
	});
};

const createTestSubTaskInput = (options?: Partial<Hooks.SubTaskInput>): Hooks.SubTaskInput => {
	return {
		delayDebounce: false,
		delayUnit: 'minutes',
		delayValue: 1,
		id: 'test',
		namespace: 'spec',
		requestBody: { subTask: 1 },
		requestHeaders: { subTask: '1' },
		requestMethod: 'POST',
		requestUrl: 'https://httpbin.org/subtask',
		...options
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
		await Promise.all([
			hooks.clearTasks('spec'),
			hooks.clearTasks('spec#DELAYED'),
			hooks.clearTasks('spec#DEBOUNCED'),
			hooks.webhooks.clearLogs('spec')
		]);
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

	describe('callWebhook', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'delete');
			vi.spyOn(hooks, 'getTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'registerSubTask');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskLock');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskSuccess');
			// @ts-expect-error
			vi.spyOn(hooks, 'setTaskError');
			vi.spyOn(hooks.webhooks, 'trigger');

			task = await hooks.registerTask(
				createTestTask(0, {
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				})
			);
		});

		afterEach(async () => {
			await Promise.all([
				hooks.clearTasks('spec'),
				hooks.clearTasks('spec#DELAYED'),
				hooks.clearTasks('spec#DEBOUNCED'),
				hooks.webhooks.clearLogs('spec')
			]);
		});

		it('should works', async () => {
			const res = await hooks.callWebhook({
				date: new Date(),
				executionType: 'MANUAL',
				tasks: [task]
			});

			// @ts-expect-error
			expect(hooks.registerSubTask).not.toHaveBeenCalled();
			expect(hooks.getTask).not.toHaveBeenCalled();
			expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				date: expect.any(Date),
				pid: expect.any(String),
				task
			});

			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: 'MANUAL',
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});

			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
				executionType: 'MANUAL',
				log: expect.objectContaining({
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				}),
				pid: expect.any(String),
				task: {
					...task,
					__updatedAt: expect.any(String),
					status: 'PROCESSING'
				}
			});

			expect(res).toEqual([
				{
					...task,
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'MANUAL',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should works with task.idPrefix', async () => {
			task = taskShape(
				await hooks.db.tasks.update({
					attributeNames: { '#idPrefix': 'idPrefix' },
					attributeValues: { ':idPrefix': 'test' },
					filter: { item: { namespace: 'spec', id: task.id } },
					updateExpression: 'SET #idPrefix = :idPrefix'
				})
			);

			const res = await hooks.callWebhook({
				date: new Date(),
				executionType: 'MANUAL',
				tasks: [task]
			});

			// @ts-expect-error
			expect(hooks.registerSubTask).not.toHaveBeenCalled();
			expect(hooks.getTask).not.toHaveBeenCalled();
			expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				date: expect.any(Date),
				pid: expect.any(String),
				task
			});

			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: 'test#MANUAL',
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});

			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
				executionType: 'MANUAL',
				log: expect.objectContaining({
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything'
				}),
				pid: expect.any(String),
				task: {
					...task,
					__updatedAt: expect.any(String),
					status: 'PROCESSING'
				}
			});

			expect(res).toEqual([
				{
					...task,
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'MANUAL',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should works with task.concurrency = true', async () => {
			task = taskShape(
				await hooks.db.tasks.update({
					attributeNames: { '#concurrency': 'concurrency' },
					attributeValues: { ':concurrency': true },
					filter: {
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: 'SET #concurrency = :concurrency'
				})
			);

			const res = await hooks.callWebhook({
				date: new Date(),
				executionType: 'MANUAL',
				tasks: [task]
			});

			// @ts-expect-error
			expect(hooks.registerSubTask).not.toHaveBeenCalled();
			expect(hooks.getTask).not.toHaveBeenCalled();
			expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskLock).not.toHaveBeenCalled();
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: 'MANUAL',
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3
			});

			// @ts-expect-error
			expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
				executionType: 'MANUAL',
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
					__updatedAt: expect.any(String),
					firstExecutionDate: expect.any(String),
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'MANUAL',
					lastResponseBody: expect.any(String),
					lastResponseHeaders: expect.any(Object),
					lastResponseStatus: 200,
					scheduledDate: expect.any(String),
					totalExecutions: 1,
					totalSuccessfulExecutions: 1
				}
			]);
		});

		it('should handle ConditionalCheckFailedException', async () => {
			task = taskShape(
				await hooks.db.tasks.update({
					attributeNames: {
						'#pid': 'pid',
						'#status': 'status'
					},
					attributeValues: {
						':pid': 'other-pid',
						':status': 'PROCESSING'
					},
					filter: {
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: 'SET #pid = :pid, #status = :status'
				})
			);

			const res = await hooks.callWebhook({
				date: new Date(),
				executionType: 'MANUAL',
				tasks: [task]
			});

			// @ts-expect-error
			expect(hooks.registerSubTask).not.toHaveBeenCalled();
			expect(hooks.getTask).not.toHaveBeenCalled();
			expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				date: expect.any(Date),
				pid: expect.any(String),
				task
			});
			expect(hooks.webhooks.trigger).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskSuccess).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(hooks.setTaskError).not.toHaveBeenCalled();

			expect(res).toEqual([]);
		});

		it('should handle exceptions', async () => {
			vi.mocked(hooks.webhooks.trigger).mockRejectedValue(new Error('test'));

			const res = await hooks.callWebhook({
				date: new Date(),
				executionType: 'MANUAL',
				tasks: [task]
			});

			// @ts-expect-error
			expect(hooks.registerSubTask).not.toHaveBeenCalled();
			expect(hooks.getTask).not.toHaveBeenCalled();
			expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

			// @ts-expect-error
			expect(hooks.setTaskLock).toHaveBeenCalledWith({
				date: expect.any(Date),
				pid: expect.any(String),
				task
			});
			expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: 'MANUAL',
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
				executionType: 'MANUAL',
				pid: expect.any(String),
				task: {
					...task,
					__updatedAt: expect.any(String),
					status: 'PROCESSING'
				}
			});

			expect(res).toEqual([
				{
					...task,
					__updatedAt: expect.any(String),
					firstErrorDate: expect.any(String),
					lastError: 'test',
					lastErrorDate: expect.any(String),
					lastErrorExecutionType: 'MANUAL',
					totalErrors: 1
				}
			]);
		});

		describe('with delay', () => {
			it('should bypass if executionType = SCHEDULED', async () => {
				task = await hooks.registerTask(
					createTestTask(0, {
						manualDelayUnit: 'minutes',
						manualDelayValue: 1,
						requestBody: { a: 1 },
						requestHeaders: { a: '1' },
						requestMethod: 'POST',
						requestUrl: 'https://httpbin.org/anything'
					})
				);

				const res = await hooks.callWebhook({
					date: new Date(),
					executionType: 'SCHEDULED',
					tasks: [task]
				});

				// @ts-expect-error
				expect(hooks.registerSubTask).not.toHaveBeenCalled();
				expect(hooks.getTask).not.toHaveBeenCalled();
				expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					date: expect.any(Date),
					pid: expect.any(String),
					task
				});

				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					idPrefix: 'SCHEDULED',
					namespace: 'spec',
					requestBody: { a: 1 },
					requestHeaders: { a: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3
				});

				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
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
						__updatedAt: expect.any(String),
						status: 'PROCESSING'
					}
				});

				expect(res).toEqual([
					{
						...task,
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

			it.only('should works with delayed task', async () => {
				task = await hooks.registerTask(
					createTestTask(0, {
						manualDelayUnit: 'minutes',
						manualDelayValue: 1,
						requestBody: { subTask: 1 },
						requestHeaders: { subTask: '1' },
						requestMethod: 'POST',
						requestUrl: 'https://httpbin.org/subtask'
					})
				);

				const res = await hooks.callWebhook({
					date: new Date(),
					executionType: 'MANUAL',
					tasks: [task]
				});

				// @ts-expect-error
				expect(hooks.registerSubTask).toHaveBeenCalledWith({
					delayDebounce: false,
					delayDebounceId: '',
					id: task.id,
					namespace: task.namespace,
					requestBody: { subTask: 1 },
					requestHeaders: { subTask: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/subtask',
					delayUnit: 'minutes',
					delayValue: 1
				});
				expect(hooks.getTask).not.toHaveBeenCalled();
				expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();

				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res[0].id).toEqual(expect.stringMatching(`${task.id}#[0-9]`));
				expect(res[0].namespace).toEqual('spec#DELAYED');
				expect(res[0].parentId).toEqual(task.id);
				expect(res[0].parentNamespace).toEqual(task.namespace);
				expect(res[0].type).toEqual('DELAYED');
			});

			it.only('should works with debounced task', async () => {
				task = await hooks.registerTask(
					createTestTask(0, {
						manualDelayDebounce: true,
						manualDelayUnit: 'minutes',
						manualDelayValue: 1,
						requestBody: { subTask: 1 },
						requestHeaders: { subTask: '1' },
						requestMethod: 'POST',
						requestUrl: 'https://httpbin.org/subtask'
					})
				);

				const res = await hooks.callWebhook({
					date: new Date(),
					executionType: 'MANUAL',
					tasks: [task]
				});

				// @ts-expect-error
				expect(hooks.registerSubTask).toHaveBeenCalledWith({
					delayDebounce: true,
					delayDebounceId: '',
					id: task.id,
					namespace: task.namespace,
					requestBody: { subTask: 1 },
					requestHeaders: { subTask: '1' },
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/subtask',
					delayUnit: 'minutes',
					delayValue: 1
				});
				expect(hooks.getTask).not.toHaveBeenCalled();
				expect(hooks.db.tasks.delete).not.toHaveBeenCalled();

				// @ts-expect-error
				expect(hooks.setTaskLock).not.toHaveBeenCalled();
				expect(hooks.webhooks.trigger).not.toHaveBeenCalled();

				// @ts-expect-error
				expect(hooks.setTaskSuccess).not.toHaveBeenCalled();

				expect(res[0].id).toEqual(task.id);
				expect(res[0].namespace).toEqual('spec#DEBOUNCED');
				expect(res[0].parentId).toEqual(task.id);
				expect(res[0].parentNamespace).toEqual(task.namespace);
				expect(res[0].type).toEqual('DEBOUNCED');
			});
		});

		describe('with subTasks', () => {
			let subTask: Hooks.Task;

			beforeEach(async () => {
				// @ts-expect-error
				subTask = await hooks.registerSubTask(
					createTestSubTaskInput({
						id: task.id
					})
				);

				// @ts-expect-error
				vi.mocked(hooks.registerSubTask).mockClear();
			});

			it('should works', async () => {
				task = taskShape(
					await hooks.db.tasks.update({
						attributeNames: { '#idPrefix': 'idPrefix' },
						attributeValues: { ':idPrefix': 'test' },
						filter: { item: { namespace: 'spec', id: task.id } },
						updateExpression: 'SET #idPrefix = :idPrefix'
					})
				);

				const res = await hooks.callWebhook({
					date: new Date(),
					executionType: 'MANUAL',
					tasks: [subTask]
				});

				// @ts-expect-error
				expect(hooks.registerSubTask).not.toHaveBeenCalled();
				expect(hooks.getTask).toHaveBeenCalledWith({
					id: task.id,
					namespace: task.namespace
				});
				expect(hooks.db.tasks.delete).toHaveBeenCalledWith({
					filter: {
						item: {
							namespace: subTask.namespace,
							id: subTask.id
						}
					}
				});

				// @ts-expect-error
				expect(hooks.setTaskLock).toHaveBeenCalledWith({
					date: expect.any(Date),
					pid: expect.any(String),
					task
				});

				expect(hooks.webhooks.trigger).toHaveBeenCalledWith({
					idPrefix: 'test#MANUAL#DELAYED',
					namespace: 'spec',
					requestBody: subTask.requestBody,
					requestHeaders: subTask.requestHeaders,
					requestMethod: subTask.requestMethod,
					requestUrl: subTask.requestUrl,
					retryLimit: 3
				});

				// @ts-expect-error
				expect(hooks.setTaskSuccess).toHaveBeenCalledWith({
					executionType: 'MANUAL',
					log: expect.objectContaining({
						requestBody: subTask.requestBody,
						requestHeaders: subTask.requestHeaders,
						requestMethod: subTask.requestMethod,
						requestUrl: subTask.requestUrl
					}),
					pid: expect.any(String),
					task: {
						...task,
						__updatedAt: expect.any(String),
						status: 'PROCESSING'
					}
				});

				expect(res).toEqual([
					{
						...task,
						__updatedAt: expect.any(String),
						firstExecutionDate: expect.any(String),
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'MANUAL',
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

	describe('clear', () => {
		it('should clear namespace', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);

			const res = await hooks.clearTasks('spec');
			expect(res.count).toEqual(3);

			const remaining = await hooks.db.tasks.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('deleteTask', () => {
		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should delete', async () => {
			const task = await hooks.registerTask(createTestTask());

			const deleted = await hooks.deleteTask({
				id: task.id,
				namespace: task.namespace
			});

			expect(deleted).toEqual(task);

			const retrieved = await hooks.getTask({
				id: task.id,
				namespace: task.namespace
			});

			expect(retrieved).toBeNull();
		});

		it('should returns null if inexistent', async () => {
			const deleted = await hooks.deleteTask({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(deleted).toBeNull();
		});
	});

	describe('deleteMany', () => {
		beforeEach(() => {
			vi.spyOn(hooks, 'fetch');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should delete many', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);

			const res = await hooks.deleteMany({
				namespace: 'spec'
			});

			expect(hooks.fetch).toHaveBeenCalledWith({
				chunkLimit: 100,
				desc: false,
				manualEventPatternPrefix: false,
				idPrefix: false,
				limit: Infinity,
				namespace: 'spec',
				onChunk: expect.any(Function),
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: res.items
			});

			const retrieved = await hooks.fetch({
				namespace: 'spec'
			});

			expect(retrieved.count).toEqual(0);
		});
	});

	describe('fetch', () => {
		let tasks: Hooks.Task[];
		let subTasks: Hooks.Task[];

		beforeAll(async () => {
			tasks = await Promise.all(
				_.map(
					[
						createTestTask(0, {
							manualEventPattern: 'event-pattern-1'
						}),
						createTestTask(1000, {
							manualEventPattern: 'event-pattern-2'
						}),
						createTestTask(2000, {
							manualEventPattern: 'event-pattern-3'
						})
					],
					task => {
						return hooks.registerTask(task);
					}
				)
			);

			subTasks = await Promise.all(
				_.map(
					[
						createTestSubTaskInput({
							id: tasks[0].id,
							namespace: tasks[0].namespace
						}),
						createTestSubTaskInput({
							delayDebounce: true,
							id: tasks[0].id,
							namespace: tasks[0].namespace
						}),
						createTestSubTaskInput({
							delayDebounce: true,
							delayDebounceId: 'debounce-id',
							id: tasks[0].id,
							namespace: tasks[0].namespace
						})
					],
					task => {
						// @ts-expect-error
						return hooks.registerSubTask(task);
					}
				)
			);
		});

		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'query');
		});

		afterAll(async () => {
			await Promise.all([
				hooks.clearTasks('spec'),
				hooks.clearTasks('spec#DELAYED'),
				hooks.clearTasks('spec#DEBOUNCED'),
				hooks.webhooks.clearLogs('spec')
			]);
		});

		it('should fetch by [namespace]', async () => {
			const res = await hooks.fetch({
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
			const res = await hooks.fetch({
				namespace: 'spec',
				type: 'DELAYED'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#DELAYED' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: expect.arrayContaining(_.filter(subTasks, { type: 'DELAYED' })),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] with debounced subTasks', async () => {
			const res = await hooks.fetch({
				namespace: 'spec',
				type: 'DEBOUNCED'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#DEBOUNCED' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 2,
				items: expect.arrayContaining(_.filter(subTasks, { type: 'DEBOUNCED' })),
				lastEvaluatedKey: null
			});
		});

		it.skip('should fetch by [namespace] with identified debounced subTasks', async () => {
			const res = await hooks.fetch({
				delayDebounceId: 'debounce-id',
				namespace: 'spec',
				type: 'DEBOUNCED'
			});

			expect(hooks.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: { '#namespace': 'namespace' },
				attributeValues: { ':namespace': 'spec#DEBOUNCED#debounce-id' },
				filterExpression: '',
				limit: 100,
				queryExpression: '#namespace = :namespace',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: expect.arrayContaining(
					_.filter(subTasks, {
						namespace: 'spec#DEBOUNCED#debounce-id',
						type: 'DEBOUNCED'
					})
				),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, desc, chunkLimit, onChunk, status]', async () => {
			const res = await hooks.fetch({
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
				const res = await hooks.fetch({
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
					queryExpression: '#namespace = :namespace AND #id = :id',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id] with idPrefix = true', async () => {
				const res = await hooks.fetch({
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
					startKey: null
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[0]],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id, manualEventPattern, scheduledDate, status]', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					id: tasks[0].id.slice(0, 8),
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#manualEventPattern': 'manualEventPattern',
						'#id': 'id',
						'#namespace': 'namespace',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':manualEventPattern': 'event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':id': tasks[0].id.slice(0, 8),
						':namespace': 'spec',
						':status': 'ACTIVE',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression:
						'#manualEventPattern = :manualEventPattern AND #scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate AND #status = :status',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #id = :id',
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

		describe('query by [manualEventPattern]', () => {
			it('should fetch by [namespace, manualEventPattern]', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#manualEventPattern': 'manualEventPattern'
					},
					attributeValues: {
						':namespace': 'spec',
						':manualEventPattern': 'event-pattern-'
					},
					filterExpression: '',
					index: 'namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #manualEventPattern = :manualEventPattern',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, manualEventPattern] with manualEventPatternPrefix = true', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					manualEventPatternPrefix: true,
					namespace: 'spec'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#manualEventPattern': 'manualEventPattern'
					},
					attributeValues: {
						':namespace': 'spec',
						':manualEventPattern': 'event-pattern-'
					},
					filterExpression: '',
					index: 'namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND begins_with(#manualEventPattern, :manualEventPattern)',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 3,
					items: expect.arrayContaining(tasks),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, manualEventPattern, scheduledDate]', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#manualEventPattern': 'manualEventPattern',
						'#scheduledDate': 'scheduledDate'
					},
					attributeValues: {
						':namespace': 'spec',
						':manualEventPattern': 'event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':toScheduledDate': '2024-03-18T10:00:00.000Z'
					},
					filterExpression: '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					index: 'namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#namespace = :namespace AND #manualEventPattern = :manualEventPattern',
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

		describe('query by [manualEventPattern, status]', () => {
			it('should fetch by [namespace, manualEventPattern, status]', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__manualEventPattern': '__namespace__manualEventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__manualEventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'status-namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND #namespace__manualEventPattern = :namespace__manualEventPattern',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, manualEventPattern, status] with manualEventPatternPrefix = true', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					manualEventPatternPrefix: true,
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__manualEventPattern': '__namespace__manualEventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__manualEventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'status-namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND begins_with(#namespace__manualEventPattern, :namespace__manualEventPattern)',
					scanIndexForward: true,
					startKey: null
				});

				expect(res).toEqual({
					count: 3,
					items: expect.arrayContaining(tasks),
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, manualEventPattern, fromScheduledDate, toScheduledDate, status]', async () => {
				const res = await hooks.fetch({
					manualEventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__manualEventPattern': '__namespace__manualEventPattern',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__manualEventPattern': 'spec#event-pattern-',
						':fromScheduledDate': '2024-03-18T10:00:00.000Z',
						':toScheduledDate': '2024-03-18T10:00:00.000Z',
						':status': 'ACTIVE'
					},
					filterExpression: '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate',
					index: 'status-namespace-manual-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND #namespace__manualEventPattern = :namespace__manualEventPattern',
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
				const res = await hooks.fetch({
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
				const res = await hooks.fetch({
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

	describe('fetchLogs', () => {
		beforeAll(async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);
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

	describe('getTask', () => {
		let task: Hooks.Task;
		let subTasks: Hooks.Task[];

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
			subTasks = await Promise.all([
				// @ts-expect-error
				hooks.registerSubTask(createTestSubTaskInput()),
				// @ts-expect-error
				hooks.registerSubTask(
					createTestSubTaskInput({
						delayDebounce: true
					})
				),
				// @ts-expect-error
				hooks.registerSubTask(
					createTestSubTaskInput({
						delayDebounce: true,
						delayDebounceId: 'debounce-id'
					})
				)
			]);
		});

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'get');
		});

		afterAll(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should get', async () => {
			const res = await hooks.getTask({
				id: task.id,
				namespace: task.namespace
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					namespace: 'spec',
					id: expect.any(String)
				},
				prefix: false
			});

			expect(res).toEqual(task);
		});

		it('should get delayed subTask', async () => {
			const res = await hooks.getTask({
				id: subTasks[0].parentId,
				namespace: subTasks[0].parentNamespace,
				type: 'DELAYED'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					namespace: 'spec#DELAYED',
					id: 'test'
				},
				prefix: true
			});

			expect(res).toEqual(subTasks[0]);
		});

		it('should get debounced subTask', async () => {
			const res = await hooks.getTask({
				id: subTasks[1].parentId,
				namespace: subTasks[1].parentNamespace,
				type: 'DEBOUNCED'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					namespace: 'spec#DEBOUNCED',
					id: 'test'
				},
				prefix: false
			});

			expect(res).toEqual(subTasks[1]);
		});

		it('should returns identified debounced subTask', async () => {
			const res = await hooks.getTask({
				delayDebounceId: 'debounce-id',
				id: subTasks[2].parentId,
				namespace: subTasks[2].parentNamespace,
				type: 'DEBOUNCED'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					namespace: 'spec#DEBOUNCED',
					id: 'test#debounce-id'
				},
				prefix: false
			});

			expect(res).toEqual(subTasks[2]);
		});

		it('should returns null if inexistent', async () => {
			const res = await hooks.getTask({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(hooks.db.tasks.get).toHaveBeenCalledWith({
				item: {
					namespace: 'spec',
					id: 'non-existent-id'
				},
				prefix: false
			});

			expect(res).toBeNull();
		});
	});

	describe('queryActiveTasks', () => {
		let tasks: Hooks.Task[];

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'query');
		});

		describe('edge cases', () => {
			beforeEach(async () => {
				tasks = await Promise.all(
					_.map([createTestTask(), createTestTask()], task => {
						return hooks.registerTask(task);
					})
				);
			});

			afterEach(async () => {
				await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
			});

			it('should no return tasks with pid', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#pid': 'pid' },
					attributeValues: { ':pid': 'test' },
					filter: {
						item: { namespace: 'spec', id: tasks[0].id }
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
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
							item: { namespace: 'spec', id: tasks[0].id }
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
							item: { namespace: 'spec', id: tasks[1].id }
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
				});

				expect(res).toEqual({
					count: 1,
					items: [
						{
							...tasks[1],
							__updatedAt: expect.any(String),
							totalExecutions: 1000
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
						item: { namespace: 'spec', id: tasks[0].id }
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
					lastEvaluatedKey: null
				});
			});

			it('should no return tasks with now > noAfter', async () => {
				await hooks.db.tasks.update({
					attributeNames: { '#noAfter': 'noAfter' },
					attributeValues: { ':noAfter': '1999-01-01T00:00:00.000Z' },
					filter: {
						item: { namespace: 'spec', id: tasks[0].id }
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by manualEventPattern', () => {
			beforeAll(async () => {
				tasks = await Promise.all(
					_.map(
						[
							createTestTask(0, {
								manualEventPattern: ''
							}),
							createTestTask(0, {
								manualEventPattern: 'event-pattern-1'
							}),
							createTestTask(0, {
								manualEventPattern: 'event-pattern-2'
							})
						],
						task => {
							return hooks.registerTask(task);
						}
					)
				);
			});

			afterAll(async () => {
				await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
			});

			it('should fetch by [namespace, manualEventPattern]', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					manualEventPattern: 'event-pattern-1',
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__manualEventPattern': '__namespace__manualEventPattern',
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
						':namespace__manualEventPattern': 'spec#event-pattern-1',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-namespace-manual-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #namespace__manualEventPattern = :namespace__manualEventPattern'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, manualEventPattern] with manualEventPatternPrefix = true', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					manualEventPattern: 'event-pattern-',
					manualEventPatternPrefix: true,
					namespace: 'spec',
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__manualEventPattern': '__namespace__manualEventPattern',
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
						':namespace__manualEventPattern': 'spec#event-pattern-',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-namespace-manual-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND begins_with(#namespace__manualEventPattern, :namespace__manualEventPattern)'
				});

				expect(res).toEqual({
					count: 2,
					items: expect.arrayContaining([tasks[1], tasks[2]]),
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by id', () => {
			beforeAll(async () => {
				tasks = await Promise.all(
					_.map([createTestTask(), createTestTask()], task => {
						return hooks.registerTask(task);
					})
				);
			});

			afterAll(async () => {
				await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)',
						'#status = :active'
					].join(' AND '),
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#namespace = :namespace AND #id = :id'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[0]],
					lastEvaluatedKey: null
				});
			});

			it('should fetch by [namespace, id] with idPrefix = true', async () => {
				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					id: tasks[0].id.slice(0, 3),
					idPrefix: true,
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
						':id': tasks[0].id.slice(0, 3),
						':namespace': 'spec',
						':now': expect.any(String),
						':zero': 0
					},
					chunkLimit: 100,
					filterExpression: [
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)',
						'#status = :active'
					].join(' AND '),
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#namespace = :namespace AND begins_with(#id, :id)'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[0]],
					lastEvaluatedKey: null
				});
			});
		});

		describe('query by scheduledDate', () => {
			beforeAll(async () => {
				tasks = await Promise.all(
					_.map(
						[
							createTestTask(0, {
								scheduledDate: ''
							}),
							createTestTask(0),
							createTestTask(5000)
						],
						task => {
							return hooks.registerTask(task);
						}
					)
				);
			});

			afterAll(async () => {
				await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'attribute_not_exists(#pid)',
						'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-scheduled-date',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
					lastEvaluatedKey: null
				});
			});
		});
	});

	describe('registerSubTask', () => {
		afterEach(async () => {
			await Promise.all([
				hooks.clearTasks('spec'),
				hooks.clearTasks('spec#DELAYED'),
				hooks.clearTasks('spec#DEBOUNCED'),
				hooks.webhooks.clearLogs('spec')
			]);
		});

		it('should create subTask', async () => {
			// @ts-expect-error
			const res = await hooks.registerSubTask({
				delayDebounce: false,
				delayUnit: 'minutes',
				delayValue: 1,
				id: 'test',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything'
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - Date.now();

			expect(scheduledDateDiff).toBeGreaterThan(55 * 1000);
			expect(scheduledDateDiff).toBeLessThan(60 * 1000);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.stringMatching(/^test#[0-9]+$/),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec#DELAYED',
				noAfter: '',
				noBefore: '',
				parentId: 'test',
				parentNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'DELAYED'
			});
		});

		it('should create debounced subTask', async () => {
			// @ts-expect-error
			const res = await hooks.registerSubTask({
				delayDebounce: true,
				delayUnit: 'minutes',
				delayValue: 1,
				id: 'test',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything'
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - Date.now();

			expect(scheduledDateDiff).toBeGreaterThan(55 * 1000);
			expect(scheduledDateDiff).toBeLessThan(60 * 1000);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: 'test',
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec#DEBOUNCED',
				noAfter: '',
				noBefore: '',
				parentId: 'test',
				parentNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'DEBOUNCED'
			});
		});

		it('should create identified debounced subTask', async () => {
			// @ts-expect-error
			const res = await hooks.registerSubTask({
				delayDebounce: true,
				delayDebounceId: 'debounce-id',
				delayUnit: 'minutes',
				delayValue: 1,
				id: 'test',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything'
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - Date.now();

			expect(scheduledDateDiff).toBeGreaterThan(55 * 1000);
			expect(scheduledDateDiff).toBeLessThan(60 * 1000);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: 'test#debounce-id',
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec#DEBOUNCED',
				noAfter: '',
				noBefore: '',
				parentId: 'test',
				parentNamespace: 'spec',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'DEBOUNCED'
			});
		});
	});

	describe('registerTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'put');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
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
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: '',
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: '-',
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
			});
		});

		it('should create task by [idPrefix, manualEventPattern, noAfter, noBefore, scheduledDate]', async () => {
			const currentYear = new Date().getFullYear();
			const res = await hooks.registerTask({
				manualEventPattern: 'test-event-pattern',
				idPrefix: 'test-',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				requestUrl: 'https://httpbin.org/anything',
				scheduledDate: `${currentYear + 1}-01-01T00:00:00-03:00`
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: 'spec#test-event-pattern',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				id: expect.any(String),
				idPrefix: 'test-',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: 'test-event-pattern',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T03:00:00.000Z`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				parentId: '',
				parentNamespace: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'GET',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
			});
		});
	});

	describe('setTaskError', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'update');

			task = await hooks.registerTask(createTestTask());
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
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
				filter: { item: { namespace: 'spec', id: task.id } },
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'MANUAL',
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
					':error': 'test',
					':executionType': 'MANUAL',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#firstErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'MANUAL',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
			});
		});

		it('should set with task.concurrency = true', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#concurrency': 'concurrency' },
				attributeValues: { ':concurrency': true },
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: 'SET #concurrency = :concurrency'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'MANUAL',
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
					':error': 'test',
					':executionType': 'MANUAL',
					':now': expect.any(String),
					':one': 1
				},
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#firstErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: true,
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'MANUAL',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
			});
		});

		it('should set task status = MAX_ERRORS_REACHED if max errors reached', async () => {
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
				filter: { item: { namespace: 'spec', id: task.id } },
				updateExpression: 'SET #pid = :pid, #status = :processing'
			});
			vi.mocked(hooks.db.tasks.update).mockClear();

			// @ts-expect-error
			const res = await hooks.setTaskError({
				executionType: 'MANUAL',
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
					':error': 'test',
					':executionType': 'MANUAL',
					':maxErrorsReached': 'MAX_ERRORS_REACHED',
					':now': expect.any(String),
					':one': 1,
					':pid': 'test',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					'SET',
					[
						'#lastErrorExecutionType = :executionType',
						'#lastError = :error',
						'#lastErrorDate = :now',
						'#firstErrorDate = :now',
						'#status = :maxErrorsReached'
					].join(', '),
					'ADD',
					['#totalErrors :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: expect.any(String),
				firstExecutionDate: '',
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: 'test',
				lastErrorDate: expect.any(String),
				lastErrorExecutionType: 'MANUAL',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'MAX_ERRORS_REACHED',
				totalErrors: 1,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
			});
		});
	});

	describe('setTaskLock', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'update');

			task = await hooks.registerTask(createTestTask());
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should set task status = PROCESSING and set pid', async () => {
			// @ts-expect-error
			await hooks.setTaskLock({
				date: new Date(),
				pid: 'test',
				task
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
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
					':now': expect.any(String),
					':pid': 'test',
					':processing': 'PROCESSING',
					':zero': 0
				},
				// in case of other process already picked the task while it was being processed
				conditionExpression: [
					'attribute_not_exists(#pid)',
					'#status = :active',
					'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
					'(#noBefore = :empty OR :now > #noBefore)',
					'(#noAfter = :empty OR :now < #noAfter)'
				].join(' AND '),
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			const retrieved = await hooks.db.tasks.get<Hooks.Task>({
				item: { namespace: 'spec', id: task.id }
			});

			expect(retrieved?.status).toEqual('PROCESSING');
			expect(retrieved?.['pid']).toEqual('test');
		});
	});

	describe('setTaskSuccess', () => {
		let task: Hooks.Task;

		beforeEach(async () => {
			vi.spyOn(hooks.db.tasks, 'update');

			task = await hooks.registerTask(createTestTask());
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
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
					item: { namespace: 'spec', id: task.id }
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
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
					`REMOVE #pid`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				type: 'REGULAR'
			});
		});

		it('should set with task.concurrency = true', async () => {
			task = await hooks.db.tasks.update({
				attributeNames: { '#concurrency': 'concurrency' },
				attributeValues: { ':concurrency': true },
				filter: {
					item: { namespace: 'spec', id: task.id }
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
					':executionType': 'SCHEDULED',
					':now': expect.any(String),
					':one': 1,
					':responseBody': 'test',
					':responseHeaders': {},
					':responseStatus': 200,
					':scheduledDate': expect.any(String)
				},
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
					`REMOVE #pid`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: true,
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				type: 'REGULAR'
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
					item: { namespace: 'spec', id: task.id }
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
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#firstExecutionDate = :now',
						'#scheduledDate = :scheduledDate',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
					`REMOVE #pid`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 400,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 1,
				totalSuccessfulExecutions: 0,
				type: 'REGULAR'
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
					item: { namespace: 'spec', id: task.id }
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
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#firstExecutionDate = :now',
						'#status = :active'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
					`REMOVE #pid`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(0);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 0,
				repeatMax: 0,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				type: 'REGULAR'
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
					item: { namespace: 'spec', id: task.id }
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
					':executionType': 'SCHEDULED',
					':maxRepeatReached': 'MAX_REPEAT_REACHED',
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
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: [
					`SET ${[
						'#lastExecutionDate = :now',
						'#lastExecutionType = :executionType',
						'#lastResponseBody = :responseBody',
						'#lastResponseHeaders = :responseHeaders',
						'#lastResponseStatus = :responseStatus',
						'#firstExecutionDate = :now',
						'#status = :maxRepeatReached'
					].join(', ')}`,
					`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
					`REMOVE #pid`
				].join(' ')
			});

			const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

			expect(scheduledDateDiff).toEqual(0);
			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__manualEventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				firstErrorDate: '',
				firstExecutionDate: expect.any(String),
				firstScheduledDate: expect.any(String),
				id: expect.any(String),
				idPrefix: '',
				lastError: '',
				lastErrorDate: '',
				lastErrorExecutionType: '',
				lastExecutionDate: expect.any(String),
				lastExecutionType: 'SCHEDULED',
				lastResponseBody: 'test',
				lastResponseHeaders: {},
				lastResponseStatus: 200,
				manualDelayDebounce: false,
				manualDelayUnit: 'minutes',
				manualDelayValue: 0,
				manualEventPattern: '-',
				manualReschedule: true,
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				parentId: '',
				parentNamespace: '',
				repeatInterval: 30,
				repeatMax: 1,
				repeatUnit: 'minutes',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'MAX_REPEAT_REACHED',
				totalErrors: 0,
				totalExecutions: 1,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 1,
				type: 'REGULAR'
			});
		});

		describe('executionType = MANUAL', () => {
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
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: 'SET #pid = :pid, #status = :processing'
				});
				vi.mocked(hooks.db.tasks.update).mockClear();

				// @ts-expect-error
				const res = await hooks.setTaskSuccess({
					executionType: 'MANUAL',
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
						':executionType': 'MANUAL',
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
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: [
						`SET ${[
							'#lastExecutionDate = :now',
							'#lastExecutionType = :executionType',
							'#lastResponseBody = :responseBody',
							'#lastResponseHeaders = :responseHeaders',
							'#lastResponseStatus = :responseStatus',
							'#firstExecutionDate = :now',
							'#scheduledDate = :scheduledDate',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
						`REMOVE #pid`
					].join(' ')
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__namespace__manualEventPattern: '-',
					__updatedAt: expect.any(String),
					concurrency: false,
					firstErrorDate: '',
					firstExecutionDate: expect.any(String),
					firstScheduledDate: expect.any(String),
					id: expect.any(String),
					idPrefix: '',
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'MANUAL',
					lastResponseBody: 'test',
					lastResponseHeaders: {},
					lastResponseStatus: 200,
					manualDelayDebounce: false,
					manualDelayUnit: 'minutes',
					manualDelayValue: 0,
					manualEventPattern: '-',
					manualReschedule: true,
					namespace: 'spec',
					noAfter: '',
					noBefore: '',
					parentId: '',
					parentNamespace: '',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 1,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 1,
					type: 'REGULAR'
				});
			});

			it('should set with task.manualReschedule = false', async () => {
				task = await hooks.db.tasks.update({
					attributeNames: {
						'#manualReschedule': 'manualReschedule',
						'#status': 'status',
						'#pid': 'pid'
					},
					attributeValues: {
						':manualReschedule': false,
						':pid': 'test',
						':processing': 'PROCESSING'
					},
					filter: {
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: 'SET #pid = :pid, #manualReschedule = :manualReschedule, #status = :processing'
				});
				vi.mocked(hooks.db.tasks.update).mockClear();

				// @ts-expect-error
				const res = await hooks.setTaskSuccess({
					executionType: 'MANUAL',
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
						':executionType': 'MANUAL',
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
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: [
						`SET ${[
							'#lastExecutionDate = :now',
							'#lastExecutionType = :executionType',
							'#lastResponseBody = :responseBody',
							'#lastResponseHeaders = :responseHeaders',
							'#lastResponseStatus = :responseStatus',
							'#firstExecutionDate = :now',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
						`REMOVE #pid`
					].join(' ')
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(0);
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__namespace__manualEventPattern: '-',
					__updatedAt: expect.any(String),
					concurrency: false,
					firstErrorDate: '',
					firstExecutionDate: expect.any(String),
					firstScheduledDate: expect.any(String),
					id: expect.any(String),
					idPrefix: '',
					lastError: '',
					lastErrorDate: '',
					lastErrorExecutionType: '',
					lastExecutionDate: expect.any(String),
					lastExecutionType: 'MANUAL',
					lastResponseBody: 'test',
					lastResponseHeaders: {},
					lastResponseStatus: 200,
					manualDelayDebounce: false,
					manualDelayUnit: 'minutes',
					manualDelayValue: 0,
					manualEventPattern: '-',
					manualReschedule: false,
					namespace: 'spec',
					noAfter: '',
					noBefore: '',
					parentId: '',
					parentNamespace: '',
					repeatInterval: 30,
					repeatMax: 0,
					repeatUnit: 'minutes',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE',
					totalErrors: 0,
					totalExecutions: 1,
					totalFailedExecutions: 0,
					totalSuccessfulExecutions: 1,
					type: 'REGULAR'
				});
			});
		});
	});

	describe('suspendTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should suspend an active task', async () => {
			const task = await hooks.registerTask(createTestTask());

			const suspended = await hooks.suspendTask({
				id: task.id,
				namespace: task.namespace
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :active',
				filter: {
					item: { namespace: task.namespace, id: task.id }
				},
				updateExpression: 'SET #status = :suspended'
			});

			expect(suspended?.status).toEqual('SUSPENDED');
		});

		it('should not suspend a non-active task', async () => {
			const task = await hooks.registerTask(createTestTask());

			// First suspend succeeds
			await hooks.suspendTask({
				id: task.id,
				namespace: task.namespace
			});

			// Second suspend should fail condition check
			await expect(
				hooks.suspendTask({
					id: task.id,
					namespace: task.namespace
				})
			).rejects.toThrow(ConditionalCheckFailedException);
		});

		it('should return null for non-existent task', async () => {
			const suspended = await hooks.suspendTask({
				id: 'non-existent',
				namespace: 'spec'
			});

			expect(suspended).toBeNull();
			expect(hooks.db.tasks.update).not.toHaveBeenCalled();
		});
	});

	describe('suspendManyTasks', () => {
		beforeEach(() => {
			vi.spyOn(hooks, 'fetch');
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should suspend many tasks', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);

			// cause condition check to fail
			await hooks.suspendTask({
				id: tasks[0].id,
				namespace: tasks[0].namespace
			});

			const res = await hooks.suspendManyTasks({
				namespace: 'spec'
			});

			expect(hooks.fetch).toHaveBeenCalledWith({
				chunkLimit: 100,
				limit: Infinity,
				namespace: 'spec',
				onChunk: expect.any(Function),
				startKey: null
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :active',
				filter: {
					item: { namespace: 'spec', id: expect.any(String) }
				},
				updateExpression: 'SET #status = :suspended'
			});

			expect(res).toEqual({
				count: 2,
				items: res.items
			});

			const retrieved = await hooks.fetch({
				namespace: 'spec'
			});

			expect(
				retrieved.items.every(item => {
					return item.status === 'SUSPENDED';
				})
			).toBe(true);
		});
	});

	describe('trigger', () => {
		beforeEach(async () => {
			await Promise.all(
				_.map(
					[
						createTestTask(0, {
							manualEventPattern: 'event-pattern-1',
							idPrefix: 'id-prefix-1',
							requestBody: { a: 1 },
							requestHeaders: { a: '1' },
							requestMethod: 'GET',
							requestUrl: 'https://httpbin.org/anything'
						}),
						createTestTask(0, {
							manualEventPattern: 'event-pattern-2',
							idPrefix: 'id-prefix-2',
							requestBody: { a: 1 },
							requestHeaders: { a: '1' },
							requestMethod: 'GET',
							requestUrl: 'https://httpbin.org/anything'
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

		it('should works MANUAL by manualEventPattern', async () => {
			const res = await hooks.trigger({
				manualEventPattern: 'event-pattern-',
				manualEventPatternPrefix: true,
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				manualEventPattern: 'event-pattern-',
				manualEventPatternPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				executionType: 'MANUAL',
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

		it('should works MANUAL by manualEventPattern with [body, headers, method, url]', async () => {
			const res = await hooks.trigger({
				manualEventPattern: 'event-pattern-',
				manualEventPatternPrefix: true,
				namespace: 'spec',
				requestBody: { a: 2, b: 3 },
				requestHeaders: { a: '2', b: '3' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything-2'
			});

			// @ts-expect-error
			expect(hooks.queryActiveTasks).toHaveBeenCalledWith({
				date: expect.any(Date),
				manualEventPattern: 'event-pattern-',
				manualEventPatternPrefix: true,
				namespace: 'spec',
				onChunk: expect.any(Function)
			});

			expect(hooks.callWebhook).toHaveBeenCalledWith({
				date: expect.any(Date),
				executionType: 'MANUAL',
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

		it('should works MANUAL with conditionFilter', async () => {
			const res = await hooks.trigger({
				conditionData: {
					a: 'test'
				},
				conditionFilter: {
					type: 'STRING',
					path: ['a'],
					operator: 'EQUALS',
					value: 'test-1'
				},
				manualEventPattern: 'event-pattern-',
				manualEventPatternPrefix: true,
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

		it('should works MANUAL by id', async () => {
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
				executionType: 'MANUAL',
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

		it('should works MANUAL by id with [body, headers, method, url]', async () => {
			const res = await hooks.trigger({
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
				executionType: 'MANUAL',
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

	describe('unsuspendTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([hooks.clearTasks('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should unsuspend a suspended task', async () => {
			const task = await hooks.registerTask(createTestTask());

			await hooks.suspendTask({
				id: task.id,
				namespace: task.namespace
			});

			const unsuspended = await hooks.unsuspendTask({
				id: task.id,
				namespace: task.namespace
			});

			expect(hooks.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :suspended',
				filter: {
					item: { namespace: task.namespace, id: task.id }
				},
				updateExpression: 'SET #status = :active'
			});

			expect(unsuspended?.status).toEqual('ACTIVE');
		});

		it('should not unsuspend a non-suspended task', async () => {
			const task = await hooks.registerTask(createTestTask());

			// Should fail because task is ACTIVE, not SUSPENDED
			await expect(
				hooks.unsuspendTask({
					id: task.id,
					namespace: task.namespace
				})
			).rejects.toThrow(ConditionalCheckFailedException);
		});

		it('should return null for non-existent task', async () => {
			const unsuspended = await hooks.unsuspendTask({
				id: 'non-existent',
				namespace: 'spec'
			});

			expect(unsuspended).toBeNull();
			expect(hooks.db.tasks.update).not.toHaveBeenCalled();
		});
	});

	describe('uuid', () => {
		it('should generate a UUID with prefix', () => {
			// @ts-expect-error
			const uuid = hooks.uuid('test');

			expect(uuid).toMatch(/^test#[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});

		it('should generate a UUID without prefix', () => {
			// @ts-expect-error
			const uuid = hooks.uuid();

			expect(uuid).toMatch(/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});
	});
});
