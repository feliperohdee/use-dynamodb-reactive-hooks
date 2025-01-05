import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

import Scheduler, { taskShape } from './index';

// @ts-expect-error
global.fetch = vi.fn(async (url, options) => {
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

const createTestTask = (scheduleDelay: number = 0, options?: Partial<Scheduler.Task>): Scheduler.Task => {
	return taskShape({
		request: {
			method: 'POST',
			url: 'https://httpbin.org/anything'
		},
		namespace: 'spec',
		repeat: {
			interval: 30,
			max: 1,
			unit: 'minutes'
		},
		scheduledDate: new Date(_.now() + scheduleDelay).toISOString(),
		...options
	});
};

describe('/index.ts', () => {
	let scheduler: Scheduler;

	beforeAll(() => {
		scheduler = new Scheduler({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tasksTableName: 'use-dynamodb-scheduler-tasks-spec',
			logsTableName: 'use-dynamodb-scheduler-logs-spec'
		});
	});

	beforeEach(() => {
		scheduler = new Scheduler({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tasksTableName: 'use-dynamodb-scheduler-tasks-spec',
			logsTableName: 'use-dynamodb-scheduler-logs-spec'
		});
	});

	afterAll(async () => {
		await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
	});

	describe('calculateNextSchedule', () => {
		it('should calculates next time by minutes', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Scheduler.Task['repeat'] = {
				interval: 30,
				max: 5,
				unit: 'minutes'
			};

			const res = scheduler.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T10:30:00.000Z').toISOString());
		});

		it('should calculates next time by hours', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Scheduler.Task['repeat'] = {
				interval: 2,
				max: 5,
				unit: 'hours'
			};

			const res = scheduler.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T12:00:00.000Z').toISOString());
		});

		it('should calculates next time by days', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Scheduler.Task['repeat'] = {
				interval: 1,
				max: 5,
				unit: 'days'
			};

			const res = scheduler.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-19T10:00:00.000Z').toISOString());
		});

		it('should handle fractional intervals', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Scheduler.Task['repeat'] = {
				interval: 1.5,
				max: 5,
				unit: 'hours'
			};

			const res = scheduler.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T11:30:00.000Z').toISOString());
		});
	});

	describe('clear', () => {
		it('should clear namespace', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);

			const res = await scheduler.clear('spec');
			expect(res.count).toEqual(3);

			const remaining = await scheduler.db.tasks.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('delete', () => {
		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should delete', async () => {
			const task = await scheduler.register(createTestTask());

			const deleted = await scheduler.delete({
				id: task.id,
				namespace: task.namespace
			});

			expect(deleted).toEqual(task);

			const retrieved = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(retrieved).toBeNull();
		});

		it('should returns null if inexistent', async () => {
			const deleted = await scheduler.delete({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(deleted).toBeNull();
		});
	});

	describe('deleteMany', () => {
		beforeEach(() => {
			vi.spyOn(scheduler, 'fetch');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should delete many', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);

			const res = await scheduler.deleteMany({
				namespace: 'spec'
			});

			expect(scheduler.fetch).toHaveBeenCalledWith({
				chunkLimit: 100,
				desc: false,
				limit: Infinity,
				namespace: 'spec',
				onChunk: expect.any(Function),
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: res.items
			});

			const retrieved = await scheduler.fetch({
				namespace: 'spec'
			});

			expect(retrieved.count).toEqual(0);
		});
	});

	describe('fetch', () => {
		let tasks: Scheduler.Task[];

		beforeAll(async () => {
			tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(2000), createTestTask(3000)], task => {
					return scheduler.register(task);
				})
			);
		});

		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'query');
		});

		afterAll(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should fetch by [namespace]', async () => {
			const res = await scheduler.fetch({
				namespace: 'spec'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: tasks,
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] desc', async () => {
			const res = await scheduler.fetch({
				desc: true,
				namespace: 'spec'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '',
				scanIndexForward: false,
				startKey: null
			});

			expect(res).toEqual({
				count: 3,
				items: [...tasks].reverse(),
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, scheduledDate]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].scheduledDate,
				namespace: 'spec',
				to: tasks[1].scheduledDate
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#scheduledDate': 'scheduledDate'
				},
				attributeValues: {
					':from': tasks[0].scheduledDate,
					':to': tasks[1].scheduledDate
				},
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '#scheduledDate BETWEEN :from AND :to',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 2,
				items: [tasks[0], tasks[1]],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, status]', async () => {
			const res = await scheduler.fetch({
				namespace: 'spec',
				status: 'DONE'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#status': 'status'
				},
				attributeValues: {
					':status': 'DONE'
				},
				filterExpression: '#status = :status',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '',
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
			const res = await scheduler.fetch({
				from: tasks[0].scheduledDate,
				namespace: 'spec',
				status: 'DONE',
				to: tasks[1].scheduledDate
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':from': tasks[0].scheduledDate,
					':status': 'DONE',
					':to': tasks[1].scheduledDate
				},
				filterExpression: '#status = :status',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '#scheduledDate BETWEEN :from AND :to',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 0,
				items: [],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace] with chunkLimit, limit, onChunk, startKey', async () => {
			const res = await scheduler.fetch({
				chunkLimit: Infinity,
				limit: 2,
				namespace: 'spec',
				onChunk: vi.fn()
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				chunkLimit: Infinity,
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 2,
				onChunk: expect.any(Function),
				queryExpression: '',
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 2,
				items: _.take(tasks, 2),
				lastEvaluatedKey: _.pick(tasks[1], ['id', 'namespace', 'scheduledDate'])
			});

			const res2 = await scheduler.fetch({
				limit: 2,
				namespace: 'spec',
				startKey: res.lastEvaluatedKey
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: 2,
				queryExpression: '',
				scanIndexForward: true,
				startKey: res.lastEvaluatedKey
			});

			expect(res2).toEqual({
				count: 1,
				items: [tasks[2]],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, id]', async () => {
			const res = await scheduler.fetch({
				id: tasks[0].id.slice(0, 8),
				namespace: 'spec'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				item: { namespace: 'spec', id: tasks[0].id.slice(0, 8) },
				limit: 100,
				prefix: true,
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: [tasks[0]],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, id, scheduledDate]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].scheduledDate,
				id: tasks[0].id.slice(0, 8),
				namespace: 'spec',
				to: tasks[0].scheduledDate
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#scheduledDate': 'scheduledDate'
				},
				attributeValues: {
					':from': tasks[0].scheduledDate,
					':to': tasks[0].scheduledDate
				},
				filterExpression: '#scheduledDate BETWEEN :from AND :to',
				item: { namespace: 'spec', id: tasks[0].id.slice(0, 8) },
				limit: 100,
				prefix: true,
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 1,
				items: [tasks[0]],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, id, status]', async () => {
			const res = await scheduler.fetch({
				id: tasks[0].id.slice(0, 8),
				namespace: 'spec',
				status: 'DONE'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#status': 'status'
				},
				attributeValues: {
					':status': 'DONE'
				},
				filterExpression: '#status = :status',
				item: { namespace: 'spec', id: tasks[0].id.slice(0, 8) },
				limit: 100,
				prefix: true,
				scanIndexForward: true,
				startKey: null
			});

			expect(res).toEqual({
				count: 0,
				items: [],
				lastEvaluatedKey: null
			});
		});

		it('should fetch by [namespace, id, scheduledDate, status]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].scheduledDate,
				namespace: 'spec',
				id: tasks[0].id.slice(0, 8),
				status: 'DONE',
				to: tasks[0].scheduledDate
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':from': tasks[0].scheduledDate,
					':status': 'DONE',
					':to': tasks[0].scheduledDate
				},
				filterExpression: '#scheduledDate BETWEEN :from AND :to AND #status = :status',
				item: { namespace: 'spec', id: tasks[0].id.slice(0, 8) },
				limit: 100,
				prefix: true,
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

	describe('fetchLogs', () => {
		beforeAll(async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);
		});

		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'query');
		});

		afterAll(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should fetch logs', async () => {
			await scheduler.trigger();

			const res = await scheduler.fetchLogs({
				namespace: 'spec'
			});

			expect(res).toEqual({
				count: 3,
				items: expect.any(Array),
				lastEvaluatedKey: null
			});
		});
	});

	describe('get', () => {
		let task: Scheduler.Task;

		beforeAll(async () => {
			task = await scheduler.register(createTestTask());
		});

		afterAll(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should get', async () => {
			const res = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(res).toEqual(task);
		});

		it('should returns null if inexistent', async () => {
			const res = await scheduler.get({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(res).toBeNull();
		});
	});

	describe('register', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'put');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should validate args', async () => {
			const invalidInput = {
				namespace: 'spec'
			};

			try {
				await scheduler.register(invalidInput as any);

				throw new Error('Expected to throw');
			} catch (err) {
				expect(scheduler.db.tasks.put).not.toHaveBeenCalled();
				expect(err).toBeInstanceOf(Error);
			}
		});

		it('should create task', async () => {
			const scheduledDate = new Date().toISOString();
			const res = await scheduler.register({
				namespace: 'spec',
				request: {
					url: 'https://httpbin.org/anything'
				},
				scheduledDate
			});

			expect(scheduler.db.tasks.put).toHaveBeenCalledWith({
				__createdAt: expect.any(String),
				__updatedAt: expect.any(String),
				errors: {
					count: 0,
					firstErrorDate: null,
					lastError: null,
					lastErrorDate: null
				},
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: null,
					firstScheduledDate: scheduledDate,
					lastExecutionDate: null,
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				repeat: {
					interval: 1,
					max: 1,
					unit: 'minutes'
				},
				request: {
					body: null,
					headers: null,
					method: 'GET',
					url: 'https://httpbin.org/anything'
				},
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__updatedAt: expect.any(String),
				errors: {
					count: 0,
					firstErrorDate: null,
					lastError: null,
					lastErrorDate: null
				},
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: null,
					firstScheduledDate: scheduledDate,
					lastExecutionDate: null,
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				repeat: {
					interval: 1,
					max: 1,
					unit: 'minutes'
				},
				request: {
					body: null,
					headers: null,
					method: 'GET',
					url: 'https://httpbin.org/anything'
				},
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE'
			});
		});

		it('should create task with idPrefix', async () => {
			const res = await scheduler.register({
				idPrefix: 'test-',
				namespace: 'spec',
				request: {
					url: 'https://httpbin.org/anything'
				},
				scheduledDate: new Date().toISOString()
			});

			expect(res.id).toMatch(/^test-/);
		});
	});

	describe('suspend', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should suspend an active task', async () => {
			const task = await scheduler.register(createTestTask());

			const suspended = await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :active',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :suspended'
			});

			expect(suspended?.status).toBe('SUSPENDED');
		});

		it('should not suspend a non-active task', async () => {
			const task = await scheduler.register(createTestTask());

			// First suspend succeeds
			await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			// Second suspend should fail condition check
			await expect(
				scheduler.suspend({
					id: task.id,
					namespace: task.namespace
				})
			).rejects.toThrow(ConditionalCheckFailedException);
		});

		it('should return null for non-existent task', async () => {
			const suspended = await scheduler.suspend({
				id: 'non-existent',
				namespace: 'spec'
			});

			expect(suspended).toBeNull();
			expect(scheduler.db.tasks.update).not.toHaveBeenCalled();
		});
	});

	describe('suspendMany', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks.client, 'send');
			vi.spyOn(scheduler.db.tasks, 'query');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should suspend many tasks', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);

			// cause condition check to fail
			await scheduler.suspend({
				id: tasks[0].id,
				namespace: tasks[0].namespace
			});

			const res = await scheduler.suspendMany({
				namespace: 'spec'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				chunkLimit: 100,
				filterExpression: '',
				index: 'namespace-scheduled-date',
				item: { namespace: 'spec' },
				limit: Infinity,
				onChunk: expect.any(Function),
				queryExpression: '',
				scanIndexForward: true,
				startKey: null
			});

			expect(scheduler.db.tasks.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '#status = :active',
						ExpressionAttributeNames: {
							'#status': 'status'
						},
						ExpressionAttributeValues: {
							':active': 'ACTIVE',
							':suspended': 'SUSPENDED'
						},
						Key: {
							namespace: expect.any(String),
							id: expect.any(String)
						},
						TableName: expect.any(String),
						UpdateExpression: 'SET #status = :suspended'
					})
				})
			);

			expect(res).toEqual({
				count: 2,
				items: res.items
			});

			const retrieved = await scheduler.fetch({
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
		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'query');
			vi.spyOn(scheduler.db.tasks, 'update');
			vi.spyOn(scheduler.webhooks, 'trigger');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should trigger', async () => {
			const task = await scheduler.register(createTestTask());
			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':now': expect.any(String),
					':zero': 0
				},
				chunkLimit: 100,
				filterExpression: 'attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				index: 'status-scheduled-date',
				onChunk: expect.any(Function),
				queryExpression: '#status = :active AND #scheduledDate <= :now'
			});

			expect(scheduler.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: '',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: task.retryLimit
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledTimes(2);
			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':zero': 0
				},
				conditionExpression:
					'#status = :active AND attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#status': 'status',
					'#successfulOrFailed': 'successful'
				},
				attributeValues: {
					':done': 'DONE',
					':now': expect.any(String),
					':one': 1,
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':responseBody': expect.any(String),
					':responseHeaders': expect.any(Object),
					':responseStatus': 200
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression:
					'SET #execution.#lastExecutionDate = :now, #execution.#lastResponseBody = :responseBody, #execution.#lastResponseHeaders = :responseHeaders, #execution.#lastResponseStatus = :responseStatus, #execution.#firstExecutionDate = :now, #status = :done ADD #execution.#count :one, #execution.#successfulOrFailed :one REMOVE #pid'
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 1,
				failed: 0,
				firstExecutionDate: expect.any(String),
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: expect.any(String),
				lastResponseBody: '{"success":true,"url":"https://httpbin.org/anything"}',
				lastResponseHeaders: { 'content-type': 'application/json' },
				lastResponseStatus: 200,
				successful: 1
			});
			expect(retrieved?.scheduledDate).toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('DONE');
		});

		it('should trigger and register next repetition', async () => {
			const task = await scheduler.register(
				createTestTask(0, {
					repeat: {
						interval: 1,
						max: 0,
						unit: 'minutes'
					}
				})
			);

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: '',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: task.retryLimit
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledTimes(2);
			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':zero': 0
				},
				conditionExpression:
					'#status = :active AND attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#firstExecutionDate': 'firstExecutionDate',
					'#lastExecutionDate': 'lastExecutionDate',
					'#lastResponseBody': 'lastResponseBody',
					'#lastResponseHeaders': 'lastResponseHeaders',
					'#lastResponseStatus': 'lastResponseStatus',
					'#pid': 'pid',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status',
					'#successfulOrFailed': 'successful'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':now': expect.any(String),
					':one': 1,
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':responseBody': expect.any(String),
					':responseHeaders': expect.any(Object),
					':responseStatus': 200,
					':scheduledDate': expect.any(String)
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression:
					'SET #execution.#lastExecutionDate = :now, #execution.#lastResponseBody = :responseBody, #execution.#lastResponseHeaders = :responseHeaders, #execution.#lastResponseStatus = :responseStatus, #execution.#firstExecutionDate = :now, #scheduledDate = :scheduledDate, #status = :active ADD #execution.#count :one, #execution.#successfulOrFailed :one REMOVE #pid'
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 1,
				failed: 0,
				firstExecutionDate: expect.any(String),
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: expect.any(String),
				lastResponseBody: '{"success":true,"url":"https://httpbin.org/anything"}',
				lastResponseHeaders: { 'content-type': 'application/json' },
				lastResponseStatus: 200,
				successful: 1
			});
			expect(retrieved?.scheduledDate).not.toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('ACTIVE');
		});

		it('should trigger and register next repetition until max is reached', async () => {
			const task = await scheduler.register(
				createTestTask(0, {
					repeat: {
						interval: 1,
						max: 2,
						unit: 'minutes'
					}
				})
			);

			await scheduler.trigger();

			let retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			// keep same scheduled date for test purposes
			await scheduler.db.tasks.update({
				attributeNames: { '#scheduledDate': 'scheduledDate' },
				attributeValues: { ':scheduledDate': task.scheduledDate },
				filter: {
					item: { id: task.id, namespace: task.namespace }
				},
				updateExpression: 'SET #scheduledDate = :scheduledDate'
			});

			expect(retrieved!.execution.count).toEqual(1);
			expect(retrieved!.execution.failed).toEqual(0);
			expect(retrieved!.execution.firstExecutionDate).toEqual(retrieved!.execution.lastExecutionDate);
			expect(retrieved!.execution.lastResponseStatus).toEqual(200);
			expect(retrieved!.execution.successful).toEqual(1);
			expect(retrieved!.status).toEqual('ACTIVE');
			expect(scheduler.webhooks.trigger).toHaveBeenCalledOnce();

			// @ts-expect-error
			vi.mocked(scheduler.webhooks.trigger).mockResolvedValueOnce({
				response: {
					body: '{"success":false,"url":"https://httpbin.org/anything"}',
					headers: { 'content-type': 'application/json' },
					ok: false,
					status: 400
				}
			});

			await scheduler.trigger();

			retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved!.execution.count).toEqual(2);
			expect(retrieved!.execution.failed).toEqual(1);
			expect(retrieved!.execution.firstExecutionDate).not.toEqual(retrieved!.execution.lastExecutionDate);
			expect(retrieved!.execution.lastResponseStatus).toEqual(400);
			expect(retrieved!.execution.successful).toEqual(1);
			expect(retrieved!.status).toEqual('DONE');
			expect(scheduler.webhooks.trigger).toHaveBeenCalledTimes(2);
		});

		it('should not trigger if task has pid', async () => {
			const task = await scheduler.register(createTestTask());

			await scheduler.db.tasks.update({
				attributeNames: { '#pid': 'pid' },
				attributeValues: { ':pid': '123' },
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #pid = :pid'
			});
			vi.mocked(scheduler.db.tasks.update).mockClear();

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).not.toHaveBeenCalled();
			expect(scheduler.db.tasks.update).not.toHaveBeenCalled();
		});

		it('should not trigger if execution.count >= repeat.max', async () => {
			const task = await scheduler.register(createTestTask());

			await scheduler.db.tasks.update({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#repeat': 'repeat'
				},
				attributeValues: {
					':count': 1,
					':max': 1
				},
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #execution.#count = :count, #repeat.#max = :max'
			});
			vi.mocked(scheduler.db.tasks.update).mockClear();

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).not.toHaveBeenCalled();
			expect(scheduler.db.tasks.update).not.toHaveBeenCalled();
		});

		it('should not trigger suspended tasks', async () => {
			const task = await scheduler.register(createTestTask());
			await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});
			vi.mocked(scheduler.db.tasks.update).mockClear();

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).not.toHaveBeenCalled();
			expect(scheduler.db.tasks.update).not.toHaveBeenCalled();
		});

		it('should trigger with [body, headers, idPrefix, method, url]', async () => {
			const task = await scheduler.register(
				createTestTask(0, {
					idPrefix: 'test-',
					request: {
						body: { a: 1 },
						headers: { a: '1' },
						method: 'POST',
						url: 'https://httpbin.org/anything'
					}
				})
			);

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: 'test-',
				namespace: 'spec',
				requestBody: { a: 1 },
				requestHeaders: { a: '1' },
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: task.retryLimit
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 1,
				failed: 0,
				firstExecutionDate: expect.any(String),
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: expect.any(String),
				lastResponseBody: '{"success":true,"url":"https://httpbin.org/anything"}',
				lastResponseHeaders: { 'content-type': 'application/json' },
				lastResponseStatus: 200,
				successful: 1
			});
			expect(retrieved?.scheduledDate).toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('DONE');
		});

		it('should process with GMT', async () => {
			const now = new Date();
			const nowLocalString =
				now
					.toLocaleString('sv', {
						timeZone: 'America/Sao_Paulo'
					})
					.replace(' ', 'T') + '-03:00';

			const task = await scheduler.register(
				createTestTask(1000, {
					scheduledDate: new Date(nowLocalString).toISOString()
				})
			);

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).toHaveBeenCalledWith({
				idPrefix: '',
				namespace: 'spec',
				requestBody: null,
				requestHeaders: null,
				requestMethod: 'POST',
				requestUrl: 'https://httpbin.org/anything',
				retryLimit: task.retryLimit
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 1,
				failed: 0,
				firstExecutionDate: expect.any(String),
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: expect.any(String),
				lastResponseBody: '{"success":true,"url":"https://httpbin.org/anything"}',
				lastResponseHeaders: { 'content-type': 'application/json' },
				lastResponseStatus: 200,
				successful: 1
			});
			expect(retrieved?.scheduledDate).toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('DONE');
		});

		it('should handle concurrent tasks', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 3,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).toHaveBeenCalledTimes(3);
			_.forEach(tasks, () => {
				expect(scheduler.webhooks.trigger).toHaveBeenCalledWith({
					idPrefix: '',
					namespace: 'spec',
					requestBody: null,
					requestHeaders: null,
					requestMethod: 'POST',
					requestUrl: 'https://httpbin.org/anything',
					retryLimit: 3
				});
			});
		});

		it('should handle ConditionalCheckFailedException', async () => {
			vi.spyOn(scheduler.db.tasks, 'update').mockImplementationOnce(() => {
				throw new ConditionalCheckFailedException({
					$metadata: {},
					message: 'ConditionalCheckFailedException'
				});
			});

			await scheduler.register(createTestTask());

			const res = await scheduler.trigger();

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(scheduler.webhooks.trigger).not.toHaveBeenCalled();
			expect(scheduler.db.tasks.update).toHaveBeenCalledOnce();
		});

		it('should keep task ACTIVE if max errors not reached', async () => {
			vi.spyOn(scheduler.webhooks, 'trigger').mockRejectedValueOnce(new Error('Failed to fetch'));

			const task = await scheduler.register(createTestTask());

			await scheduler.trigger();

			expect(scheduler.db.tasks.update).toHaveBeenCalledTimes(2);
			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':zero': 0
				},
				conditionExpression:
					'#status = :active AND attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#errors': 'errors',
					'#lastError': 'lastError',
					'#pid': 'pid',
					'#firstErrorDate': 'firstErrorDate',
					'#status': 'status'
				},
				attributeValues: {
					':error': 'Failed to fetch',
					':one': 1,
					':now': expect.any(String),
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':active': 'ACTIVE'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: {
						namespace: 'spec',
						id: task.id
					}
				},
				updateExpression:
					'SET #errors.#lastError = :error, #errors.#firstErrorDate = :now, #status = :active ADD #errors.#count :one REMOVE #pid'
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 0,
				failed: 0,
				firstExecutionDate: null,
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: null,
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				successful: 0
			});
			expect(retrieved?.scheduledDate).toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('ACTIVE');
		});

		it('should mark task as FAILED after max errors', async () => {
			vi.spyOn(scheduler.webhooks, 'trigger').mockRejectedValueOnce(new Error('Failed to fetch'));

			scheduler.maxErrors = 1;
			const task = await scheduler.register(createTestTask());

			await scheduler.trigger();

			expect(scheduler.db.tasks.update).toHaveBeenCalledTimes(2);
			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':zero': 0
				},
				conditionExpression:
					'#status = :active AND attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: {
					'#count': 'count',
					'#errors': 'errors',
					'#lastError': 'lastError',
					'#pid': 'pid',
					'#firstErrorDate': 'firstErrorDate',
					'#status': 'status'
				},
				attributeValues: {
					':error': 'Failed to fetch',
					':one': 1,
					':now': expect.any(String),
					':pid': expect.any(String),
					':processing': 'PROCESSING',
					':failed': 'FAILED'
				},
				conditionExpression: '#status = :processing AND #pid = :pid',
				filter: {
					item: {
						namespace: 'spec',
						id: task.id
					}
				},
				updateExpression:
					'SET #errors.#lastError = :error, #errors.#firstErrorDate = :now, #status = :failed ADD #errors.#count :one REMOVE #pid'
			});

			const retrieved = await scheduler.db.tasks.get({
				item: {
					id: task.id,
					namespace: task.namespace
				}
			});

			expect(retrieved?.['pid']).toBeUndefined();
			expect(retrieved?.errors).toEqual({
				count: 1,
				firstErrorDate: expect.any(String),
				lastError: 'Failed to fetch',
				lastErrorDate: null
			});
			expect(retrieved?.execution.firstExecutionDate).toEqual(retrieved?.execution.lastExecutionDate);
			expect(retrieved?.execution).toEqual({
				count: 0,
				failed: 0,
				firstExecutionDate: null,
				firstScheduledDate: task.scheduledDate,
				lastExecutionDate: null,
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				successful: 0
			});
			expect(retrieved?.scheduledDate).toEqual(task.scheduledDate);
			expect(retrieved?.status).toEqual('FAILED');
		});
	});

	describe('triggerDryrun', () => {
		beforeAll(async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.register(task);
				})
			);
		});

		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'query');
		});

		afterAll(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should process dry run', async () => {
			const res = await scheduler.triggerDryrun();

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':date': expect.any(String),
					':active': 'ACTIVE'
				},
				index: 'status-scheduled-date',
				filterExpression: '',
				limit: 100,
				queryExpression: '#status = :active AND #scheduledDate <= :date'
			});

			expect(res.count).toEqual(3);
		});

		it('should process dry run by [namespace]', async () => {
			const res = await scheduler.triggerDryrun({ namespace: 'spec' });

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#namespace': 'namespace',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':date': expect.any(String),
					':namespace': 'spec',
					':active': 'ACTIVE'
				},
				index: 'status-scheduled-date',
				filterExpression: '#namespace = :namespace',
				limit: 100,
				queryExpression: '#status = :active AND #scheduledDate <= :date'
			});

			expect(res.count).toEqual(3);
		});

		it('should process dry run by [namespace, id, date] with limit', async () => {
			const date = new Date('2024-03-18T10:00:00.000Z').toISOString();
			const res = await scheduler.triggerDryrun({
				date,
				id: 'id',
				limit: 500,
				namespace: 'spec'
			});

			expect(scheduler.db.tasks.query).toHaveBeenCalledWith({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':date': date,
					':id': 'id',
					':namespace': 'spec',
					':active': 'ACTIVE'
				},
				index: 'status-scheduled-date',
				filterExpression: '#namespace = :namespace AND begins_with(#id, :id)',
				limit: 500,
				queryExpression: '#status = :active AND #scheduledDate <= :date'
			});

			expect(res.count).toEqual(0);
		});
	});

	describe('unsuspend', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([scheduler.clear('spec'), scheduler.webhooks.clearLogs('spec')]);
		});

		it('should unsuspend a suspended task', async () => {
			const task = await scheduler.register(createTestTask());

			await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			const unsuspended = await scheduler.unsuspend({
				id: task.id,
				namespace: task.namespace
			});

			expect(scheduler.db.tasks.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :suspended',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :active'
			});

			expect(unsuspended?.status).toBe('ACTIVE');
		});

		it('should not unsuspend a non-suspended task', async () => {
			const task = await scheduler.register(createTestTask());

			// Should fail because task is ACTIVE, not SUSPENDED
			await expect(
				scheduler.unsuspend({
					id: task.id,
					namespace: task.namespace
				})
			).rejects.toThrow(ConditionalCheckFailedException);
		});

		it('should return null for non-existent task', async () => {
			const unsuspended = await scheduler.unsuspend({
				id: 'non-existent',
				namespace: 'spec'
			});

			expect(unsuspended).toBeNull();
			expect(scheduler.db.tasks.update).not.toHaveBeenCalled();
		});
	});

	describe('uuid', () => {
		it('should generate a UUID with prefix', () => {
			const uuid = scheduler.uuid('test');

			expect(uuid).toMatch(/^test#[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});

		it('should generate a UUID without prefix', () => {
			const uuid = scheduler.uuid();

			expect(uuid).toMatch(/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i);
		});
	});
});
