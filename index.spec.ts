import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import Scheduler, { taskShape } from './index';

// @ts-expect-error
global.fetch = vi.fn(async (url, options) => {
	if (url === 'https://httpbin.org/anything') {
		return {
			headers: new Headers({
				'content-type': 'application/json'
			}),
			json: async () => {
				return {
					success: true,
					url,
					options
				};
			},
			ok: true,
			text: async () => {
				return JSON.stringify({
					success: true,
					url,
					options
				});
			},
			status: 200
		};
	}

	return {
		headers: new Headers({
			'content-type': 'application/json'
		}),
		json: async () => {
			return {
				error: 'Not Found'
			};
		},
		ok: false,
		text: async () => {
			return JSON.stringify({
				error: 'Not Found'
			});
		},
		status: 404
	};
});

const createTestTask = (scheduleDelay: number = 1000, options?: Partial<Scheduler.Task>): Scheduler.Task => {
	return taskShape({
		method: 'POST',
		namespace: 'spec',
		repeat: {
			count: 0,
			enabled: false,
			parent: '',
			rule: {
				interval: 30,
				max: 5,
				unit: 'minutes'
			}
		},
		schedule: new Date(_.now() + scheduleDelay).toISOString(),
		url: 'https://httpbin.org/anything',
		...options
	});
};

const wait = (ms: number) => {
	return new Promise(resolve => setTimeout(resolve, ms));
};

describe('/index.ts', () => {
	let scheduler: Scheduler;

	beforeAll(() => {
		scheduler = new Scheduler({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tableName: 'use-dynamodb-scheduler-spec'
		});
	});

	beforeEach(() => {
		scheduler = new Scheduler({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tableName: 'use-dynamodb-scheduler-spec'
		});
	});

	afterAll(async () => {
		await scheduler.clear('spec');
	});

	describe('$calculateNextSchedule', () => {
		it('should calculates next time by minutes', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const rule: Scheduler.TaskRepeatRule = {
				interval: 30,
				max: 5,
				unit: 'minutes'
			};

			const res = scheduler.$calculateNextSchedule(currentTime, rule);

			expect(res).toEqual(new Date('2024-03-18T10:30:00.000Z').toISOString());
		});

		it('should calculates next time by hours', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const rule: Scheduler.TaskRepeatRule = {
				interval: 2,
				max: 5,
				unit: 'hours'
			};

			const res = scheduler.$calculateNextSchedule(currentTime, rule);

			expect(res).toEqual(new Date('2024-03-18T12:00:00.000Z').toISOString());
		});

		it('should calculates next time by days', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const rule: Scheduler.TaskRepeatRule = {
				interval: 1,
				max: 5,
				unit: 'days'
			};

			const res = scheduler.$calculateNextSchedule(currentTime, rule);

			expect(res).toEqual(new Date('2024-03-19T10:00:00.000Z').toISOString());
		});

		it('should handle fractional intervals', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const rule: Scheduler.TaskRepeatRule = {
				interval: 1.5,
				max: 5,
				unit: 'hours'
			};

			const res = scheduler.$calculateNextSchedule(currentTime, rule);

			expect(res).toEqual(new Date('2024-03-18T11:30:00.000Z').toISOString());
		});
	});

	describe('clear', () => {
		it('should clear namespace', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
				})
			);

			const res = await scheduler.clear('spec');
			expect(res.count).toEqual(3);

			const remaining = await scheduler.db.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('delete', () => {
		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should delete', async () => {
			const task = await scheduler.schedule(createTestTask());

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
			vi.spyOn(scheduler.db, 'query');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should delete many', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
				})
			);

			const res = await scheduler.deleteMany({
				namespace: 'spec'
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				chunkLimit: 100,
				filterExpression: '',
				index: 'namespace__schedule',
				item: { namespace: 'spec' },
				limit: Infinity,
				onChunk: expect.any(Function),
				queryExpression: '',
				scanIndexForward: true,
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
					return scheduler.schedule(task);
				})
			);
		});

		beforeEach(() => {
			vi.spyOn(scheduler.db, 'query');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should fetch by [namespace]', async () => {
			const res = await scheduler.fetch({
				namespace: 'spec'
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace__schedule',
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

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace__schedule',
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

		it('should fetch by [namespace] with chunkLimit, limit, onChunk, startKey', async () => {
			const res = await scheduler.fetch({
				chunkLimit: Infinity,
				limit: 2,
				namespace: 'spec',
				onChunk: vi.fn()
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				chunkLimit: Infinity,
				filterExpression: '',
				index: 'namespace__schedule',
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
				lastEvaluatedKey: _.pick(tasks[1], ['id', 'namespace', 'schedule'])
			});

			const res2 = await scheduler.fetch({
				limit: 2,
				namespace: 'spec',
				startKey: res.lastEvaluatedKey
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				filterExpression: '',
				index: 'namespace__schedule',
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

			expect(scheduler.db.query).toHaveBeenCalledWith({
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

		it('should fetch by [namespace, id, schedule]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].schedule,
				id: tasks[0].id.slice(0, 8),
				namespace: 'spec',
				to: tasks[0].schedule
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule'
				},
				attributeValues: {
					':from': tasks[0].schedule,
					':to': tasks[0].schedule
				},
				filterExpression: '#schedule BETWEEN :from AND :to',
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
				status: 'COMPLETED'
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#status': 'status'
				},
				attributeValues: {
					':status': 'COMPLETED'
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

		it('should fetch by [namespace, id, schedule, status]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].schedule,
				namespace: 'spec',
				id: tasks[0].id.slice(0, 8),
				status: 'COMPLETED',
				to: tasks[0].schedule
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':from': tasks[0].schedule,
					':status': 'COMPLETED',
					':to': tasks[0].schedule
				},
				filterExpression: '#schedule BETWEEN :from AND :to AND #status = :status',
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

		it('should fetch by [namespace, schedule]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].schedule,
				namespace: 'spec',
				to: tasks[1].schedule
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule'
				},
				attributeValues: {
					':from': tasks[0].schedule,
					':to': tasks[1].schedule
				},
				filterExpression: '',
				index: 'namespace__schedule',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '#schedule BETWEEN :from AND :to',
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
				status: 'COMPLETED'
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#status': 'status'
				},
				attributeValues: {
					':status': 'COMPLETED'
				},
				filterExpression: '#status = :status',
				index: 'namespace__schedule',
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

		it('should fetch by [namespace, schedule, status]', async () => {
			const res = await scheduler.fetch({
				from: tasks[0].schedule,
				namespace: 'spec',
				status: 'COMPLETED',
				to: tasks[1].schedule
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':from': tasks[0].schedule,
					':status': 'COMPLETED',
					':to': tasks[1].schedule
				},
				filterExpression: '#status = :status',
				index: 'namespace__schedule',
				item: { namespace: 'spec' },
				limit: 100,
				queryExpression: '#schedule BETWEEN :from AND :to',
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

	describe('get', () => {
		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should get', async () => {
			const task = await scheduler.schedule(createTestTask());
			const retrieved = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(retrieved).toEqual(task);
		});

		it('should returns null if inexistent', async () => {
			const retrieved = await scheduler.get({
				id: 'non-existent-id',
				namespace: 'spec'
			});

			expect(retrieved).toBeNull();
		});
	});

	describe('process', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db, 'query');
			vi.spyOn(scheduler.db, 'update');
			vi.spyOn(scheduler, 'scheduleNextRepetition');
		});

		afterEach(async () => {
			vi.mocked(global.fetch).mockClear();
			await scheduler.clear('spec');
		});

		it('should process', async () => {
			const task = await scheduler.schedule(createTestTask());

			await wait(1100);
			const res = await scheduler.process();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':now': expect.any(String),
					':pending': 'PENDING'
				},
				chunkLimit: 100,
				index: 'status__schedule',
				onChunk: expect.any(Function),
				queryExpression: '#status = :pending AND #schedule <= :now'
			});

			expect(global.fetch).toHaveBeenCalledWith(task.url, {
				body: JSON.stringify(task.body),
				headers: task.headers,
				method: task.method
			});

			expect(scheduler.db.update).toHaveBeenCalledTimes(2);
			expect(scheduler.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#status': 'status'
				},
				attributeValues: {
					':pending': 'PENDING',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :pending',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :processing'
			});

			expect(scheduler.db.update).toHaveBeenCalledWith({
				attributeNames: {
					'#response': 'response',
					'#status': 'status'
				},
				attributeValues: {
					':response': expect.any(Object),
					':completed': 'COMPLETED',
					':processing': 'PROCESSING'
				},
				conditionExpression: '#status = :processing',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #response = :response, #status = :completed'
			});

			expect(scheduler.scheduleNextRepetition).toHaveBeenCalledOnce();
		});

		it('should process with GMT', async () => {
			const now = new Date();
			const nowLocalString =
				now
					.toLocaleString('sv', {
						timeZone: 'America/Sao_Paulo'
					})
					.replace(' ', 'T') + '-03:00';

			const task = await scheduler.schedule({
				...createTestTask(),
				schedule: new Date(nowLocalString).toISOString()
			});

			const res = await scheduler.process();

			expect(res).toEqual({
				processed: 1,
				errors: 0
			});

			expect(global.fetch).toHaveBeenCalledWith(task.url, {
				body: JSON.stringify(task.body),
				headers: task.headers,
				method: task.method
			});
		});

		it('should handle concurrent tasks', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
				})
			);
			await wait(1100);

			const res = await scheduler.process();

			expect(res).toEqual({
				processed: 3,
				errors: 0
			});

			expect(global.fetch).toHaveBeenCalledTimes(3);
			_.forEach(tasks, task => {
				expect(global.fetch).toHaveBeenCalledWith(task.url, {
					body: JSON.stringify(task.body),
					headers: task.headers,
					method: task.method
				});
			});

			expect(scheduler.scheduleNextRepetition).toHaveBeenCalledTimes(3);
		});

		it('should retry failed tasks', async () => {
			const task = await scheduler.schedule({
				...createTestTask(),
				url: 'https://invalid-url-that-will-fail.com'
			});

			await wait(1100);
			await scheduler.process();

			const updatedTask = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(updatedTask!.errors).toHaveLength(1);
			expect(updatedTask!.retries).toEqual(1);
			expect(updatedTask!.status).toEqual('PENDING');

			expect(scheduler.scheduleNextRepetition).not.toHaveBeenCalled();
		});

		it('should mark task as failed after max retries', async () => {
			const task = await scheduler.schedule({
				...createTestTask(),
				maxRetries: 0,
				url: 'https://invalid-url-that-will-fail.com'
			});

			await wait(1100);
			await scheduler.process();

			const updatedTask = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(updatedTask!.status).toEqual('FAILED');
			expect(updatedTask!.errors).toHaveLength(1);

			expect(scheduler.scheduleNextRepetition).not.toHaveBeenCalled();
		});

		it('should handle ConditionalCheckFailedException', async () => {
			vi.spyOn(scheduler.db, 'update').mockImplementationOnce(() => {
				throw new ConditionalCheckFailedException({
					$metadata: {},
					message: 'ConditionalCheckFailedException'
				});
			});

			const task = await scheduler.schedule(createTestTask());

			await wait(1100);

			const res = await scheduler.process();
			const updatedTask = await scheduler.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(scheduler.db.update).toHaveBeenCalledOnce();
			expect(updatedTask!.status).toEqual('PENDING');
			expect(scheduler.scheduleNextRepetition).not.toHaveBeenCalled();
		});

		it('should not process suspended tasks', async () => {
			const task = await scheduler.schedule(createTestTask());
			await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			await wait(1100);
			const res = await scheduler.process();

			expect(res).toEqual({
				processed: 0,
				errors: 0
			});

			expect(global.fetch).not.toHaveBeenCalled();
		});

		it('should ignore suspended tasks during processing', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
				})
			);

			// Suspend one task
			await scheduler.suspend({
				id: tasks[1].id,
				namespace: tasks[1].namespace
			});

			await wait(1100);
			const res = await scheduler.process();

			expect(res).toEqual({
				processed: 2,
				errors: 0
			});

			expect(global.fetch).toHaveBeenCalledTimes(2);

			// Verify suspended task wasn't processed
			const suspendedTask = await scheduler.get({
				id: tasks[1].id,
				namespace: tasks[1].namespace
			});
			expect(suspendedTask?.status).toBe('SUSPENDED');
		});
	});

	describe('processDryrun', () => {
		beforeAll(async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
				})
			);

			await wait(1100);
		});

		beforeEach(() => {
			vi.spyOn(scheduler.db, 'query');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should process dry run', async () => {
			const res = await scheduler.processDryrun();

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':date': expect.any(String),
					':pending': 'PENDING'
				},
				index: 'status__schedule',
				filterExpression: '',
				limit: 100,
				queryExpression: '#status = :pending AND #schedule <= :date'
			});

			expect(res.count).toEqual(3);
		});

		it('should process dry run by [namespace]', async () => {
			const res = await scheduler.processDryrun({ namespace: 'spec' });

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#namespace': 'namespace',
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':date': expect.any(String),
					':namespace': 'spec',
					':pending': 'PENDING'
				},
				index: 'status__schedule',
				filterExpression: '#namespace = :namespace',
				limit: 100,
				queryExpression: '#status = :pending AND #schedule <= :date'
			});

			expect(res.count).toEqual(3);
		});

		it('should process dry run by [namespace, id, date] with limit', async () => {
			const date = new Date('2024-03-18T10:00:00.000Z').toISOString();
			const res = await scheduler.processDryrun({
				date,
				id: 'id',
				limit: 500,
				namespace: 'spec'
			});

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {
					'#id': 'id',
					'#namespace': 'namespace',
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':date': date,
					':id': 'id',
					':namespace': 'spec',
					':pending': 'PENDING'
				},
				index: 'status__schedule',
				filterExpression: '#namespace = :namespace AND begins_with(#id, :id)',
				limit: 500,
				queryExpression: '#status = :pending AND #schedule <= :date'
			});

			expect(res.count).toEqual(0);
		});
	});

	describe('schedule', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db, 'put');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should validate args', async () => {
			const invalidInput = {
				namespace: 'spec'
			};

			try {
				await scheduler.schedule(invalidInput as any);

				throw new Error('Expected to throw');
			} catch (err) {
				expect(scheduler.db.put).not.toHaveBeenCalled();
				expect(err).toBeInstanceOf(Error);
			}
		});

		it('should create task', async () => {
			const task = createTestTask();
			const res = await scheduler.schedule(task);

			expect(scheduler.db.put).toHaveBeenCalledWith(
				expect.objectContaining({
					...task,
					__createdAt: expect.any(String),
					__updatedAt: expect.any(String),
					id: expect.any(String)
				})
			);
			expect(res).toEqual(
				expect.objectContaining({
					...task,
					__createdAt: res.__createdAt,
					__updatedAt: res.__updatedAt,
					id: res.id
				})
			);
		});
	});

	describe('scheduleNextRepetition', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db, 'put');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should schedule next task', async () => {
			const task: Scheduler.Task = {
				...createTestTask(),
				errors: ['error'],
				repeat: {
					count: 0,
					enabled: true,
					parent: '',
					rule: {
						interval: 30,
						max: 5,
						unit: 'minutes'
					}
				},
				response: {
					body: 'body',
					headers: { 'content-type': 'application/json' },
					status: 200
				},
				retries: 2,
				status: 'COMPLETED'
			};

			// ensure different __createdAt
			await wait(100);

			const nextTask = await scheduler.scheduleNextRepetition(task);

			expect(new Date(nextTask!.__createdAt).getTime()).toBeGreaterThan(new Date(task.__createdAt).getTime());
			expect(new Date(nextTask!.__updatedAt).getTime()).toBeGreaterThan(new Date(task.__updatedAt).getTime());
			expect(new Date(nextTask!.schedule).getTime()).toBeGreaterThan(new Date(task.schedule).getTime());
			expect(nextTask!.repeat.count).toBeGreaterThan(task.repeat.count);

			expect(scheduler.db.put).toHaveBeenCalledWith({
				__createdAt: expect.any(String),
				__updatedAt: expect.any(String),
				body: task.body,
				headers: task.headers,
				errors: [],
				id: `${task.id}__1`,
				maxRetries: task.maxRetries,
				method: task.method,
				namespace: task.namespace,
				repeat: {
					enabled: true,
					count: 1,
					parent: task.id,
					rule: task.repeat.rule
				},
				response: {
					body: '',
					headers: {},
					status: 0
				},
				retries: 0,
				schedule: expect.any(String),
				status: 'PENDING',
				url: task.url
			});
		});

		it(`should doesn't schedule when repeat is disabled`, async () => {
			const task: Scheduler.Task = {
				...createTestTask(),
				repeat: {
					count: 0,
					enabled: false,
					parent: '',
					rule: {
						interval: 30,
						max: 5,
						unit: 'minutes'
					}
				}
			};

			const nextTask = await scheduler.scheduleNextRepetition(task);

			expect(nextTask).toBeUndefined();
			expect(scheduler.db.put).not.toHaveBeenCalled();
		});

		it(`should doesn't schedule when max count is reached`, async () => {
			const task: Scheduler.Task = {
				...createTestTask(),
				repeat: {
					count: 5,
					enabled: true,
					parent: '',
					rule: {
						interval: 30,
						max: 5,
						unit: 'minutes'
					}
				}
			};

			const nextTask = await scheduler.scheduleNextRepetition(task);

			expect(nextTask).toBeUndefined();
			expect(scheduler.db.put).not.toHaveBeenCalled();
		});

		it('should keep parent ID for subsequent repetitions', async () => {
			const task: Scheduler.Task = {
				...createTestTask(),
				repeat: {
					count: 1,
					enabled: true,
					parent: 'original-parent',
					rule: {
						interval: 30,
						max: 5,
						unit: 'minutes'
					}
				}
			};

			await scheduler.scheduleNextRepetition(task);

			expect(scheduler.db.put).toHaveBeenCalledWith(
				expect.objectContaining({
					repeat: expect.objectContaining({
						parent: 'original-parent'
					})
				})
			);
		});
	});

	describe('suspend', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db, 'update');
		});

		afterEach(async () => {
			await scheduler.clear('spec');
		});

		it('should suspend a pending task', async () => {
			const task = await scheduler.schedule(createTestTask());

			const suspended = await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			expect(scheduler.db.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':pending': 'PENDING',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :pending',
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

		it('should not suspend a non-pending task', async () => {
			const task = await scheduler.schedule(createTestTask());

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
			expect(scheduler.db.update).not.toHaveBeenCalled();
		});
	});

	describe('suspendMany', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db.client, 'send');
			vi.spyOn(scheduler.db, 'query');
		});

		afterAll(async () => {
			await scheduler.clear('spec');
		});

		it('should suspend many tasks', async () => {
			const tasks = await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return scheduler.schedule(task);
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

			expect(scheduler.db.query).toHaveBeenCalledWith({
				attributeNames: {},
				attributeValues: {},
				chunkLimit: 100,
				filterExpression: '',
				index: 'namespace__schedule',
				item: { namespace: 'spec' },
				limit: Infinity,
				onChunk: expect.any(Function),
				queryExpression: '',
				scanIndexForward: true,
				startKey: null
			});

			expect(scheduler.db.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '#status = :pending',
						ExpressionAttributeNames: {
							'#status': 'status'
						},
						ExpressionAttributeValues: {
							':pending': 'PENDING',
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

	describe('taskFetchRequest', () => {
		describe('GET', () => {
			it('should returns adding qs', () => {
				const task = createTestTask(1000, {
					body: { a: 1, b: 2 },
					headers: { a: '1' },
					method: 'GET',
					url: 'https://httpbin.org/anything'
				});

				const res = scheduler.taskFetchRequest(task);

				expect(res.body).toBeNull();
				expect(res.headers).toEqual({ a: '1' });
				expect(res.method).toEqual('GET');
				expect(res.url).toEqual('https://httpbin.org/anything?a=1&b=2');
			});

			it('should returns merging qs', () => {
				const task = createTestTask(1000, {
					body: { b: 2 },
					headers: { a: '1' },
					method: 'GET',
					url: 'https://httpbin.org/anything?a=1&b=1'
				});

				const res = scheduler.taskFetchRequest(task);

				expect(res.body).toBeNull();
				expect(res.headers).toEqual({ a: '1' });
				expect(res.method).toEqual('GET');
				expect(res.url).toEqual('https://httpbin.org/anything?a=1&b=2');
			});

			it('should returns without qs', () => {
				const task = createTestTask(1000, {
					body: null,
					headers: { a: '1' },
					method: 'GET'
				});

				const res = scheduler.taskFetchRequest(task);

				expect(res.body).toBeNull();
				expect(res.headers).toEqual({ a: '1' });
				expect(res.method).toEqual('GET');
				expect(res.url).toEqual('https://httpbin.org/anything');
			});
		});

		describe('POST', () => {
			it('should returns with body', () => {
				const task = createTestTask(1000, {
					body: { a: 1, b: 2 },
					headers: { a: '1' },
					method: 'POST',
					url: 'https://httpbin.org/anything?a=1&b=2'
				});

				const res = scheduler.taskFetchRequest(task);

				expect(res.body).toEqual({ a: 1, b: 2 });
				expect(res.headers).toEqual({ a: '1' });
				expect(res.method).toEqual('POST');
				expect(res.url).toEqual('https://httpbin.org/anything?a=1&b=2');
			});

			it('should returns without body', () => {
				const task = createTestTask(1000, {
					body: null,
					headers: { a: '1' }
				});

				const res = scheduler.taskFetchRequest(task);

				expect(res.body).toBeNull();
				expect(res.headers).toEqual({ a: '1' });
				expect(res.method).toEqual('POST');
				expect(res.url).toEqual('https://httpbin.org/anything');
			});
		});
	});

	describe('unsuspend', () => {
		beforeEach(() => {
			vi.spyOn(scheduler.db, 'update');
		});

		afterEach(async () => {
			await scheduler.clear('spec');
		});

		it('should unsuspend a suspended task', async () => {
			const task = await scheduler.schedule(createTestTask());

			await scheduler.suspend({
				id: task.id,
				namespace: task.namespace
			});

			const unsuspended = await scheduler.unsuspend({
				id: task.id,
				namespace: task.namespace
			});

			expect(scheduler.db.update).toHaveBeenCalledWith({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':pending': 'PENDING',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :suspended',
				filter: {
					item: {
						namespace: task.namespace,
						id: task.id
					}
				},
				updateExpression: 'SET #status = :pending'
			});

			expect(unsuspended?.status).toBe('PENDING');
		});

		it('should not unsuspend a non-suspended task', async () => {
			const task = await scheduler.schedule(createTestTask());

			// Should fail because task is PENDING, not SUSPENDED
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
			expect(scheduler.db.update).not.toHaveBeenCalled();
		});
	});
});
