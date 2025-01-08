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
		request: {
			method: 'POST',
			url: 'https://httpbin.org/anything'
		},
		namespace: 'spec',
		repeat: {
			interval: 30,
			unit: 'minutes'
		},
		scheduledDate: new Date(_.now() + scheduleDelay).toISOString(),
		...options
	});
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
		await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
	});

	describe('calculateNextSchedule', () => {
		it('should calculates next time by minutes', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Hooks.Task['repeat'] = {
				interval: 30,
				max: 5,
				unit: 'minutes'
			};

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T10:30:00.000Z').toISOString());
		});

		it('should calculates next time by hours', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Hooks.Task['repeat'] = {
				interval: 2,
				max: 5,
				unit: 'hours'
			};

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T12:00:00.000Z').toISOString());
		});

		it('should calculates next time by days', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Hooks.Task['repeat'] = {
				interval: 1,
				max: 5,
				unit: 'days'
			};

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-19T10:00:00.000Z').toISOString());
		});

		it('should handle fractional intervals', () => {
			const currentTime = '2024-03-18T10:00:00.000Z';
			const repeat: Hooks.Task['repeat'] = {
				interval: 1.5,
				max: 5,
				unit: 'hours'
			};

			// @ts-expect-error
			const res = hooks.calculateNextSchedule(currentTime, repeat);

			expect(res).toEqual(new Date('2024-03-18T11:30:00.000Z').toISOString());
		});
	});

	describe('clear', () => {
		it('should clear namespace', async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);

			const res = await hooks.clear('spec');
			expect(res.count).toEqual(3);

			const remaining = await hooks.db.tasks.query({
				item: { namespace: 'spec' }
			});
			expect(remaining.count).toEqual(0);
		});
	});

	describe('delete', () => {
		afterEach(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should delete', async () => {
			const task = await hooks.registerTask(createTestTask());

			const deleted = await hooks.delete({
				id: task.id,
				namespace: task.namespace
			});

			expect(deleted).toEqual(task);

			const retrieved = await hooks.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(retrieved).toBeNull();
		});

		it('should returns null if inexistent', async () => {
			const deleted = await hooks.delete({
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
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
				eventPatternPrefix: false,
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

		beforeAll(async () => {
			tasks = await Promise.all(
				_.map(
					[
						createTestTask(0, {
							eventPattern: 'event-pattern-1'
						}),
						createTestTask(1000, {
							eventPattern: 'event-pattern-2'
						}),
						createTestTask(2000, {
							eventPattern: 'event-pattern-3'
						})
					],
					task => {
						return hooks.registerTask(task);
					}
				)
			);
		});

		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'query');
		});

		afterAll(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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

			it('should fetch by [namespace, id, eventPattern, scheduledDate, status]', async () => {
				const res = await hooks.fetch({
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
				const res = await hooks.fetch({
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
				const res = await hooks.fetch({
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
				const res = await hooks.fetch({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace': 'namespace',
						'#eventPattern': 'eventPattern',
						'#scheduledDate': 'scheduledDate'
					},
					attributeValues: {
						':namespace': 'spec',
						':eventPattern': 'event-pattern-',
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
				const res = await hooks.fetch({
					eventPattern: 'event-pattern-',
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': '__namespace__eventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__eventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'status-namespace-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND #namespace__eventPattern = :namespace__eventPattern',
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
				const res = await hooks.fetch({
					eventPattern: 'event-pattern-',
					eventPatternPrefix: true,
					namespace: 'spec',
					status: 'ACTIVE'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': '__namespace__eventPattern',
						'#status': 'status'
					},
					attributeValues: {
						':namespace__eventPattern': 'spec#event-pattern-',
						':status': 'ACTIVE'
					},
					filterExpression: '',
					index: 'status-namespace-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND begins_with(#namespace__eventPattern, :namespace__eventPattern)',
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
				const res = await hooks.fetch({
					eventPattern: 'event-pattern-',
					fromScheduledDate: '2024-03-18T10:00:00.000Z',
					namespace: 'spec',
					status: 'ACTIVE',
					toScheduledDate: '2024-03-18T10:00:00.000Z'
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#namespace__eventPattern': '__namespace__eventPattern',
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
					index: 'status-namespace-event-pattern',
					limit: 100,
					queryExpression: '#status = :status AND #namespace__eventPattern = :namespace__eventPattern',
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

	describe.skip('fetchLogs', () => {
		beforeAll(async () => {
			await Promise.all(
				_.map([createTestTask(), createTestTask(), createTestTask()], task => {
					return hooks.registerTask(task);
				})
			);
		});

		afterAll(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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

	describe('get', () => {
		let task: Hooks.Task;

		beforeAll(async () => {
			task = await hooks.registerTask(createTestTask());
		});

		afterAll(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		it('should get', async () => {
			const res = await hooks.get({
				id: task.id,
				namespace: task.namespace
			});

			expect(res).toEqual(task);
		});

		it('should returns null if inexistent', async () => {
			const res = await hooks.get({
				id: 'non-existent-id',
				namespace: 'spec'
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
				await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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

			it('should no return tasks with execution.count > repeat.max', async () => {
				await Promise.all([
					hooks.db.tasks.update({
						attributeNames: {
							'#count': 'count',
							'#execution': 'execution',
							'#max': 'max',
							'#repeat': 'repeat'
						},
						attributeValues: {
							':executionCount': 1,
							':repeatCount': 1
						},
						filter: {
							item: { namespace: 'spec', id: tasks[0].id }
						},
						updateExpression: 'SET #execution.#count = :executionCount, #repeat.#max = :repeatCount'
					}),
					hooks.db.tasks.update({
						attributeNames: {
							'#count': 'count',
							'#execution': 'execution'
						},
						attributeValues: {
							':executionCount': 1000
						},
						filter: {
							item: { namespace: 'spec', id: tasks[1].id }
						},
						updateExpression: 'SET #execution.#count = :executionCount'
					})
				]);

				// @ts-expect-error
				const res = await hooks.queryActiveTasks({
					date: new Date(),
					onChunk: vi.fn()
				});

				expect(hooks.db.tasks.query).toHaveBeenCalledWith({
					attributeNames: {
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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
							execution: {
								...tasks[1].execution,
								count: 1000
							}
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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

		describe('query by eventPattern', () => {
			beforeAll(async () => {
				tasks = await Promise.all(
					_.map(
						[
							createTestTask(0, {
								eventPattern: ''
							}),
							createTestTask(0, {
								eventPattern: 'event-pattern-1'
							}),
							createTestTask(0, {
								eventPattern: 'event-pattern-2'
							})
						],
						task => {
							return hooks.registerTask(task);
						}
					)
				);
			});

			afterAll(async () => {
				await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#namespace__eventPattern': '__namespace__eventPattern',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#status': 'status'
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
						'attribute_not_exists(#pid)',
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-namespace-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND #namespace__eventPattern = :namespace__eventPattern'
				});

				expect(res).toEqual({
					count: 1,
					items: [tasks[1]],
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#namespace__eventPattern': '__namespace__eventPattern',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#status': 'status'
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
						'attribute_not_exists(#pid)',
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
						'(#noBefore = :empty OR :now > #noBefore)',
						'(#noAfter = :empty OR :now < #noAfter)'
					].join(' AND '),
					index: 'status-namespace-event-pattern',
					limit: Infinity,
					onChunk: expect.any(Function),
					queryExpression: '#status = :active AND begins_with(#namespace__eventPattern, :namespace__eventPattern)'
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
				await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'#count': 'count',
						'#execution': 'execution',
						'#id': 'id',
						'#max': 'max',
						'#namespace': 'namespace',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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
						'#count': 'count',
						'#execution': 'execution',
						'#id': 'id',
						'#max': 'max',
						'#namespace': 'namespace',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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
				await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
						'#count': 'count',
						'#execution': 'execution',
						'#max': 'max',
						'#noAfter': 'noAfter',
						'#noBefore': 'noBefore',
						'#pid': 'pid',
						'#repeat': 'repeat',
						'#scheduledDate': 'scheduledDate',
						'#status': 'status'
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
						'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
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

	describe('registerTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'put');
		});

		afterEach(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
					request: {
						url: 'https://httpbin.org/anything'
					}
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
					request: {
						url: 'https://httpbin.org/anything'
					}
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
					request: {
						url: 'https://httpbin.org/anything'
					}
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
					request: {
						url: 'https://httpbin.org/anything'
					}
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
				request: {
					url: 'https://httpbin.org/anything'
				}
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__eventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				errors: {
					count: 0,
					firstErrorDate: '',
					lastError: '',
					lastErrorDate: '',
					lastExecutionType: ''
				},
				eventPattern: '-',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: '',
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				repeat: {
					interval: 0,
					max: 0,
					unit: 'minutes'
				},
				request: {
					body: null,
					headers: null,
					method: 'GET',
					url: 'https://httpbin.org/anything'
				},
				rescheduleOnManualExecution: true,
				retryLimit: 3,
				scheduledDate: '-',
				status: 'ACTIVE'
			});
		});

		it('should create task by [idPrefix, eventPattern, noAfter, noBefore, scheduledDate]', async () => {
			const currentYear = new Date().getFullYear();
			const res = await hooks.registerTask({
				eventPattern: 'test-event-pattern',
				idPrefix: 'test-',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T00:00:00-03:00`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				request: {
					url: 'https://httpbin.org/anything'
				},
				scheduledDate: `${currentYear + 1}-01-01T00:00:00-03:00`
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__eventPattern: 'spec#test-event-pattern',
				__updatedAt: expect.any(String),
				concurrency: false,
				errors: {
					count: 0,
					firstErrorDate: '',
					lastError: '',
					lastErrorDate: '',
					lastExecutionType: ''
				},
				eventPattern: 'test-event-pattern',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.stringMatching(/^test-/),
				idPrefix: 'test-',
				namespace: 'spec',
				noAfter: `${currentYear + 1}-01-01T03:00:00.000Z`,
				noBefore: `${currentYear + 1}-01-01T00:00:00.000Z`,
				repeat: {
					interval: 0,
					max: 0,
					unit: 'minutes'
				},
				request: {
					body: null,
					headers: null,
					method: 'GET',
					url: 'https://httpbin.org/anything'
				},
				rescheduleOnManualExecution: true,
				retryLimit: 3,
				scheduledDate: `${currentYear + 1}-01-01T03:00:00.000Z`,
				status: 'ACTIVE'
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
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
					'#count': 'count',
					'#errors': 'errors',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastExecutionType': 'lastExecutionType',
					'#pid': 'pid',
					'#status': 'status'
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
						'#errors.#lastExecutionType = :executionType',
						'#errors.#lastError = :error',
						'#errors.#lastErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#errors.#count :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__eventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				errors: {
					count: 1,
					firstErrorDate: '',
					lastErrorDate: expect.any(String),
					lastError: 'test',
					lastExecutionType: 'MANUAL'
				},
				eventPattern: '-',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				repeat: { interval: 30, max: 0, unit: 'minutes' },
				request: {
					body: null,
					headers: null,
					method: 'POST',
					url: 'https://httpbin.org/anything'
				},
				rescheduleOnManualExecution: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE'
			});
		});

		it('should set with concurrency = true', async () => {
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
					'#count': 'count',
					'#errors': 'errors',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastExecutionType': 'lastExecutionType',
					'#pid': 'pid',
					'#status': 'status'
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
						'#errors.#lastExecutionType = :executionType',
						'#errors.#lastError = :error',
						'#errors.#lastErrorDate = :now',
						'#status = :active'
					].join(', '),
					'ADD',
					['#errors.#count :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__eventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: true,
				errors: {
					count: 1,
					firstErrorDate: '',
					lastErrorDate: expect.any(String),
					lastError: 'test',
					lastExecutionType: 'MANUAL'
				},
				eventPattern: '-',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				repeat: { interval: 30, max: 0, unit: 'minutes' },
				request: {
					body: null,
					headers: null,
					method: 'POST',
					url: 'https://httpbin.org/anything'
				},
				rescheduleOnManualExecution: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'ACTIVE'
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
					'#count': 'count',
					'#errors': 'errors',
					'#lastError': 'lastError',
					'#lastErrorDate': 'lastErrorDate',
					'#lastExecutionType': 'lastExecutionType',
					'#pid': 'pid',
					'#status': 'status'
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
						'#errors.#lastExecutionType = :executionType',
						'#errors.#lastError = :error',
						'#errors.#lastErrorDate = :now',
						'#status = :maxErrorsReached'
					].join(', '),
					'ADD',
					['#errors.#count :one'].join(', '),
					'REMOVE #pid'
				].join(' ')
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__namespace__eventPattern: '-',
				__updatedAt: expect.any(String),
				concurrency: false,
				errors: {
					count: 1,
					firstErrorDate: '',
					lastErrorDate: expect.any(String),
					lastError: 'test',
					lastExecutionType: 'MANUAL'
				},
				eventPattern: '-',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: expect.any(String),
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: expect.any(String),
				idPrefix: '',
				namespace: 'spec',
				noAfter: '',
				noBefore: '',
				repeat: { interval: 30, max: 0, unit: 'minutes' },
				request: {
					body: null,
					headers: null,
					method: 'POST',
					url: 'https://httpbin.org/anything'
				},
				rescheduleOnManualExecution: true,
				retryLimit: 3,
				scheduledDate: expect.any(String),
				status: 'MAX_ERRORS_REACHED'
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
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#noAfter': 'noAfter',
					'#noBefore': 'noBefore',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
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
					'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
					'(#noBefore = :empty OR :now > #noBefore)',
					'(#noAfter = :empty OR :now < #noAfter)'
				].join(' AND '),
				filter: {
					item: { namespace: 'spec', id: task.id }
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			});

			const retrieved = await hooks.db.tasks.get({
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
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
		});

		describe.todo('executionType = MANUAL', () => {
			it('should set with response.ok = true', async () => {
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

				const res = await hooks.setTaskSuccess({
					executionType: 'MANUAL',
					pid: 'test',
					task,
					response: {
						body: 'test',
						headers: {},
						ok: true,
						status: 200
					}
				});

				expect(hooks.db.tasks.update).toHaveBeenCalledWith({
					attributeNames: {
						'#count': 'count',
						'#execution': 'execution',
						'#lastExecutionDate': 'lastExecutionDate',
						'#lastExecutionType': 'lastExecutionType',
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
							'#execution.#lastExecutionDate = :now',
							'#execution.#lastExecutionType = :executionType',
							'#execution.#lastResponseBody = :responseBody',
							'#execution.#lastResponseHeaders = :responseHeaders',
							'#execution.#lastResponseStatus = :responseStatus',
							'#scheduledDate = :scheduledDate',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#execution.#count :one', '#execution.#successfulOrFailed :one'].join(', ')}`,
						`REMOVE #pid`
					].join(' ')
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(1800000); // 30 minutes
				expect(res).toEqual({
					__createdAt: expect.any(String),
					__namespace__eventPattern: '-',
					__updatedAt: expect.any(String),
					concurrency: false,
					errors: {
						count: 0,
						firstErrorDate: '',
						lastErrorDate: '',
						lastError: '',
						lastExecutionType: ''
					},
					eventPattern: '-',
					execution: {
						count: 1,
						failed: 0,
						firstExecutionDate: '',
						firstScheduledDate: expect.any(String),
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'MANUAL',
						lastResponseBody: 'test',
						lastResponseHeaders: {},
						lastResponseStatus: 200,
						successful: 1
					},
					id: expect.any(String),
					idPrefix: '',
					namespace: 'spec',
					noAfter: '',
					noBefore: '',
					repeat: {
						interval: 30,
						max: 0,
						unit: 'minutes'
					},
					request: {
						body: null,
						headers: null,
						method: 'POST',
						url: 'https://httpbin.org/anything'
					},
					rescheduleOnManualExecution: true,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE'
				});
			});

			it('should set with response.ok = false and task.rescheduleOnManualExecution = false', async () => {
				task = await hooks.db.tasks.update({
					attributeNames: {
						'#status': 'status',
						'#pid': 'pid',
						'#rescheduleOnManualExecution': 'rescheduleOnManualExecution'
					},
					attributeValues: {
						':pid': 'test',
						':processing': 'PROCESSING',
						':rescheduleOnManualExecution': false
					},
					filter: {
						item: { namespace: 'spec', id: task.id }
					},
					updateExpression: 'SET #pid = :pid, #rescheduleOnManualExecution = :rescheduleOnManualExecution, #status = :processing'
				});
				vi.mocked(hooks.db.tasks.update).mockClear();

				const res = await hooks.setTaskSuccess({
					executionType: 'MANUAL',
					pid: 'test',
					task,
					response: {
						body: 'test',
						headers: {},
						ok: false,
						status: 200
					}
				});

				const scheduledDateDiff = new Date(res.scheduledDate).getTime() - new Date(task.scheduledDate).getTime();

				expect(scheduledDateDiff).toEqual(0);
				expect(hooks.db.tasks.update).toHaveBeenCalledWith({
					attributeNames: {
						'#count': 'count',
						'#execution': 'execution',
						'#lastExecutionDate': 'lastExecutionDate',
						'#lastExecutionType': 'lastExecutionType',
						'#lastResponseBody': 'lastResponseBody',
						'#lastResponseHeaders': 'lastResponseHeaders',
						'#lastResponseStatus': 'lastResponseStatus',
						'#pid': 'pid',
						'#status': 'status',
						'#successfulOrFailed': 'failed'
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
							'#execution.#lastExecutionDate = :now',
							'#execution.#lastExecutionType = :executionType',
							'#execution.#lastResponseBody = :responseBody',
							'#execution.#lastResponseHeaders = :responseHeaders',
							'#execution.#lastResponseStatus = :responseStatus',
							'#status = :active'
						].join(', ')}`,
						`ADD ${['#execution.#count :one', '#execution.#successfulOrFailed :one'].join(', ')}`,
						`REMOVE #pid`
					].join(' ')
				});

				expect(res).toEqual({
					__createdAt: expect.any(String),
					__namespace__eventPattern: '-',
					__updatedAt: expect.any(String),
					concurrency: false,
					errors: {
						count: 0,
						firstErrorDate: '',
						lastErrorDate: '',
						lastError: '',
						lastExecutionType: ''
					},
					eventPattern: '-',
					execution: {
						count: 1,
						failed: 1,
						firstExecutionDate: '',
						firstScheduledDate: expect.any(String),
						lastExecutionDate: expect.any(String),
						lastExecutionType: 'MANUAL',
						lastResponseBody: 'test',
						lastResponseHeaders: {},
						lastResponseStatus: 200,
						successful: 0
					},
					id: expect.any(String),
					idPrefix: '',
					namespace: 'spec',
					noAfter: '',
					noBefore: '',
					repeat: {
						interval: 30,
						max: 0,
						unit: 'minutes'
					},
					request: {
						body: null,
						headers: null,
						method: 'POST',
						url: 'https://httpbin.org/anything'
					},
					rescheduleOnManualExecution: false,
					retryLimit: 3,
					scheduledDate: expect.any(String),
					status: 'ACTIVE'
				});
			});

			it('should set with task.repeat.interval = 0', async () => {});
		});
	});

	describe('suspendTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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

	describe.todo('trigger', () => {});

	describe('unsuspendTask', () => {
		beforeEach(() => {
			vi.spyOn(hooks.db.tasks, 'update');
		});

		afterEach(async () => {
			await Promise.all([hooks.clear('spec'), hooks.webhooks.clearLogs('spec')]);
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

	describe.todo('webhookCaller', () => {});
});
