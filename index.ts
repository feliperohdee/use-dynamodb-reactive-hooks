import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAll } from 'use-async-helpers';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import Dynamodb, { concatConditionExpression } from 'use-dynamodb';
import qs from 'use-qs';
import z from 'zod';
import zDefault from 'zod-default-instance';

const DEFAULT_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;

const schedulerTaskMethod = z.enum(['DELETE', 'GET', 'HEAD', 'POST', 'PUT']);
const schedulerTaskStatus = z.enum(['COMPLETED', 'FAILED', 'SUSPENDED', 'PENDING', 'PROCESSING']);
const schedulerTaskRepeatRule = z.object({
	interval: z.number().min(1),
	max: z.number().min(0),
	unit: z.enum(['minutes', 'hours', 'days'])
});

const schedulerTask = z.object({
	__createdAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	__updatedAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	body: z.record(z.any()).nullable(),
	errors: z.array(z.string()),
	headers: z.record(z.string()),
	id: z.string().uuid(),
	method: schedulerTaskMethod.default('GET'),
	namespace: z.string(),
	repeat: z.object({
		count: z.number(),
		enabled: z.boolean().default(false),
		parent: z.string(),
		rule: schedulerTaskRepeatRule
	}),
	response: z.object({
		body: z.string(),
		headers: z.record(z.string()),
		status: z.number()
	}),
	retries: z.object({
		count: z.number(),
		last: z.string().datetime().nullable(),
		max: z.number().default(3)
	}),
	schedule: z.string().datetime(),
	status: schedulerTaskStatus.default('PENDING'),
	url: z.string().url()
});

const schedulerTaskInput = schedulerTask
	.omit({
		__createdAt: true,
		__updatedAt: true,
		errors: true,
		id: true,
		response: true,
		retries: true,
		status: true
	})
	.extend({
		body: schedulerTask.shape.body.optional(),
		headers: schedulerTask.shape.headers.optional(),
		repeat: schedulerTask.shape.repeat
			.omit({
				count: true,
				parent: true
			})
			.optional(),
		retries: schedulerTask.shape.retries
			.omit({
				count: true,
				last: true
			})
			.optional()
	});

const schedulerDeleteInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const schedulerFetchInput = z
	.object({
		chunkLimit: z.number().min(1).optional(),
		desc: z.boolean().default(false),
		from: z.string().datetime({ offset: true }).optional(),
		id: z.string().optional(),
		limit: z.number().min(1).default(100),
		namespace: z.string(),
		onChunk: z
			.function()
			.args(
				z.object({
					count: z.number(),
					items: z.array(schedulerTask)
				})
			)
			.returns(z.promise(z.void()))
			.optional(),
		startKey: z.record(z.any()).nullable().default(null),
		status: schedulerTaskStatus.nullable().optional(),
		to: z.string().datetime({ offset: true }).optional()
	})
	.refine(
		data => {
			if (_.isNil(data.onChunk)) {
				return data.limit <= 1000;
			}

			return true;
		},
		{
			message: 'Number must be less than or equal to 1000',
			path: ['limit']
		}
	);

const schedulerGetInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const schedulerTriggerDryrunInput = z.object({
	date: z.string().datetime({ offset: true }).nullable().optional(),
	id: z.string().optional(),
	limit: z.number().min(1).default(100),
	namespace: z.string().optional()
});

namespace Scheduler {
	export type ConstructorOptions = {
		accessKeyId: string;
		concurrency?: number;
		createTable?: boolean;
		idPrefix?: string;
		region: string;
		secretAccessKey: string;
		tableName: string;
	};

	export type DeleteInput = z.input<typeof schedulerDeleteInput>;
	export type FetchInput = z.input<typeof schedulerFetchInput>;
	export type GetInput = z.input<typeof schedulerGetInput>;
	export type Task = z.infer<typeof schedulerTask>;
	export type TaskInput = z.input<typeof schedulerTaskInput>;
	export type TaskMethod = z.infer<typeof schedulerTaskMethod>;
	export type TaskRepeatRule = z.infer<typeof schedulerTaskRepeatRule>;
	export type TaskStatus = z.infer<typeof schedulerTaskStatus>;
	export type TriggerDryrunInput = z.input<typeof schedulerTriggerDryrunInput>;

	export type TaskFetchRequestResponse = {
		body: BodyInit | null;
		headers: Headers;
		method: TaskMethod;
		url: string;
	};
}

const taskShape = (task: Partial<Scheduler.Task | Scheduler.TaskInput>): Scheduler.Task => {
	return zDefault(schedulerTask, task);
};

class Scheduler {
	public concurrency: number;
	public db: Dynamodb<Scheduler.Task>;
	public idPrefix?: string;

	constructor(options: Scheduler.ConstructorOptions) {
		const db = new Dynamodb<Scheduler.Task>({
			accessKeyId: options.accessKeyId,
			indexes: [
				{
					name: 'status__schedule',
					partition: 'status',
					partitionType: 'S',
					sort: 'schedule',
					sortType: 'S'
				},
				{
					name: 'namespace__schedule',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'schedule',
					sortType: 'S'
				}
			],
			region: options.region,
			schema: {
				partition: 'namespace',
				sort: 'id'
			},
			secretAccessKey: options.secretAccessKey,
			table: options.tableName
		});

		if (options.createTable) {
			(async () => {
				await db.createTable();
			})();
		}

		this.concurrency = options.concurrency || DEFAULT_CONCURRENCY;
		this.db = db;
		this.idPrefix = options.idPrefix;
	}

	calculateNextSchedule(currentTime: string, rule: Scheduler.TaskRepeatRule): string {
		let current = new Date(currentTime);
		let intervalMs: number;

		switch (rule.unit) {
			case 'minutes':
				intervalMs = rule.interval * MINUTE_IN_MS;
				break;
			case 'hours':
				intervalMs = rule.interval * HOUR_IN_MS;
				break;
			case 'days':
				intervalMs = rule.interval * DAY_IN_MS;
				break;
		}

		const next = new Date(current.getTime() + intervalMs);

		return next.toISOString();
	}

	async clear(namespace: string): Promise<{ count: number }> {
		return this.db.clear(namespace);
	}

	async delete(args: Scheduler.DeleteInput): Promise<Scheduler.Task | null> {
		args = await schedulerDeleteInput.parseAsync(args);

		const res = await this.db.delete({
			filter: {
				item: {
					namespace: args.namespace,
					id: args.id
				}
			}
		});

		return res ? taskShape(res) : null;
	}

	async deleteMany(
		args: Omit<Scheduler.FetchInput, 'limit' | 'onChunk' | 'startKey'>
	): Promise<{ count: number; items: { id: string; namespace: string }[] }> {
		args = await schedulerFetchInput.parseAsync(args);

		let deleted: { id: string; namespace: string }[] = [];

		await this.fetch({
			...args,
			chunkLimit: args.chunkLimit || 100,
			limit: Infinity,
			onChunk: async ({ items }) => {
				await this.db.batchDelete(items);

				deleted = [...deleted, ...items];
			},
			startKey: null
		});

		return {
			count: _.size(deleted),
			items: deleted
		};
	}

	async fetch(args: Scheduler.FetchInput): Promise<Dynamodb.MultiResponse<Scheduler.Task, false>> {
		args = await schedulerFetchInput.parseAsync(args);

		let queryOptions: Dynamodb.QueryOptions<Scheduler.Task> = {
			attributeNames: {},
			attributeValues: {},
			filterExpression: '',
			item: { namespace: args.namespace, id: args.id },
			limit: args.limit,
			prefix: true,
			scanIndexForward: args.desc ? false : true,
			startKey: args.startKey
		};

		if (args.from && args.to) {
			queryOptions.attributeNames = {
				'#schedule': 'schedule'
			};

			queryOptions.attributeValues = {
				':from': args.from,
				':to': args.to
			};
		}

		if (args.status) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#status': 'status'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':status': args.status
			};
		}

		if (args.id) {
			if (args.chunkLimit) {
				queryOptions.chunkLimit = args.chunkLimit;
			}

			if (args.from && args.to) {
				queryOptions.filterExpression = '#schedule BETWEEN :from AND :to';
			}

			if (args.onChunk) {
				queryOptions.onChunk = args.onChunk;
			}

			if (args.status) {
				queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression!, '#status = :status');
			}

			const res = await this.db.query(queryOptions);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		}

		queryOptions = {
			attributeNames: queryOptions.attributeNames,
			attributeValues: queryOptions.attributeValues,
			filterExpression: '',
			index: 'namespace__schedule',
			item: { namespace: args.namespace },
			limit: args.limit,
			queryExpression: '',
			scanIndexForward: args.desc ? false : true,
			startKey: args.startKey
		};

		if (args.chunkLimit) {
			queryOptions.chunkLimit = args.chunkLimit;
		}

		if (args.from && args.to) {
			queryOptions.queryExpression = '#schedule BETWEEN :from AND :to';
		}

		if (args.onChunk) {
			queryOptions.onChunk = args.onChunk;
		}

		if (args.status) {
			queryOptions.filterExpression = '#status = :status';
		}

		const res = await this.db.query(queryOptions);

		return {
			...res,
			items: _.map(res.items, taskShape)
		};
	}

	async get(args: Scheduler.GetInput): Promise<Scheduler.Task | null> {
		args = await schedulerGetInput.parseAsync(args);

		const res = await this.db.get({
			item: {
				namespace: args.namespace,
				id: args.id
			}
		});

		return res ? taskShape(res) : null;
	}

	async register(args: Scheduler.TaskInput): Promise<Scheduler.Task> {
		args = await schedulerTaskInput.parseAsync(args);

		const res = await this.db.put(
			taskShape({
				...args,
				id: this.uuid()
			})
		);

		return _.omit(res, ['__ts']);
	}

	async registerNextRepetition(task: Scheduler.Task): Promise<Scheduler.Task | void> {
		if (!task.repeat?.enabled || task.status === 'FAILED') {
			return;
		}

		if (task.repeat.rule.max > 0 && task.repeat.count >= task.repeat.rule.max) {
			return;
		}

		const date = new Date().toISOString();
		const repeatCount = task.repeat.count + 1;
		const resettedTask: Scheduler.Task = taskShape({
			...task,
			id: `${task.repeat.parent || task.id}__${repeatCount}`,
			errors: [],
			repeat: {
				...task.repeat,
				count: repeatCount,
				parent: task.repeat.parent || task.id
			},
			response: {
				body: '',
				headers: {},
				status: 0
			},
			retries: {
				count: 0,
				last: null,
				max: task.retries?.max || 3
			},
			schedule: this.calculateNextSchedule(date, task.repeat.rule),
			status: 'PENDING',
			url: task.url
		});

		return taskShape(await this.db.put(resettedTask));
	}

	async suspend(args: Scheduler.GetInput): Promise<Scheduler.Task | null> {
		const task = await this.get(args);

		if (!task) {
			return null;
		}

		return taskShape(
			await this.db.update({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':pending': 'PENDING',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :pending',
				filter: {
					item: {
						namespace: args.namespace,
						id: args.id
					}
				},
				updateExpression: 'SET #status = :suspended'
			})
		);
	}

	async suspendMany(
		args: Omit<Scheduler.FetchInput, 'limit' | 'onChunk' | 'startKey'>
	): Promise<{ count: number; items: { id: string; namespace: string }[] }> {
		let suspended: { id: string; namespace: string }[] = [];

		await this.fetch({
			...args,
			chunkLimit: args.chunkLimit || 100,
			limit: Infinity,
			onChunk: async ({ items }) => {
				await Promise.all(
					_.map(items, async item => {
						try {
							await this.db.client.send(
								new UpdateCommand({
									ConditionExpression: '#status = :pending',
									ExpressionAttributeNames: {
										'#status': 'status'
									},
									ExpressionAttributeValues: {
										':pending': 'PENDING',
										':suspended': 'SUSPENDED'
									},
									Key: {
										namespace: item.namespace,
										id: item.id
									},
									TableName: this.db.table,
									UpdateExpression: 'SET #status = :suspended'
								})
							);

							suspended = [
								...suspended,
								{
									id: item.id,
									namespace: item.namespace
								}
							];
						} catch (err) {
							// suppress error
						}
					})
				);
			},
			startKey: null
		});

		return {
			count: _.size(suspended),
			items: suspended
		};
	}

	taskFetchRequest(task: Scheduler.Task): Scheduler.TaskFetchRequestResponse {
		const url = new URL(task.url);
		const headers = new Headers(task.headers);

		if (task.method === 'POST' || task.method === 'PUT') {
			if (task.body && _.size(task.body)) {
				const contentType = headers.get('content-type');

				if (_.includes(contentType, 'application/x-www-form-urlencoded')) {
					return {
						body: qs.stringify(task.body, { addQueryPrefix: false }),
						headers,
						method: task.method,
						url: task.url
					};
				} else if (_.includes(contentType, 'multipart/form-data')) {
					const formdata = new FormData();

					_.forEach(task.body, (value, key) => {
						formdata.append(key, value);
					});

					return {
						body: formdata,
						headers,
						method: task.method,
						url: task.url
					};
				} else {
					return {
						body: JSON.stringify(task.body),
						headers,
						method: task.method,
						url: task.url
					};
				}
			}

			return {
				body: null,
				headers,
				method: task.method,
				url: task.url
			};
		}

		const queryString = qs.stringify({
			...Object.fromEntries(url.searchParams.entries()),
			...task.body
		});

		return {
			body: null,
			headers,
			method: task.method,
			url: `${url.protocol}//${url.host}${url.pathname}${queryString}`
		};
	}

	async trigger(): Promise<{ processed: number; errors: number }> {
		const now = new Date().toISOString();

		let processed = 0;
		let errors = 0;

		try {
			await this.db.query({
				attributeNames: {
					'#schedule': 'schedule',
					'#status': 'status'
				},
				attributeValues: {
					':now': now,
					':pending': 'PENDING'
				},
				chunkLimit: 100,
				index: 'status__schedule',
				onChunk: async ({ items }) => {
					let promiseTasks: (() => Promise<void>)[] = [];

					for (const item of items) {
						const task = taskShape(item);
						const promiseTask = async () => {
							try {
								await this.db.update({
									attributeNames: {
										'#status': 'status'
									},
									attributeValues: {
										':pending': 'PENDING',
										':processing': 'PROCESSING'
									},
									// in case of other process already picked the task
									conditionExpression: '#status = :pending',
									filter: {
										item: {
											namespace: task.namespace,
											id: task.id
										}
									},
									updateExpression: 'SET #status = :processing'
								});

								const { body, headers, method, url } = this.taskFetchRequest(task);
								const res = await fetch(
									url,
									body && (method === 'POST' || method === 'PUT')
										? {
												body,
												headers,
												method
											}
										: {
												headers,
												method
											}
								);

								if (res.ok) {
									const taskUpdate = await this.db.update({
										attributeNames: {
											'#response': 'response',
											'#status': 'status'
										},
										attributeValues: {
											':completed': 'COMPLETED',
											':processing': 'PROCESSING',
											':response': {
												body: await res.text(),
												headers: Object.fromEntries(res.headers.entries()),
												status: res.status
											}
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

									await this.registerNextRepetition(taskUpdate);
									processed++;
								} else {
									throw new Error(`Request failed with status ${res.status}`);
								}
							} catch (err) {
								if (err instanceof ConditionalCheckFailedException) {
									return;
								}

								errors++;

								const newRetriesCount = task.retries.count + 1;
								const errorMessage = err instanceof Error ? err.message : 'Unknown error';

								if (newRetriesCount <= task.retries.max) {
									await this.db.update({
										attributeNames: {
											'#errors': 'errors',
											'#retries': 'retries',
											'#status': 'status'
										},
										attributeValues: {
											':errors': [errorMessage],
											':list': [],
											':retries': {
												count: newRetriesCount,
												last: new Date().toISOString(),
												max: task.retries.max
											},
											':pending': 'PENDING'
										},
										filter: {
											item: {
												namespace: task.namespace,
												id: task.id
											}
										},
										updateExpression:
											'SET #errors = list_append(if_not_exists(#errors, :list), :errors), #retries = :retries, #status = :pending'
									});
								} else {
									await this.db.update({
										attributeNames: {
											'#errors': 'errors',
											'#status': 'status'
										},
										attributeValues: {
											':errors': [errorMessage],
											':list': [],
											':failed': 'FAILED'
										},
										filter: {
											item: {
												namespace: task.namespace,
												id: task.id
											}
										},
										updateExpression: 'SET #errors = list_append(if_not_exists(#errors, :list), :errors), #status = :failed'
									});
								}
							}
						};

						promiseTasks = [...promiseTasks, promiseTask];
					}

					await promiseAll(promiseTasks, this.concurrency);
				},
				queryExpression: '#status = :pending AND #schedule <= :now'
			});
		} catch (err) {
			console.error('Error processing tasks:', err);
			throw err;
		}

		return { processed, errors };
	}

	async triggerDryrun(args: Scheduler.TriggerDryrunInput = {}): Promise<{
		count: number;
		items: { namespace: string; id: string; schedule: string }[];
	}> {
		args = await schedulerTriggerDryrunInput.parseAsync(args);

		const date = args.date ? new Date(args.date).toISOString() : new Date().toISOString();
		const queryOptions: Dynamodb.QueryOptions<Scheduler.Task> = {
			filterExpression: '',
			index: 'status__schedule',
			limit: args.limit,
			queryExpression: '#status = :pending AND #schedule <= :date'
		};

		queryOptions.attributeNames = {
			'#schedule': 'schedule',
			'#status': 'status'
		};

		queryOptions.attributeValues = {
			':date': date,
			':pending': 'PENDING'
		};

		if (args.namespace) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace': 'namespace'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace': args.namespace
			};

			queryOptions.filterExpression = '#namespace = :namespace';
		}

		if (args.id) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': args.id
			};

			queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression || '', 'begins_with(#id, :id)');
		}

		const res = await this.db.query(queryOptions);

		return {
			count: res.count,
			items: _.map(res.items, (task: Scheduler.Task) => {
				return {
					namespace: task.namespace,
					id: task.id,
					schedule: task.schedule
				};
			})
		};
	}

	async unsuspend(args: Scheduler.GetInput): Promise<Scheduler.Task | null> {
		const task = await this.get(args);

		if (!task) {
			return null;
		}

		return taskShape(
			await this.db.update({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':pending': 'PENDING',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :suspended',
				filter: {
					item: {
						namespace: args.namespace,
						id: args.id
					}
				},
				updateExpression: 'SET #status = :pending'
			})
		);
	}

	uuid(): string {
		return _.compact([this.idPrefix, crypto.randomUUID()]).join('#');
	}
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, taskShape };
export default Scheduler;
