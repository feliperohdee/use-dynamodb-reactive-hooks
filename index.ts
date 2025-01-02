import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAll } from 'use-async-helpers';
import Dynamodb, { concatConditionExpression } from 'use-dynamodb';
import z from 'zod';
import zDefault from 'zod-default-instance';

const DEFAULT_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;

const schedulerTaskStatus = z.enum(['COMPLETED', 'FAILED', 'SUSPENDED', 'PENDING', 'PROCESSING']);
const schedulerTaskRepeatRuleSchema = z.object({
	interval: z.number().min(1),
	max: z.number().min(0),
	unit: z.enum(['minutes', 'hours', 'days'])
});

const schedulerTaskSchema = z.object({
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
	body: z.unknown().nullable(),
	errors: z.array(z.string()),
	headers: z.record(z.string()),
	id: z.string().uuid(),
	maxRetries: z.number().default(3),
	method: z.enum(['GET', 'POST', 'PUT']).default('GET'),
	namespace: z.string(),
	repeat: z.object({
		count: z.number(),
		enabled: z.boolean().default(false),
		parent: z.string(),
		rule: schedulerTaskRepeatRuleSchema
	}),
	response: z.object({
		body: z.string(),
		headers: z.record(z.string()),
		status: z.number()
	}),
	retries: z.number(),
	schedule: z.string().datetime(),
	status: schedulerTaskStatus.default('PENDING'),
	url: z.string().url()
});

const schedulerTaskInputSchema = schedulerTaskSchema
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
		headers: schedulerTaskSchema.shape.headers.optional(),
		repeat: schedulerTaskSchema.shape.repeat
			.omit({
				count: true,
				parent: true
			})
			.optional()
	});

const schedulerDeleteInputSchema = z.object({
	id: z.string(),
	namespace: z.string()
});

const schedulerFetchInputSchema = z.object({
	desc: z.boolean().default(false),
	from: z.string().datetime({ offset: true }).optional(),
	id: z.string().optional(),
	limit: z.number().min(1).max(1000).default(100),
	namespace: z.string(),
	startKey: z.record(z.unknown()).nullable().default(null),
	status: schedulerTaskStatus.nullable().optional(),
	to: z.string().datetime({ offset: true }).optional()
});

const schedulerGetInputSchema = z.object({
	id: z.string(),
	namespace: z.string()
});

const schedulerProcessDryrunInputSchema = z.object({
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
		region: string;
		secretAccessKey: string;
		tableName: string;
	};

	export type DeleteInput = z.input<typeof schedulerDeleteInputSchema>;
	export type FetchInput = z.input<typeof schedulerFetchInputSchema>;
	export type GetInput = z.input<typeof schedulerGetInputSchema>;
	export type ProcessDryrunInput = z.input<typeof schedulerProcessDryrunInputSchema>;
	export type Task = z.infer<typeof schedulerTaskSchema>;
	export type TaskInput = z.infer<typeof schedulerTaskInputSchema>;
	export type TaskRepeatRule = z.infer<typeof schedulerTaskRepeatRuleSchema>;
	export type TaskStatus = z.infer<typeof schedulerTaskStatus>;
}

const taskShape = (task: Partial<Scheduler.Task | Scheduler.TaskInput>): Scheduler.Task => {
	return zDefault(schedulerTaskSchema, task);
};

class Scheduler {
	public concurrency: number;
	public db: Dynamodb<Scheduler.Task>;

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
	}

	$calculateNextSchedule(currentTime: string, rule: Scheduler.TaskRepeatRule): string {
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
		args = await schedulerDeleteInputSchema.parseAsync(args);

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

	async fetch(args: Scheduler.FetchInput): Promise<Dynamodb.MultiResponse<Scheduler.Task, false>> {
		args = await schedulerFetchInputSchema.parseAsync(args);

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
			if (args.from && args.to) {
				queryOptions.filterExpression = '#schedule BETWEEN :from AND :to';
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

		if (args.from && args.to) {
			queryOptions.queryExpression = '#schedule BETWEEN :from AND :to';
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
		args = await schedulerGetInputSchema.parseAsync(args);

		const res = await this.db.get({
			item: {
				namespace: args.namespace,
				id: args.id
			}
		});

		return res ? taskShape(res) : null;
	}

	async process(): Promise<{ processed: number; errors: number }> {
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

								const res = await fetch(task.url, {
									method: task.method,
									headers: task.headers,
									body: task.method === 'POST' ? JSON.stringify(task.body) : undefined
								});

								const headers: Record<string, string> = {};

								res.headers.forEach((value, key) => {
									headers[key] = value;
								});

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
												headers,
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

									await this.$scheduleNextRepetition(taskUpdate);
									processed++;
								} else {
									throw new Error(`Request failed with status ${res.status}`);
								}
							} catch (err) {
								if (err instanceof ConditionalCheckFailedException) {
									return;
								}

								errors++;

								const newRetries = (task.retries || 0) + 1;
								const errorMessage = err instanceof Error ? err.message : 'Unknown error';

								if (newRetries <= task.maxRetries) {
									await this.db.update({
										attributeNames: {
											'#errors': 'errors',
											'#retries': 'retries',
											'#status': 'status'
										},
										attributeValues: {
											':errors': [errorMessage],
											':list': [],
											':retries': newRetries,
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

	// add date and args with validation
	async processDryrun(args: Scheduler.ProcessDryrunInput = {}): Promise<{
		count: number;
		items: { namespace: string; id: string; schedule: string }[];
	}> {
		args = await schedulerProcessDryrunInputSchema.parseAsync(args);

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
			items: _.map(res.items, (item: Scheduler.Task) => {
				return {
					namespace: item.namespace,
					id: item.id,
					schedule: item.schedule
				};
			})
		};
	}

	async schedule(args: Scheduler.TaskInput): Promise<Scheduler.Task> {
		args = await schedulerTaskInputSchema.parseAsync(args);

		const res = await this.db.put(
			taskShape({
				...args,
				id: crypto.randomUUID()
			})
		);

		return _.omit(res, ['__ts']);
	}

	async $scheduleNextRepetition(task: Scheduler.Task): Promise<Scheduler.Task | void> {
		if (!task.repeat?.enabled || task.status === 'FAILED') {
			return;
		}

		if (task.repeat.rule.max > 0 && task.repeat.count >= task.repeat.rule.max) {
			return;
		}

		const now = new Date().toISOString();
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
			retries: 0,
			schedule: this.$calculateNextSchedule(now, task.repeat.rule),
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
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, taskShape };
export default Scheduler;
