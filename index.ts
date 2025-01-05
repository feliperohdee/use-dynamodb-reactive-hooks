import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAll } from 'use-async-helpers';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import Dynamodb, { concatConditionExpression, concatUpdateExpression } from 'use-dynamodb';
import HttpError from 'use-http-error';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';
import zDefault from 'zod-default-instance';

const DEFAULT_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;

const schedulerTaskStatus = z.enum(['ACTIVE', 'DONE', 'FAILED', 'SUSPENDED', 'PROCESSING']);
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
	errors: z.object({
		count: z.number(),
		firstErrorDate: z.string().datetime().nullable(),
		lastErrorDate: z.string().datetime().nullable(),
		lastError: z.string().nullable()
	}),
	execution: z.object({
		count: z.number(),
		failed: z.number(),
		firstExecutionDate: z.string().datetime().nullable(),
		firstScheduledDate: z.string().datetime().nullable(),
		lastExecutionDate: z.string().datetime().nullable(),
		lastResponseBody: z.string(),
		lastResponseHeaders: z.record(z.string()),
		lastResponseStatus: z.number(),
		successful: z.number()
	}),
	id: z.string(),
	idPrefix: z.string(),
	namespace: z.string(),
	repeat: z.object({
		interval: z.number().min(1),
		max: z.number().min(0).default(1),
		unit: z.enum(['minutes', 'hours', 'days'])
	}),
	request: z.object({
		body: z.record(z.any()).nullable(),
		headers: z.record(z.string()).nullable(),
		method: Webhooks.schema.method.default('GET'),
		url: z.string().url()
	}),
	retryLimit: z.number().min(0).default(3),
	scheduledDate: z.string().datetime(),
	status: schedulerTaskStatus.default('ACTIVE')
});

const schedulerTaskInput = schedulerTask
	.extend({
		request: schedulerTask.shape.request.partial({
			body: true,
			headers: true,
			method: true
		})
	})
	.omit({
		__createdAt: true,
		__updatedAt: true,
		errors: true,
		execution: true,
		id: true,
		status: true
	})
	.partial({
		idPrefix: true,
		repeat: true
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

const schedulerFetchLogsInput = Webhooks.schema.fetchLogsInput;
const schedulerGetInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const schedulerLog = Webhooks.schema.log;
const schedulerTriggerDryrunInput = z.object({
	date: z.string().datetime({ offset: true }).nullable().optional(),
	id: z.string().optional(),
	limit: z.number().min(1).default(100),
	namespace: z.string().optional()
});

const schema = {
	deleteInput: schedulerDeleteInput,
	fetchInput: schedulerFetchInput,
	fetchLogsInput: schedulerFetchLogsInput,
	getInput: schedulerGetInput,
	log: schedulerLog,
	task: schedulerTask,
	taskInput: schedulerTaskInput,
	taskStatus: schedulerTaskStatus,
	triggerDryrunInput: schedulerTriggerDryrunInput
};

namespace Scheduler {
	export type ConstructorOptions = {
		accessKeyId: string;
		concurrency?: number;
		createTable?: boolean;
		logsTableName: string;
		logsTtlInSeconds?: number;
		maxErrors?: number;
		region: string;
		secretAccessKey: string;
		tasksTableName: string;
	};

	export type DeleteInput = z.input<typeof schedulerDeleteInput>;
	export type FetchInput = z.input<typeof schedulerFetchInput>;
	export type FetchLogsInput = z.input<typeof schedulerFetchLogsInput>;
	export type GetInput = z.input<typeof schedulerGetInput>;
	export type Log = z.infer<typeof schedulerLog>;
	export type Task = z.infer<typeof schedulerTask>;
	export type TaskInput = z.input<typeof schedulerTaskInput>;
	export type TaskStatus = z.infer<typeof schedulerTaskStatus>;
	export type TriggerDryrunInput = z.input<typeof schedulerTriggerDryrunInput>;
}

const taskShape = (task: Partial<Scheduler.Task | Scheduler.TaskInput>): Scheduler.Task => {
	return zDefault(schedulerTask, task);
};

class Scheduler {
	public static schema = schema;

	public concurrency: number;
	public db: { tasks: Dynamodb<Scheduler.Task> };
	public maxErrors: number;
	public webhooks: Webhooks;

	constructor(options: Scheduler.ConstructorOptions) {
		const tasks = new Dynamodb<Scheduler.Task>({
			accessKeyId: options.accessKeyId,
			indexes: [
				{
					name: 'status-scheduled-date',
					partition: 'status',
					partitionType: 'S',
					sort: 'scheduledDate',
					sortType: 'S'
				},
				{
					name: 'namespace-scheduled-date',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'scheduledDate',
					sortType: 'S'
				}
			],
			region: options.region,
			schema: {
				partition: 'namespace',
				sort: 'id'
			},
			secretAccessKey: options.secretAccessKey,
			table: options.tasksTableName
		});

		const webhooks = new Webhooks({
			accessKeyId: options.accessKeyId,
			createTable: options.createTable,
			region: options.region,
			secretAccessKey: options.secretAccessKey,
			tableName: options.logsTableName,
			ttlInSeconds: options.logsTtlInSeconds
		});

		if (options.createTable) {
			(async () => {
				await tasks.createTable();
			})();
		}

		this.concurrency = options.concurrency || DEFAULT_CONCURRENCY;
		this.db = { tasks };
		this.maxErrors = options.maxErrors || 5;
		this.webhooks = webhooks;
	}

	calculateNextSchedule(currentTime: string, rule: Scheduler.Task['repeat']): string {
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
		return this.db.tasks.clear(namespace);
	}

	async delete(args: Scheduler.DeleteInput): Promise<Scheduler.Task | null> {
		args = await schedulerDeleteInput.parseAsync(args);

		const res = await this.db.tasks.delete({
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
				await this.db.tasks.batchDelete(items);

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
				'#scheduledDate': 'scheduledDate'
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
				queryOptions.filterExpression = '#scheduledDate BETWEEN :from AND :to';
			}

			if (args.onChunk) {
				queryOptions.onChunk = args.onChunk;
			}

			if (args.status) {
				queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression!, '#status = :status');
			}

			const res = await this.db.tasks.query(queryOptions);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		}

		queryOptions = {
			attributeNames: queryOptions.attributeNames,
			attributeValues: queryOptions.attributeValues,
			filterExpression: '',
			index: 'namespace-scheduled-date',
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
			queryOptions.queryExpression = '#scheduledDate BETWEEN :from AND :to';
		}

		if (args.onChunk) {
			queryOptions.onChunk = args.onChunk;
		}

		if (args.status) {
			queryOptions.filterExpression = '#status = :status';
		}

		const res = await this.db.tasks.query(queryOptions);

		return {
			...res,
			items: _.map(res.items, taskShape)
		};
	}

	async fetchLogs(args: Scheduler.FetchLogsInput): Promise<Dynamodb.MultiResponse<Scheduler.Log, false>> {
		return this.webhooks.fetchLogs(args);
	}

	async get(args: Scheduler.GetInput): Promise<Scheduler.Task | null> {
		args = await schedulerGetInput.parseAsync(args);

		const res = await this.db.tasks.get({
			item: {
				namespace: args.namespace,
				id: args.id
			}
		});

		return res ? taskShape(res) : null;
	}

	async register(args: Scheduler.TaskInput): Promise<Scheduler.Task> {
		args = await schedulerTaskInput.parseAsync(args);

		const res = await this.db.tasks.put(
			taskShape({
				...args,
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: null,
					firstScheduledDate: args.scheduledDate,
					lastExecutionDate: null,
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: this.uuid(args.idPrefix)
			})
		);

		return _.omit(res, ['__ts']);
	}

	async suspend(args: Scheduler.GetInput): Promise<Scheduler.Task | null> {
		const task = await this.get(args);

		if (!task) {
			return null;
		}

		return taskShape(
			await this.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :active',
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
							await this.db.tasks.client.send(
								new UpdateCommand({
									ConditionExpression: '#status = :active',
									ExpressionAttributeNames: {
										'#status': 'status'
									},
									ExpressionAttributeValues: {
										':active': 'ACTIVE',
										':suspended': 'SUSPENDED'
									},
									Key: {
										namespace: item.namespace,
										id: item.id
									},
									TableName: this.db.tasks.table,
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

	async trigger(): Promise<{ processed: number; errors: number }> {
		const now = new Date().toISOString();
		const result = { processed: 0, errors: 0 };

		try {
			await this.db.tasks.query({
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
					':now': now,
					':active': 'ACTIVE',
					':zero': 0
				},
				chunkLimit: 100,
				filterExpression: 'attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				index: 'status-scheduled-date',
				onChunk: async ({ items }) => {
					let promiseTasks: (() => Promise<void>)[] = [];

					for (const item of items) {
						const pid = this.uuid();
						const promiseTask = async () => {
							try {
								const task = taskShape(
									await this.db.tasks.update({
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
											':pid': pid,
											':processing': 'PROCESSING',
											':zero': 0
										},
										// in case of other process already picked the task while it was being processed
										conditionExpression:
											'#status = :active AND attribute_not_exists(#pid) AND (#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
										filter: {
											item: {
												namespace: item.namespace,
												id: item.id
											}
										},
										updateExpression: 'SET #status = :processing, #pid = :pid'
									})
								);

								const { response } = await this.webhooks.trigger({
									idPrefix: task.idPrefix,
									namespace: task.namespace,
									requestBody: task.request.body,
									requestHeaders: task.request.headers,
									requestMethod: task.request.method,
									requestUrl: task.request.url,
									retryLimit: task.retryLimit
								});

								console.log(response);

								const updateOptions: Dynamodb.UpdateOptions<Scheduler.Task> = {
									attributeNames: {
										'#count': 'count',
										'#execution': 'execution',
										'#lastExecutionDate': 'lastExecutionDate',
										'#lastResponseBody': 'lastResponseBody',
										'#lastResponseHeaders': 'lastResponseHeaders',
										'#lastResponseStatus': 'lastResponseStatus',
										'#pid': 'pid',
										'#successfulOrFailed': response.ok ? 'successful' : 'failed'
									},
									attributeValues: {
										':now': now,
										':one': 1,
										':pid': pid,
										':processing': 'PROCESSING',
										':responseBody': response.body,
										':responseHeaders': response.headers,
										':responseStatus': response.status
									},
									conditionExpression: '#status = :processing AND #pid = :pid',
									filter: {
										item: {
											namespace: task.namespace,
											id: task.id
										}
									}
								};

								updateOptions.updateExpression = [
									`ADD ${['#execution.#count :one', '#execution.#successfulOrFailed :one'].join(', ')}`,
									`SET ${[
										'#execution.#lastExecutionDate = :now',
										'#execution.#lastResponseBody = :responseBody',
										'#execution.#lastResponseHeaders = :responseHeaders',
										'#execution.#lastResponseStatus = :responseStatus'
									].join(', ')}`,
									`REMOVE #pid`
								].join(' ');

								if (task.execution.firstExecutionDate === null) {
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#firstExecutionDate': 'firstExecutionDate'
									};

									updateOptions.updateExpression = concatUpdateExpression(
										updateOptions.updateExpression || '',
										'SET #execution.#firstExecutionDate = :now'
									);
								}

								const nextExecutionCount = task.execution.count + 1;
								const repeat = task.repeat.max === 0 || nextExecutionCount < task.repeat.max;

								// keep ACTIVE status if repeat is enabled
								if (repeat) {
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#scheduledDate': 'scheduledDate',
										'#status': 'status'
									};

									updateOptions.attributeValues = {
										...updateOptions.attributeValues,
										':scheduledDate': this.calculateNextSchedule(task.scheduledDate, task.repeat),
										':active': 'ACTIVE'
									};

									updateOptions.updateExpression = concatUpdateExpression(
										updateOptions.updateExpression || '',
										'SET #scheduledDate = :scheduledDate, #status = :active'
									);
								} else {
									// set DONE status if repeat is disabled
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#status': 'status'
									};

									updateOptions.attributeValues = {
										...updateOptions.attributeValues,
										':done': 'DONE'
									};

									updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :done');
								}

								await this.db.tasks.update(updateOptions);

								result.processed++;
							} catch (err) {
								if (err instanceof ConditionalCheckFailedException) {
									return;
								}

								result.errors++;

								const task = taskShape(item);
								const httpError = HttpError.wrap(err as Error);
								const updateOptions: Dynamodb.UpdateOptions<Scheduler.Task> = {
									attributeNames: {
										'#count': 'count',
										'#errors': 'errors',
										'#lastError': 'lastError',
										'#pid': 'pid'
									},
									attributeValues: {
										':error': httpError.message,
										':one': 1,
										':now': now,
										':pid': pid,
										':processing': 'PROCESSING'
									},
									conditionExpression: '#status = :processing AND #pid = :pid',
									filter: {
										item: {
											namespace: task.namespace,
											id: task.id
										}
									}
								};

								updateOptions.updateExpression = [
									`ADD ${['#errors.#count :one'].join(', ')}`,
									`SET ${['#errors.#lastError = :error'].join(', ')}`,
									`REMOVE #pid`
								].join(' ');

								if (task.errors.firstErrorDate === null) {
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#firstErrorDate': 'firstErrorDate'
									};

									updateOptions.updateExpression = concatUpdateExpression(
										updateOptions.updateExpression || '',
										'SET #errors.#firstErrorDate = :now'
									);
								}

								// set FAILED status if max errors reached
								const nextErrorsCount = task.errors.count + 1;

								if (nextErrorsCount >= this.maxErrors) {
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#status': 'status'
									};

									updateOptions.attributeValues = {
										...updateOptions.attributeValues,
										':failed': 'FAILED'
									};

									updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :failed');
								} else {
									// keep ACTIVE status if max errors not reached
									updateOptions.attributeNames = {
										...updateOptions.attributeNames,
										'#status': 'status'
									};

									updateOptions.attributeValues = {
										...updateOptions.attributeValues,
										':active': 'ACTIVE'
									};

									updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :active');
								}

								await this.db.tasks.update(updateOptions);
							}
						};

						promiseTasks = [...promiseTasks, promiseTask];
					}

					await promiseAll(promiseTasks, this.concurrency);
				},
				queryExpression: '#status = :active AND #scheduledDate <= :now'
			});
		} catch (err) {
			console.error('Error processing tasks:', err);
			throw err;
		}

		return result;
	}

	async triggerDryrun(args: Scheduler.TriggerDryrunInput = {}): Promise<{
		count: number;
		items: { namespace: string; id: string; scheduledDate: string }[];
	}> {
		args = await schedulerTriggerDryrunInput.parseAsync(args);

		const date = args.date ? new Date(args.date).toISOString() : new Date().toISOString();
		const queryOptions: Dynamodb.QueryOptions<Scheduler.Task> = {
			filterExpression: '',
			index: 'status-scheduled-date',
			limit: args.limit,
			queryExpression: '#status = :active AND #scheduledDate <= :date'
		};

		queryOptions.attributeNames = {
			'#scheduledDate': 'scheduledDate',
			'#status': 'status'
		};

		queryOptions.attributeValues = {
			':active': 'ACTIVE',
			':date': date
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

		const res = await this.db.tasks.query(queryOptions);

		return {
			count: res.count,
			items: _.map(res.items, (task: Scheduler.Task) => {
				return {
					namespace: task.namespace,
					id: task.id,
					scheduledDate: task.scheduledDate
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
			await this.db.tasks.update({
				attributeNames: { '#status': 'status' },
				attributeValues: {
					':active': 'ACTIVE',
					':suspended': 'SUSPENDED'
				},
				conditionExpression: '#status = :suspended',
				filter: {
					item: {
						namespace: args.namespace,
						id: args.id
					}
				},
				updateExpression: 'SET #status = :active'
			})
		);
	}

	uuid(idPrefix?: string): string {
		return _.compact([idPrefix, crypto.randomUUID()]).join('#');
	}
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, taskShape };
export default Scheduler;
