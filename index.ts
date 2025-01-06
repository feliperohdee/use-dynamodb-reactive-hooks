import _, { now } from 'lodash';
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

const executionType = z.enum(['MANUAL', 'SCHEDULED']);
const taskStatus = z.enum(['ACTIVE', 'MAX_ERRORS_REACHED', 'MAX_REPEAT_REACHED', 'SUSPENDED', 'PROCESSING']);
const task = z.object({
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
	concurrency: z.boolean().default(false),
	errors: z.object({
		count: z.number(),
		firstErrorDate: z.string().datetime().nullable(),
		lastErrorDate: z.string().datetime().nullable(),
		lastError: z.string().nullable(),
		lastExecutionType: executionType.nullable()
	}),
	eventPattern: z.string(),
	execution: z.object({
		count: z.number(),
		failed: z.number(),
		firstExecutionDate: z.string().datetime().nullable(),
		firstScheduledDate: z.string().datetime().nullable(),
		lastExecutionDate: z.string().datetime().nullable(),
		lastExecutionType: executionType.nullable(),
		lastResponseBody: z.string(),
		lastResponseHeaders: z.record(z.string()),
		lastResponseStatus: z.number(),
		successful: z.number()
	}),
	id: z.string(),
	idPrefix: z.string(),
	namespace: z.string(),
	notAfter: z.string().datetime().nullable(),
	notBefore: z.string().datetime().nullable(),
	repeat: z.object({
		interval: z.number().min(1),
		max: z.number().min(0).default(1),
		unit: z.enum(['minutes', 'hours', 'days'])
	}),
	request: z.object({
		body: z.record(z.any()).nullable(),
		headers: z.record(z.string()).nullable(),
		method: Webhooks.schema.requestMethod.default('GET'),
		url: z.string().url()
	}),
	rescheduleOnManualExecution: z.boolean().default(true),
	retryLimit: z.number().min(0).default(3),
	scheduledDate: z.string().datetime().nullable(),
	status: taskStatus.default('ACTIVE')
});

const taskInput = task
	.extend({
		notAfter: z.string().datetime({ offset: true }).nullable().optional(),
		notBefore: z.string().datetime({ offset: true }).nullable().optional(),
		request: task.shape.request.partial({
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
		concurrency: true,
		eventPattern: true,
		idPrefix: true,
		repeat: true,
		rescheduleOnManualExecution: true,
		scheduledDate: true
	})
	.refine(
		data => {
			return _.isNil(data.notBefore) || _.isNil(data.notAfter) || new Date(data.notBefore) < new Date(data.notAfter);
		},
		{
			message: 'notBefore cannot be equal or after notAfter',
			path: ['notBefore', 'notAfter']
		}
	)
	.refine(
		data => {
			return _.isNil(data.scheduledDate) || new Date(data.scheduledDate) > new Date();
		},
		{
			message: 'scheduledDate cannot be in the past',
			path: ['scheduledDate']
		}
	);

const callWebhookInput = z.object({
	executionType,
	pid: z.string(),
	task
});

const deleteInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const fetchInput = z
	.object({
		chunkLimit: z.number().min(1).optional(),
		desc: z.boolean().default(false),
		eventPattern: z.string().optional(),
		eventPatternPrefix: z.boolean().default(false),
		fromScheduledDate: z.string().datetime({ offset: true }).optional(),
		id: z.string().optional(),
		idPrefix: z.boolean().default(false),
		limit: z.number().min(1).default(100),
		namespace: z.string(),
		onChunk: z
			.function()
			.args(
				z.object({
					count: z.number(),
					items: z.array(task)
				})
			)
			.returns(z.promise(z.void()))
			.optional(),
		startKey: z.record(z.any()).nullable().default(null),
		status: taskStatus.nullable().optional(),
		toScheduledDate: z.string().datetime({ offset: true }).optional()
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

const fetchLogsInput = Webhooks.schema.fetchLogsInput;
const getInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const setTaskErrorInput = z.object({
	error: z.instanceof(Error),
	executionType,
	pid: z.string(),
	task
});

const setTaskLockInput = z.object({
	date: z.date(),
	id: z.string(),
	namespace: z.string(),
	pid: z.string()
});

const setTaskSuccessInput = z.object({
	executionType,
	pid: z.string(),
	response: Webhooks.schema.response,
	task
});

const log = Webhooks.schema.log;
const triggerManualInput = z.union([
	z.object({
		id: z.string().optional(),
		idPrefix: z.string().optional(),
		namespace: z.string(),
		requestBody: z.record(z.any()).optional(),
		requestHeaders: z.record(z.string()).optional(),
		requestMethod: Webhooks.schema.requestMethod.optional(),
		requestUrl: z.string().url().optional()
	}),
	z.object({
		eventPattern: z.string(),
		eventPatternPrefix: z.boolean().default(false),
		namespace: z.string(),
		requestBody: z.record(z.any()).optional(),
		requestHeaders: z.record(z.string()).optional(),
		requestMethod: Webhooks.schema.requestMethod.optional(),
		requestUrl: z.string().url().optional()
	})
]);

const schema = {
	callWebhookInput,
	deleteInput,
	fetchInput,
	fetchLogsInput,
	getInput,
	log,
	setTaskErrorInput,
	setTaskLockInput,
	setTaskSuccessInput,
	task,
	taskInput,
	taskStatus,
	triggerManualInput
};

namespace Hooks {
	export type ConstructorOptions = {
		accessKeyId: string;
		createTable?: boolean;
		logsTableName: string;
		logsTtlInSeconds?: number;
		maxConcurrency?: number;
		maxErrors?: number;
		region: string;
		secretAccessKey: string;
		tasksTableName: string;
		webhookCaller?: (input: Hooks.CallWebhookInput) => Promise<void>;
	};

	export type CallWebhookInput = z.input<typeof callWebhookInput>;
	export type DeleteInput = z.input<typeof deleteInput>;
	export type FetchInput = z.input<typeof fetchInput>;
	export type FetchLogsInput = z.input<typeof fetchLogsInput>;
	export type GetInput = z.input<typeof getInput>;
	export type Log = z.infer<typeof log>;
	export type SetTaskErrorInput = z.input<typeof setTaskErrorInput>;
	export type SetTaskLockInput = z.input<typeof setTaskLockInput>;
	export type SetTaskSuccessInput = z.input<typeof setTaskSuccessInput>;
	export type Task = z.infer<typeof task>;
	export type TaskInput = z.input<typeof taskInput>;
	export type TaskStatus = z.infer<typeof taskStatus>;
	export type TriggerManualInput = z.input<typeof triggerManualInput>;
}

const taskShape = (input: Partial<Hooks.Task | Hooks.TaskInput>): Hooks.Task => {
	return zDefault(task, input);
};

class Hooks {
	public static schema = schema;

	public db: { tasks: Dynamodb<Hooks.Task> };
	public maxConcurrency: number;
	public maxErrors: number;
	public webhookCaller?: (input: Hooks.CallWebhookInput) => Promise<void>;
	public webhooks: Webhooks;

	constructor(options: Hooks.ConstructorOptions) {
		const tasks = new Dynamodb<Hooks.Task>({
			accessKeyId: options.accessKeyId,
			indexes: [
				{
					name: 'namespace-event-pattern',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'eventPattern',
					sortType: 'S'
				},
				{
					name: 'namespace-scheduled-date',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'scheduledDate',
					sortType: 'S'
				},
				{
					name: 'namespace-status-event-pattern',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'status__eventPattern',
					sortType: 'S'
				},
				{
					name: 'status-scheduled-date',
					partition: 'status',
					partitionType: 'S',
					sort: 'scheduledDate',
					sortType: 'S'
				}
			],
			metaAttributes: {
				status__eventPattern: ['status', 'eventPattern']
			},
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

		this.db = { tasks };
		this.maxConcurrency = options.maxConcurrency || DEFAULT_CONCURRENCY;
		this.maxErrors = options.maxErrors || 5;
		this.webhookCaller = options.webhookCaller;
		this.webhooks = webhooks;
	}

	private calculateNextSchedule(currentTime: string, rule: Hooks.Task['repeat']): string {
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

	async callWebhook(input: Hooks.CallWebhookInput) {
		try {
			const args = await callWebhookInput.parseAsync(input);
			const { response } = await this.webhooks.trigger({
				idPrefix: _.compact([args.executionType, args.task.idPrefix]).join('#'),
				namespace: args.task.namespace,
				requestBody: args.task.request.body,
				requestHeaders: args.task.request.headers,
				requestMethod: args.task.request.method,
				requestUrl: args.task.request.url,
				retryLimit: args.task.retryLimit
			});

			return await this.setTaskSuccess({
				executionType: args.executionType,
				pid: args.pid,
				response,
				task: args.task
			});
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				return null;
			}

			throw err;
		}
	}

	async clear(namespace: string): Promise<{ count: number }> {
		return this.db.tasks.clear(namespace);
	}

	async delete(input: Hooks.DeleteInput): Promise<Hooks.Task | null> {
		const args = await deleteInput.parseAsync(input);
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
		args: Omit<Hooks.FetchInput, 'limit' | 'onChunk' | 'startKey'>
	): Promise<{ count: number; items: { id: string; namespace: string }[] }> {
		args = await fetchInput.parseAsync(args);

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

	async fetch(input: Hooks.FetchInput): Promise<Dynamodb.MultiResponse<Hooks.Task, false>> {
		const args = await fetchInput.parseAsync(input);
		const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
			attributeNames: {},
			attributeValues: {},
			filterExpression: '',
			item: { namespace: args.namespace },
			limit: args.limit,
			onChunk: args.onChunk,
			prefix: true,
			scanIndexForward: args.desc ? false : true,
			startKey: args.startKey
		};

		if (args.eventPattern) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#eventPattern': 'eventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':eventPattern': args.eventPattern
			};
		}

		if (args.fromScheduledDate && args.toScheduledDate) {
			const fromScheduledDate = new Date(args.fromScheduledDate);
			const toScheduledDate = new Date(args.toScheduledDate);

			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#scheduledDate': 'scheduledDate'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':fromScheduledDate': fromScheduledDate.toISOString(),
				':toScheduledDate': toScheduledDate.toISOString()
			};
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
			// QUERY BY ID INDEX
			queryOptions.queryExpression = args.idPrefix ? 'begins_with(#id, :id)' : '#id = :id';

			if (args.eventPattern) {
				queryOptions.filterExpression = args.eventPatternPrefix
					? 'begins_with(#eventPattern, :eventPattern)'
					: '#eventPattern = :eventPattern';
			}

			if (args.fromScheduledDate && args.toScheduledDate) {
				queryOptions.filterExpression = concatConditionExpression(
					queryOptions.filterExpression || '',
					'#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate'
				);
			}

			if (args.status) {
				queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression || '', '#status = :status');
			}

			const res = await this.db.tasks.query(queryOptions);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		}

		if (args.eventPattern) {
			if (args.status) {
				// QUERY BY STATUS -> EVENT_PATTERN INDEX
				queryOptions.index = 'namespace-status-event-pattern';
				queryOptions.queryExpression = args.eventPatternPrefix
					? 'begins_with(#status__#eventPattern, :status__#eventPattern)'
					: '#status__:eventPattern = :status__:eventPattern';
			} else {
				// QUERY BY EVENT_PATTERN INDEX
				queryOptions.index = 'namespace-event-pattern';
				queryOptions.queryExpression = args.eventPatternPrefix
					? 'begins_with(#eventPattern, :eventPattern)'
					: '#eventPattern = :eventPattern';
			}

			if (args.fromScheduledDate && args.toScheduledDate) {
				queryOptions.filterExpression = concatConditionExpression(
					queryOptions.filterExpression || '',
					'#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate'
				);
			}

			const res = await this.db.tasks.query(queryOptions);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		}

		if (args.fromScheduledDate) {
			// QUERY BY SCHEDULED_DATE INDEX
			queryOptions.index = 'namespace-scheduled-date';
			queryOptions.queryExpression = '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate';

			if (args.status) {
				queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression || '', '#status = :status');
			}

			const res = await this.db.tasks.query(queryOptions);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		}

		if (args.status) {
			// QUERY BY STATUS INDEX
			queryOptions.index = 'namespace-status-event-pattern';
			queryOptions.queryExpression = '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate';
		}

		const res = await this.db.tasks.query(queryOptions);

		return {
			...res,
			items: _.map(res.items, taskShape)
		};
	}

	async fetchLogs(input: Hooks.FetchLogsInput): Promise<Dynamodb.MultiResponse<Hooks.Log, false>> {
		return this.webhooks.fetchLogs(input);
	}

	async get(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const args = await getInput.parseAsync(input);
		const res = await this.db.tasks.get({
			item: {
				namespace: args.namespace,
				id: args.id
			}
		});

		return res ? taskShape(res) : null;
	}

	async registerTask(input: Hooks.TaskInput): Promise<Hooks.Task> {
		const args = await taskInput.parseAsync(input);
		const scheduledDate = args.scheduledDate ? new Date(args.scheduledDate).toISOString() : null;
		const res = await this.db.tasks.put(
			taskShape({
				...args,
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: null,
					firstScheduledDate: scheduledDate,
					lastExecutionDate: null,
					lastExecutionType: null,
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: this.uuid(args.idPrefix),
				notAfter: args.notAfter ? new Date(args.notAfter).toISOString() : null,
				notBefore: args.notBefore ? new Date(args.notBefore).toISOString() : null,
				scheduledDate
			})
		);

		return _.omit(res, ['__ts']);
	}

	private async setTaskError(input: Hooks.SetTaskErrorInput) {
		const args = await setTaskErrorInput.parseAsync(input);
		const httpError = HttpError.wrap(args.error);
		const updateOptions: Dynamodb.UpdateOptions<Hooks.Task> = {
			attributeNames: {
				'#count': 'count',
				'#errors': 'errors',
				'#lastError': 'lastError',
				'#lastExecutionType': 'lastExecutionType',
				'#pid': 'pid'
			},
			attributeValues: {
				':error': httpError.message,
				':executionType': args.executionType,
				':now': now,
				':one': 1
			},
			filter: {
				item: {
					namespace: args.task.namespace,
					id: args.task.id
				}
			}
		};

		// concurrency is disabled by default (exclusive execution)
		if (!args.task.concurrency) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#pid': 'pid',
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				':pid': args.pid,
				':processing': 'PROCESSING'
			};

			updateOptions.conditionExpression = '#status = :processing AND #pid = :pid';
		}

		updateOptions.updateExpression = [
			`ADD ${['#errors.#count :one'].join(', ')}`,
			`SET ${['#errors.#lastExecutionType = :executionType', '#errors.#lastError = :error', '#errors.#lastErrorDate = :now'].join(', ')}`,
			`REMOVE #pid`
		].join(' ');

		if (args.task.errors.firstErrorDate === null) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#firstErrorDate': 'firstErrorDate'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #errors.#firstErrorDate = :now');
		}

		// set MAX_ERRORS_REACHED status if max errors reached
		const nextErrorsCount = args.task.errors.count + 1;

		if (nextErrorsCount >= this.maxErrors) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				...updateOptions.attributeValues,
				':maxErrorsReached': 'MAX_ERRORS_REACHED'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :maxErrorsReached');
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

		return taskShape(await this.db.tasks.update(updateOptions));
	}

	private async setTaskSuccess(input: Hooks.SetTaskSuccessInput) {
		const args = await setTaskSuccessInput.parseAsync(input);
		const date = new Date();
		const updateOptions: Dynamodb.UpdateOptions<Hooks.Task> = {
			attributeNames: {
				'#count': 'count',
				'#execution': 'execution',
				'#lastExecutionDate': 'lastExecutionDate',
				'#lastExecutionType': 'lastExecutionType',
				'#lastResponseBody': 'lastResponseBody',
				'#lastResponseHeaders': 'lastResponseHeaders',
				'#lastResponseStatus': 'lastResponseStatus',
				'#successfulOrFailed': args.response.ok ? 'successful' : 'failed'
			},
			attributeValues: {
				':executionType': args.executionType,
				':now': date.toISOString(),
				':one': 1,
				':responseBody': args.response.body,
				':responseHeaders': args.response.headers,
				':responseStatus': args.response.status
			},
			filter: {
				item: {
					namespace: args.task.namespace,
					id: args.task.id
				}
			}
		};

		updateOptions.updateExpression = [
			`ADD ${['#execution.#count :one', '#execution.#successfulOrFailed :one'].join(', ')}`,
			`SET ${[
				'#execution.#lastExecutionDate = :now',
				'#execution.#lastExecutionType = :executionType',
				'#execution.#lastResponseBody = :responseBody',
				'#execution.#lastResponseHeaders = :responseHeaders',
				'#execution.#lastResponseStatus = :responseStatus'
			].join(', ')}`,
			`REMOVE #pid`
		].join(' ');

		// concurrency is disabled by default (exclusive execution)
		if (!args.task.concurrency) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#pid': 'pid',
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				...updateOptions.attributeValues,
				':pid': args.pid,
				':processing': 'PROCESSING'
			};

			updateOptions.conditionExpression = '#status = :processing AND #pid = :pid';
		}

		if (args.task.execution.firstExecutionDate === null) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#firstExecutionDate': 'firstExecutionDate'
			};

			updateOptions.updateExpression = concatUpdateExpression(
				updateOptions.updateExpression || '',
				'SET #execution.#firstExecutionDate = :now'
			);
		}

		const nextExecutionCount = args.task.execution.count + 1;
		const repeat = args.task.repeat.max === 0 || nextExecutionCount < args.task.repeat.max;

		if (repeat && args.task.scheduledDate) {
			// keep ACTIVE status if repeat is enabled with a scheduled date
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#scheduledDate': 'scheduledDate',
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				...updateOptions.attributeValues,
				':scheduledDate': this.calculateNextSchedule(args.task.scheduledDate, args.task.repeat),
				':active': 'ACTIVE'
			};

			updateOptions.updateExpression = concatUpdateExpression(
				updateOptions.updateExpression || '',
				'SET #scheduledDate = :scheduledDate, #status = :active'
			);
		} else if (repeat && !args.task.scheduledDate) {
			// keep ACTIVE status if repeat is enabled but no scheduled date is provided
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				...updateOptions.attributeValues,
				':active': 'ACTIVE'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :active');
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

		return taskShape(await this.db.tasks.update(updateOptions));
	}

	private async setTaskLock(input: Hooks.SetTaskLockInput) {
		const args = await setTaskLockInput.parseAsync(input);

		return taskShape(
			await this.db.tasks.update({
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#notAfter': 'notAfter',
					'#notBefore': 'notBefore',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':now': args.date.toISOString(),
					':null': null,
					':pid': args.pid,
					':processing': 'PROCESSING',
					':zero': 0
				},
				// in case of other process already picked the task while it was being processed
				conditionExpression: [
					'#status = :active',
					'attribute_not_exists(#pid)',
					'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
					'(#notBefore = :null OR :now > #notBefore)',
					'(#notAfter = :null OR :now < #notAfter)'
				].join(' AND '),
				filter: {
					item: {
						namespace: args.namespace,
						id: args.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			})
		);
	}

	async suspendTask(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const task = await this.get(input);

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
						namespace: input.namespace,
						id: input.id
					}
				},
				updateExpression: 'SET #status = :suspended'
			})
		);
	}

	async suspendManyTasks(
		args: Omit<Hooks.FetchInput, 'limit' | 'onChunk' | 'startKey'>
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

	async triggerManual(input: Hooks.TriggerManualInput): Promise<{ processed: number; errors: number }> {
		const args = await triggerManualInput.parseAsync(input);
		const date = new Date();
		const result = { processed: 0, errors: 0 };

		try {
			const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#notAfter': 'notAfter',
					'#notBefore': 'notBefore',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#status': 'status'
				},
				attributeValues: {
					':now': date.toISOString(),
					':null': null,
					':zero': 0
				},
				chunkLimit: 100,
				filterExpression: [
					'attribute_not_exists(#pid)',
					'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
					'(#notBefore = :null OR :now > #notBefore)',
					'(#notAfter = :null OR :now < #notAfter)'
				].join(' AND '),
				item: { namespace: args.namespace },
				onChunk: async ({ items }) => {
					let promiseTasks: (() => Promise<void>)[] = [];

					for (const item of items) {
						let task: Hooks.Task = taskShape(item);
						let pid: string = '';

						const promiseTask = async () => {
							try {
								// concurrency is disabled by default (exclusive execution)
								if (!task.concurrency) {
									// update task status to processing and set pid disallowing concurrency
									pid = this.uuid();
									task = await this.setTaskLock({
										date,
										id: task.id,
										namespace: task.namespace,
										pid
									});
								}

								if (this.webhookCaller) {
									await this.webhookCaller({
										executionType: 'MANUAL',
										pid,
										task
									});
								} else {
									await this.callWebhook({
										executionType: 'MANUAL',
										pid,
										task
									});
								}

								result.processed++;
							} catch (err) {
								if (err instanceof ConditionalCheckFailedException) {
									return;
								}

								result.errors++;

								await this.setTaskError({
									error: err as Error,
									executionType: 'MANUAL',
									pid,
									task
								});
							}
						};

						promiseTasks = [...promiseTasks, promiseTask];
					}

					await promiseAll(promiseTasks, this.maxConcurrency);
				}
			};

			if ('eventPattern' in args && args.eventPattern) {
				queryOptions.index = 'namespace-status-event-pattern';
				queryOptions.attributeNames = {
					...queryOptions.attributeNames,
					'#status__eventPattern': 'status__eventPattern'
				};

				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':status__eventPattern': `ACTIVE#${args.eventPattern}`
				};

				queryOptions.queryExpression = args.eventPatternPrefix
					? 'begins_with(#status__eventPattern, :eventPattern)'
					: '#status__eventPattern = :eventPattern';
			} else if ('id' in args && args.id) {
				queryOptions.attributeNames = {
					...queryOptions.attributeNames,
					'#id': 'id',
					'#status': 'status'
				};

				queryOptions.attributeValues = {
					':active': 'ACTIVE',
					':id': args.id
				};

				queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression || '', '#status = :active');
				queryOptions.queryExpression = args.idPrefix ? 'begins_with(#id, :id)' : '#id = :id';
			}

			await this.db.tasks.query(queryOptions);
		} catch (err) {
			console.error('Error processing tasks:', err);
			throw err;
		}

		return result;
	}

	async triggerScheduled(): Promise<{ processed: number; errors: number }> {
		const date = new Date();
		const result = { processed: 0, errors: 0 };

		try {
			const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
				attributeNames: {
					'#count': 'count',
					'#execution': 'execution',
					'#max': 'max',
					'#notAfter': 'notAfter',
					'#notBefore': 'notBefore',
					'#pid': 'pid',
					'#repeat': 'repeat',
					'#scheduledDate': 'scheduledDate',
					'#status': 'status'
				},
				attributeValues: {
					':active': 'ACTIVE',
					':now': date.toISOString(),
					':startOfTimes': '0000-00-00T00:00:00.000Z',
					':zero': 0
				},
				chunkLimit: 100,
				filterExpression: [
					'attribute_not_exists(#pid)',
					'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
					'(#notBefore = :null OR :now > #notBefore)',
					'(#notAfter = :null OR :now < #notAfter)'
				].join(' AND '),
				index: 'status-scheduled-date',
				onChunk: async ({ items }) => {
					let promiseTasks: (() => Promise<void>)[] = [];

					for (const item of items) {
						let task: Hooks.Task = taskShape(item);
						let pid: string = '';

						const promiseTask = async () => {
							try {
								// concurrency is disabled by default (exclusive execution)
								if (!task.concurrency) {
									// update task status to processing and set pid disallowing concurrency
									pid = this.uuid();
									task = await this.setTaskLock({
										date,
										id: task.id,
										namespace: task.namespace,
										pid
									});
								}

								if (this.webhookCaller) {
									await this.webhookCaller({
										executionType: 'SCHEDULED',
										pid,
										task
									});
								} else {
									await this.callWebhook({
										executionType: 'SCHEDULED',
										pid,
										task
									});
								}

								result.processed++;
							} catch (err) {
								if (err instanceof ConditionalCheckFailedException) {
									return;
								}

								result.errors++;

								await this.setTaskError({
									error: err as Error,
									executionType: 'SCHEDULED',
									pid,
									task
								});
							}
						};

						promiseTasks = [...promiseTasks, promiseTask];
					}

					await promiseAll(promiseTasks, this.maxConcurrency);
				},
				queryExpression: '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now'
			};

			await this.db.tasks.query(queryOptions);
		} catch (err) {
			console.error('Error processing tasks:', err);
			throw err;
		}

		return result;
	}

	async unsuspendTask(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const task = await this.get(input);

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
						namespace: input.namespace,
						id: input.id
					}
				},
				updateExpression: 'SET #status = :active'
			})
		);
	}

	private uuid(idPrefix?: string): string {
		return _.compact([idPrefix, crypto.randomUUID()]).join('#');
	}
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, taskShape };
export default Hooks;
