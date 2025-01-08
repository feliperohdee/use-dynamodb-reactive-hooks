import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAll } from 'use-async-helpers';
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
	__namespace__eventPattern: z.union([z.string(), z.literal('-')]),
	__updatedAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	concurrency: z.boolean().default(false),
	errors: z.object({
		count: z.number(),
		firstErrorDate: z.union([z.string().datetime(), z.literal('')]),
		lastErrorDate: z.union([z.string().datetime(), z.literal('')]),
		lastError: z.string(),
		lastExecutionType: z.union([executionType, z.literal('')])
	}),
	eventPattern: z.string(),
	execution: z.object({
		count: z.number(),
		failed: z.number(),
		firstExecutionDate: z.union([z.string().datetime(), z.literal('')]),
		firstScheduledDate: z.union([z.string().datetime(), z.literal('')]),
		lastExecutionDate: z.union([z.string().datetime(), z.literal('')]),
		lastExecutionType: z.union([executionType, z.literal('')]),
		lastResponseBody: z.string(),
		lastResponseHeaders: z.record(z.string()),
		lastResponseStatus: z.number(),
		successful: z.number()
	}),
	id: z.string(),
	idPrefix: z.string(),
	namespace: z.string(),
	noAfter: z.union([z.string().datetime(), z.literal('')]),
	noBefore: z.union([z.string().datetime(), z.literal('')]),
	repeat: z.object({
		interval: z.number().min(0),
		max: z.number().min(0).default(0),
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
	scheduledDate: z.union([z.string().datetime(), z.literal('-')]),
	status: taskStatus.default('ACTIVE')
});

const taskInput = task
	.extend({
		noAfter: z.union([
			z.literal(''),
			z
				.string()
				.datetime({ offset: true })
				.refine(date => {
					return new Date(date) > new Date(_.now() - 1000);
				}, 'noAfter cannot be in the past')
		]),
		noBefore: z.union([
			z.literal(''),
			z
				.string()
				.datetime({ offset: true })
				.refine(date => {
					return new Date(date) > new Date(_.now() - 1000);
				}, 'noBefore cannot be in the past')
		]),
		request: task.shape.request.partial({
			body: true,
			headers: true,
			method: true
		}),
		scheduledDate: z.union([
			z.literal(''),
			z
				.string()
				.datetime({ offset: true })
				.refine(date => {
					return new Date(date) > new Date(_.now() - 1000);
				}, 'scheduledDate cannot be in the past')
		])
	})
	.omit({
		__createdAt: true,
		__namespace__eventPattern: true,
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
		noAfter: true,
		noBefore: true,
		repeat: true,
		rescheduleOnManualExecution: true,
		scheduledDate: true
	})
	.refine(
		data => {
			if (data.noAfter && data.noBefore) {
				return new Date(data.noAfter) > new Date(data.noBefore);
			}

			return true;
		},
		{
			message: 'noAfter must be after noBefore',
			path: ['noAfter']
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

const queryActiveTasksInputBase = z.object({
	date: z.date(),
	onChunk: z
		.function()
		.args(
			z.object({
				count: z.number(),
				items: z.array(task)
			})
		)
		.returns(z.promise(z.void()))
});

const queryActiveTasksInput = z.union([
	queryActiveTasksInputBase.extend({
		eventPattern: z.string(),
		eventPatternPrefix: z.boolean().default(false),
		namespace: z.string()
	}),
	queryActiveTasksInputBase.extend({
		id: z.string(),
		idPrefix: z.boolean().default(false),
		namespace: z.string()
	}),
	queryActiveTasksInputBase
]);

const setTaskErrorInput = z.object({
	error: z.instanceof(Error),
	executionType,
	pid: z.string(),
	task
});

const setTaskLockInput = z.object({
	date: z.date(),
	pid: z.string(),
	task
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
		idPrefix: z.boolean().optional(),
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
	queryActiveTasksInput,
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
	export type QueryActiveTasksInput = z.input<typeof queryActiveTasksInput>;
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
				// used to fetch tasks by namespace / eventPattern
				{
					name: 'namespace-event-pattern',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'eventPattern',
					sortType: 'S'
				},
				// used to fetch tasks by namespace / scheduledDate
				{
					name: 'namespace-scheduled-date',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'scheduledDate',
					sortType: 'S'
				},
				// used to trigger tasks by status / namespace#eventPattern
				{
					name: 'status-namespace-event-pattern',
					partition: 'status',
					partitionType: 'S',
					sort: '__namespace__eventPattern',
					sortType: 'S'
				},
				// used to trigger tasks by status / scheduledDate
				{
					name: 'status-scheduled-date',
					partition: 'status',
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
			attributeNames: { '#namespace': 'namespace' },
			attributeValues: { ':namespace': args.namespace },
			filterExpression: '',
			limit: args.limit,
			queryExpression: '',
			scanIndexForward: args.desc ? false : true,
			startKey: args.startKey
		};

		const filters = {
			eventPattern: '',
			scheduledDate: '',
			status: ''
		};

		const query = async (options: Dynamodb.QueryOptions<Hooks.Task>) => {
			options.filterExpression = _.values(filters).filter(Boolean).join(' AND ');

			const res = await this.db.tasks.query(options);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		};

		if (args.chunkLimit) {
			queryOptions.chunkLimit = args.chunkLimit;
		}

		if (args.onChunk) {
			queryOptions.onChunk = args.onChunk;
		}

		// FILTER BY EVENT_PATTERN
		if (args.eventPattern) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#eventPattern': 'eventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':eventPattern': args.eventPattern
			};

			filters.eventPattern = args.eventPatternPrefix ? 'begins_with(#eventPattern, :eventPattern)' : '#eventPattern = :eventPattern';
		}

		// FILTER BY SCHEDULED_DATE
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

			filters.scheduledDate = '#scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate';
		}

		// FILTER BY STATUS
		if (args.status) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#status': 'status'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':status': args.status
			};

			filters.status = '#status = :status';
		}

		// QUERY BY ID INDEX
		if (args.id) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': args.id
			};

			queryOptions.queryExpression = `#namespace = :namespace AND ${args.idPrefix ? 'begins_with(#id, :id)' : '#id = :id'}`;

			return query(queryOptions);
		}

		// QUERY BY EVENT_PATTERN INDEX
		if (args.eventPattern && !args.status) {
			// omit [eventPattern] filter
			filters.eventPattern = '';

			queryOptions.index = 'namespace-event-pattern';
			queryOptions.queryExpression = [
				'#namespace = :namespace',
				args.eventPatternPrefix ? 'begins_with(#eventPattern, :eventPattern)' : '#eventPattern = :eventPattern'
			].join(' AND ');

			return query(queryOptions);
		}

		// QUERY BY STATUS -> EVENT_PATTERN INDEX
		if (args.eventPattern && args.status) {
			// omit [eventPattern, status] filters
			filters.eventPattern = '';
			filters.status = '';
			queryOptions.attributeNames = _.omit(queryOptions.attributeNames, ['#eventPattern', '#namespace']);
			queryOptions.attributeValues = _.omit(queryOptions.attributeValues, [':eventPattern', ':namespace']);

			queryOptions.index = 'status-namespace-event-pattern';
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace__eventPattern': '__namespace__eventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace__eventPattern': `${args.namespace}#${args.eventPattern}`
			};

			queryOptions.queryExpression = [
				'#status = :status',
				args.eventPatternPrefix
					? 'begins_with(#namespace__eventPattern, :namespace__eventPattern)'
					: '#namespace__eventPattern = :namespace__eventPattern'
			].join(' AND ');

			return query(queryOptions);
		}

		// QUERY BY SCHEDULED_DATE INDEX
		if (args.fromScheduledDate && args.toScheduledDate) {
			// omit [scheduledDate] filter
			filters.scheduledDate = '';

			queryOptions.index = 'namespace-scheduled-date';
			queryOptions.queryExpression = '#namespace = :namespace AND #scheduledDate BETWEEN :fromScheduledDate AND :toScheduledDate';

			return query(queryOptions);
		}

		queryOptions.queryExpression = '#namespace = :namespace';

		return query(queryOptions);
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

	private async queryActiveTasks(args: Hooks.QueryActiveTasksInput) {
		args = await queryActiveTasksInput.parseAsync(args);

		const query = async (options: Dynamodb.QueryOptions<Hooks.Task>) => {
			const res = await this.db.tasks.query(options);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		};

		const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
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
				':now': args.date.toISOString(),
				':zero': 0
			},
			chunkLimit: 100,
			filterExpression: [
				'attribute_not_exists(#pid)',
				'(#repeat.#max = :zero OR #execution.#count < #repeat.#max)',
				'(#noBefore = :empty OR :now > #noBefore)',
				'(#noAfter = :empty OR :now < #noAfter)'
			].join(' AND '),
			limit: Infinity,
			onChunk: args.onChunk
		};

		if ('eventPattern' in args && args.eventPattern) {
			queryOptions.index = 'status-namespace-event-pattern';
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace__eventPattern': '__namespace__eventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace__eventPattern': `${args.namespace}#${args.eventPattern}`
			};

			queryOptions.queryExpression = [
				'#status = :active',
				args.eventPatternPrefix
					? 'begins_with(#namespace__eventPattern, :namespace__eventPattern)'
					: '#namespace__eventPattern = :namespace__eventPattern'
			].join(' AND ');

			return query(queryOptions);
		}

		if ('id' in args && args.id) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id',
				'#namespace': 'namespace'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': args.id,
				':namespace': args.namespace
			};

			queryOptions.filterExpression = concatConditionExpression(queryOptions.filterExpression || '', '#status = :active');
			queryOptions.queryExpression = ['#namespace = :namespace', args.idPrefix ? 'begins_with(#id, :id)' : '#id = :id'].join(' AND ');

			return query(queryOptions);
		}

		// by scheduleTime
		queryOptions.index = 'status-scheduled-date';
		queryOptions.attributeNames = {
			...queryOptions.attributeNames,
			'#scheduledDate': 'scheduledDate'
		};

		queryOptions.attributeValues = {
			...queryOptions.attributeValues,
			':startOfTimes': '0000-00-00T00:00:00.000Z'
		};

		queryOptions.queryExpression = '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now';

		return query(queryOptions);
	}

	async registerTask(input: Hooks.TaskInput): Promise<Hooks.Task> {
		const args = await taskInput.parseAsync(input);
		const scheduledDate = args.scheduledDate ? new Date(args.scheduledDate).toISOString() : '-';
		const res = await this.db.tasks.put(
			taskShape({
				...args,
				__namespace__eventPattern: args.eventPattern ? `${args.namespace}#${args.eventPattern}` : '-',
				errors: {
					count: 0,
					firstErrorDate: '',
					lastError: '',
					lastErrorDate: '',
					lastExecutionType: ''
				},
				eventPattern: args.eventPattern || '-',
				execution: {
					count: 0,
					failed: 0,
					firstExecutionDate: '',
					firstScheduledDate: scheduledDate,
					lastExecutionDate: '',
					lastExecutionType: '',
					lastResponseBody: '',
					lastResponseHeaders: {},
					lastResponseStatus: 0,
					successful: 0
				},
				id: this.uuid(args.idPrefix),
				noAfter: args.noAfter ? new Date(args.noAfter).toISOString() : '',
				noBefore: args.noBefore ? new Date(args.noBefore).toISOString() : '',
				scheduledDate
			})
		);

		return _.omit(res, ['__ts']);
	}

	private async setTaskError(input: Hooks.SetTaskErrorInput) {
		const args = await setTaskErrorInput.parseAsync(input);
		const date = new Date();
		const httpError = HttpError.wrap(args.error);
		const updateOptions: Dynamodb.UpdateOptions<Hooks.Task> = {
			attributeNames: {
				'#count': 'count',
				'#errors': 'errors',
				'#lastError': 'lastError',
				'#lastErrorDate': 'lastErrorDate',
				'#lastExecutionType': 'lastExecutionType',
				'#pid': 'pid'
			},
			attributeValues: {
				':error': httpError.message,
				':executionType': args.executionType,
				':now': date.toISOString(),
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
				...updateOptions.attributeValues,
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

		if (this.maxErrors > 0 && nextErrorsCount >= this.maxErrors) {
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

	private async setTaskLock(input: Hooks.SetTaskLockInput) {
		const args = await setTaskLockInput.parseAsync(input);

		return taskShape(
			await this.db.tasks.update({
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
					':now': args.date.toISOString(),
					':pid': args.pid,
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
					item: {
						namespace: args.task.namespace,
						id: args.task.id
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			})
		);
	}

	async setTaskSuccess(input: Hooks.SetTaskSuccessInput) {
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

		if (
			(args.executionType === 'SCHEDULED' || (args.executionType === 'MANUAL' && args.task.rescheduleOnManualExecution)) &&
			repeat &&
			args.task.repeat.interval > 0 &&
			args.task.scheduledDate
		) {
			// keep ACTIVE status and reschedule if can repeat and have scheduled date
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
		} else if (repeat) {
			// keep ACTIVE status if can repeat
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
			// set DONE status if can't repeat
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
							await this.db.tasks.update({
								attributeNames: { '#status': 'status' },
								attributeValues: {
									':active': 'ACTIVE',
									':suspended': 'SUSPENDED'
								},
								conditionExpression: '#status = :active',
								filter: {
									item: {
										namespace: item.namespace,
										id: item.id
									}
								},
								updateExpression: 'SET #status = :suspended'
							});

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

	async trigger(input?: Hooks.TriggerManualInput): Promise<{ processed: number; errors: number }> {
		const date = new Date();
		const result = { processed: 0, errors: 0 };

		try {
			let queryActiveTasksOptions: Hooks.QueryActiveTasksInput = {
				date,
				onChunk: async () => {}
			};

			if (input) {
				const args = await triggerManualInput.parseAsync(input);

				if ('eventPattern' in args && args.eventPattern) {
					queryActiveTasksOptions = {
						...queryActiveTasksOptions,
						eventPattern: args.eventPattern,
						eventPatternPrefix: args.eventPatternPrefix,
						namespace: args.namespace
					};
				} else if ('id' in args && args.id) {
					queryActiveTasksOptions = {
						...queryActiveTasksOptions,
						id: args.id,
						idPrefix: args.idPrefix,
						namespace: args.namespace
					};
				}
			}

			const executionType = _.some(['eventPattern', 'id'], key => {
				return key in queryActiveTasksOptions;
			})
				? 'MANUAL'
				: 'SCHEDULED';

			queryActiveTasksOptions.onChunk = async ({ items }) => {
				let promiseTasks: (() => Promise<void>)[] = [];

				for (const item of items) {
					let pid: string = '';
					let task: Hooks.Task = taskShape(item);

					const promiseTask = async () => {
						try {
							// concurrency is disabled by default (exclusive execution)
							if (!task.concurrency) {
								// update task status to processing and set pid disallowing concurrency
								pid = this.uuid();
								task = await this.setTaskLock({
									date,
									pid,
									task
								});
							}

							if (this.webhookCaller) {
								await this.webhookCaller({
									executionType,
									pid,
									task
								});
							} else {
								await this.callWebhook({
									executionType,
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
								executionType,
								pid,
								task
							});
						}
					};

					promiseTasks = [...promiseTasks, promiseTask];
				}

				await promiseAll(promiseTasks, this.maxConcurrency);
			};

			await this.queryActiveTasks(queryActiveTasksOptions);
		} catch (err) {
			console.error('Error processing tasks:', err);
			result.errors += 1;

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
