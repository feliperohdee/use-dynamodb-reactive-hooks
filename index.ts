import _ from 'lodash';
import { ConditionalCheckFailedException, TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { promiseAllSettled } from 'use-async-helpers';
import { TransactWriteCommandOutput } from '@aws-sdk/lib-dynamodb';
import Dynamodb, { concatConditionExpression, concatUpdateExpression } from 'use-dynamodb';
import HttpError from 'use-http-error';
import UseFilterCriteria from 'use-filter-criteria';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';
import zDefault from 'zod-default-instance';

import schema from './index.schema';

const DEFAULT_MAX_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;
const SUBTASK_TTL_IN_MS = 15 * DAY_IN_MS;

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
		webhookCaller?: (input: Hooks.CallWebhookInput) => Promise<Hooks.Task[]>;
	};

	export type CallWebhookInput = z.input<typeof schema.callWebhookInput>;
	export type CheckExecuteTaskInput = z.input<typeof schema.checkExecuteTaskInput>;
	export type DeleteInput = z.input<typeof schema.deleteInput>;
	export type FetchInput = z.input<typeof schema.fetchInput>;
	export type FetchLogsInput = z.input<typeof schema.fetchLogsInput>;
	export type GetInput = z.input<typeof schema.getTaskInput>;
	export type Log = z.infer<typeof schema.log>;
	export type QueryActiveTasksInput = z.input<typeof schema.queryActiveTasksInput>;
	export type RegisterForkTaskInput = z.input<typeof schema.registerForkTaskInput>;
	export type RegisterScheduledSubTaskInput = z.input<typeof schema.registerScheduledSubTaskInput>;
	export type SetTaskActiveInput = z.input<typeof schema.setTaskActiveInput>;
	export type SetTaskErrorInput = z.input<typeof schema.setTaskErrorInput>;
	export type SetTaskLockInput = z.input<typeof schema.setTaskLockInput>;
	export type SetTaskSuccessInput = z.input<typeof schema.setTaskSuccessInput>;
	export type Task = z.infer<typeof schema.task>;
	export type TaskExecutionType = z.infer<typeof schema.taskExecutionType>;
	export type TaskInput = z.input<typeof schema.taskInput>;
	export type TaskKeys = z.infer<typeof schema.taskKeys>;
	export type TaskStatus = z.infer<typeof schema.taskStatus>;
	export type TaskType = z.infer<typeof schema.taskType>;
	export type TimeUnit = z.infer<typeof schema.timeUnit>;
	export type TriggerInput = z.input<typeof schema.triggerInput>;
	export type TriggerInputConditionFilter = UseFilterCriteria.MatchInput;
}

const taskShape = (input: Partial<Hooks.Task | Hooks.TaskInput>): Hooks.Task => {
	return zDefault(schema.task, input);
};

class TaskException extends Error {
	constructor(message: string) {
		super(message);
		this.name = 'TaskException';
	}
}

class Hooks {
	public static schema = schema;

	public customWebhookCall?: (input: Hooks.CallWebhookInput) => Promise<Hooks.Task[]>;
	public db: { tasks: Dynamodb<Hooks.Task> };
	public maxConcurrency: number;
	public maxErrors: number;
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
					name: 'trigger-status-namespace-event-pattern',
					partition: 'status',
					partitionType: 'S',
					projection: {
						nonKeyAttributes: ['noAfter', 'noBefore', 'pid', 'repeatMax', 'totalExecutions'],
						type: 'INCLUDE'
					},
					sort: 'namespace__eventPattern',
					sortType: 'S'
				},
				// used to trigger tasks by status / scheduledDate
				{
					name: 'trigger-status-scheduled-date',
					partition: 'status',
					partitionType: 'S',
					projection: {
						nonKeyAttributes: ['noAfter', 'noBefore', 'pid', 'repeatMax', 'totalExecutions'],
						type: 'INCLUDE'
					},
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

		this.customWebhookCall = options.webhookCaller;
		this.db = { tasks };
		this.maxConcurrency = options.maxConcurrency || DEFAULT_MAX_CONCURRENCY;
		this.maxErrors = options.maxErrors || 5;
		this.webhooks = webhooks;
	}

	private calculateNextSchedule(currentTime: string, rule: { unit: Hooks.TimeUnit; value: number }): string {
		let current = new Date(currentTime);
		let ms: number;

		switch (rule.unit) {
			case 'minutes':
				ms = rule.value * MINUTE_IN_MS;
				break;
			case 'hours':
				ms = rule.value * HOUR_IN_MS;
				break;
			case 'days':
				ms = rule.value * DAY_IN_MS;
				break;
		}

		const next = new Date(current.getTime() + ms);

		return next.toISOString();
	}

	async callWebhook(input: Hooks.CallWebhookInput): Promise<Hooks.Task[]> {
		let args = await schema.callWebhookInput.parseAsync(input);
		let promiseTasks: (() => Promise<Hooks.Task | null>)[] = [];

		for (const keys of args.keys) {
			let pid = this.uuid();
			let task = await this.getTask({
				id: keys.id,
				namespace: keys.namespace
			});

			if (!task) {
				continue;
			}

			if (task.conditionFilter && _.size(args.conditionData) > 0) {
				const match = await UseFilterCriteria.match(args.conditionData, task.conditionFilter);

				if (!match) {
					continue;
				}
			}

			const promiseTask = async () => {
				let type = task!.type;
				let request = {
					requestBody: {
						...task!.requestBody,
						...args.requestBody
					},
					requestHeaders: {
						...task!.requestHeaders,
						...args.requestHeaders
					},
					requestMethod: args.requestMethod || task!.requestMethod,
					requestUrl: args.requestUrl || task!.requestUrl
				};

				try {
					if (args.executionType === 'EVENT' && args.forkId) {
						task = await this.registerForkTask({
							forkId: args.forkId,
							parentTask: {
								...task!,
								requestBody: request.requestBody,
								requestHeaders: request.requestHeaders,
								requestMethod: request.requestMethod,
								requestUrl: request.requestUrl
							}
						});
					}

					// handle delayed tasks registering subTasks to handle them afterwards
					if (args.executionType === 'EVENT' && args.delayValue > 0) {
						return await this.registerScheduledSubTask({
							delayDebounce: args.delayDebounce,
							delayUnit: args.delayUnit,
							delayValue: args.delayValue,
							parentTask: {
								...task!,
								requestBody: request.requestBody,
								requestHeaders: request.requestHeaders,
								requestMethod: request.requestMethod,
								requestUrl: request.requestUrl
							}
						});
					}

					// handle subTasks
					if (args.executionType === 'SCHEDULED' && type === 'SUBTASK') {
						// check if parent task is not disabled
						const parentTask = await this.checkParentTask(task!);

						task = parentTask;
					}

					// check if task is able to run just before execution
					task = await this.checkExecuteTask({
						date: args.date,
						task: task!
					});

					// concurrency is disabled by default (exclusive execution)
					if (!task.concurrency) {
						// update task status to processing and set pid disallowing concurrency
						task = await this.setTaskLock({ pid, task });
					}

					const log = await this.webhooks.trigger({
						metadata: {
							executionType: args.executionType,
							taskId: task.id,
							taskType: type
						},
						namespace: task.namespace,
						requestBody: request.requestBody,
						requestHeaders: request.requestHeaders,
						requestMethod: request.requestMethod,
						requestUrl: request.requestUrl,
						retryLimit: task.retryLimit
					});

					task = await this.setTaskSuccess({
						executionType: args.executionType,
						log,
						pid,
						task
					});
				} catch (err) {
					if (err instanceof ConditionalCheckFailedException || err instanceof TaskException) {
						return null;
					}

					task = await this.setTaskError({
						error: err as Error,
						executionType: args.executionType,
						pid,
						task: task!
					});
				}

				return task;
			};

			promiseTasks = [...promiseTasks, promiseTask];
		}

		const res = await promiseAllSettled(promiseTasks, this.maxConcurrency);

		return _.compact(_.map(res, 'value'));
	}

	private async checkExecuteTask(input: Hooks.CheckExecuteTaskInput): Promise<Hooks.Task> {
		const args = await schema.checkExecuteTaskInput.parseAsync(input);
		const task = await this.getTask({
			id: args.task.id,
			namespace: args.task.namespace
		});

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (args.task.__ts !== task.__ts) {
			throw new TaskException('Task was modified');
		}

		if (task.noAfter && args.date > new Date(task.noAfter)) {
			throw new TaskException('Task must not be executed after the noAfter date');
		}

		if (task.noBefore && args.date < new Date(task.noBefore)) {
			throw new TaskException('Task must not be executed before the noBefore date');
		}

		if (task.pid) {
			throw new TaskException('Task is already running');
		}

		if (task.repeatMax > 0 && task.totalExecutions >= task.repeatMax) {
			throw new TaskException('Task has reached the repeat max');
		}

		if (task.status !== 'ACTIVE') {
			throw new TaskException('Task is not in a valid state');
		}

		return task;
	}

	private async checkParentTask(input: Hooks.Task): Promise<Hooks.Task> {
		if (!input.parentId || !input.parentNamespace) {
			throw new TaskException('Input is not a subtask');
		}

		const parentTask = await this.getTask({
			id: input.parentId,
			namespace: input.parentNamespace
		});

		if (!parentTask) {
			throw new TaskException('Parent task not found');
		}

		if (parentTask.status === 'DISABLED') {
			throw new TaskException('Parent task is disabled');
		}

		if (parentTask.type !== 'PRIMARY' && parentTask.type !== 'FORK') {
			throw new TaskException('Parent task must be a primary or fork task');
		}

		return parentTask;
	}

	private async countSubTasks(namespace: string, id: string): Promise<number> {
		const { count } = await this.db.tasks.query({
			item: { id: `${id}#`, namespace: `${namespace}#SUBTASK` },
			prefix: true,
			select: ['id']
		});

		return count;
	}

	async clearLogs(namespace: string): Promise<{ count: number }> {
		return this.webhooks.clearLogs(namespace);
	}

	async clearTasks(namespace: string): Promise<{ count: number }> {
		const res = await Promise.all([
			this.db.tasks.clear(namespace),
			this.db.tasks.clear(`${namespace}#FORK`),
			this.db.tasks.clear(`${namespace}#SUBTASK`),
			this.db.tasks.clear(`${namespace}#FORK#SUBTASK`)
		]);

		return {
			count: _.sumBy(res, 'count')
		};
	}

	async deleteTask(input: Hooks.DeleteInput): Promise<Hooks.Task | null> {
		try {
			const args = await schema.deleteInput.parseAsync(input);
			const res = await this.db.tasks.delete({
				attributeNames: { '#type': 'type' },
				attributeValues: { ':primary': 'PRIMARY' },
				conditionExpression: '#type = :primary',
				filter: {
					item: {
						id: args.id,
						namespace: args.namespace
					}
				}
			});

			if (!res) {
				return null;
			}

			const task = taskShape(res);

			await Promise.all([
				this.db.tasks.deleteMany({
					item: {
						id: task.id,
						namespace: `${task.namespace}#FORK`
					},
					prefix: true
				}),
				this.db.tasks.deleteMany({
					item: {
						id: task.id,
						namespace: `${task.namespace}#FORK#SUBTASK`
					},
					prefix: true
				}),
				this.db.tasks.deleteMany({
					item: {
						id: task.id,
						namespace: `${task.namespace}#SUBTASK`
					},
					prefix: true
				})
			]);

			return task;
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				throw new TaskException('Task must be a primary task');
			}

			throw err;
		}
	}

	async fetchLogs(input: Hooks.FetchLogsInput): Promise<Dynamodb.MultiResponse<Hooks.Log, false>> {
		return this.webhooks.fetchLogs(input);
	}

	async fetchTasks(input: Hooks.FetchInput): Promise<Dynamodb.MultiResponse<Hooks.Task, false>> {
		const args = await schema.fetchInput.parseAsync(input);
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

		// QUERY BY EVENT_PATTERN INDEX
		if (args.eventPattern) {
			// omit [eventPattern] filter
			filters.eventPattern = '';

			queryOptions.index = 'namespace-event-pattern';
			queryOptions.queryExpression = [
				'#namespace = :namespace',
				args.eventPatternPrefix ? 'begins_with(#eventPattern, :eventPattern)' : '#eventPattern = :eventPattern'
			].join(' AND ');

			return query(queryOptions);
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

			queryOptions.queryExpression = `#namespace = :namespace AND ${args.idPrefix ? 'begins_with(#id, :id)' : '#id = :id'}`;

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

	async getTask(input: Hooks.GetInput, consistentRead: boolean = false): Promise<Hooks.Task | null> {
		const args = await schema.getTaskInput.parseAsync(input);

		let [id, namespace] = [args.id, args.namespace];
		let prefix = false;

		if (args.forkId) {
			id = _.compact([args.id, args.forkId]).join('#');
		}

		if (args.forkId) {
			id = [args.id, args.forkId].join('#');
			namespace = `${namespace}#FORK`;
			prefix = true;
		}

		if (args.type === 'SUBTASK-DELAY-STANDARD') {
			id = [id, 'DELAY-STANDARD'].join('#');
			namespace = `${namespace}#SUBTASK`;
			prefix = true;
		} else if (args.type === 'SUBTASK-DELAY-DEBOUNCE') {
			id = [id, 'DELAY-DEBOUNCE'].join('#');
			namespace = `${namespace}#SUBTASK`;
			prefix = true;
		}

		const res = await this.db.tasks.get({
			consistentRead,
			item: { namespace, id },
			prefix
		});

		return res ? taskShape(res) : null;
	}

	private async queryActiveTasks(args: Hooks.QueryActiveTasksInput): Promise<Dynamodb.MultiResponse<Hooks.TaskKeys>> {
		args = await schema.queryActiveTasksInput.parseAsync(args);

		const queryOptions: Dynamodb.QueryOptions<Hooks.TaskKeys> = {
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
				':now': args.date.toISOString(),
				':zero': 0
			},
			chunkLimit: 100,
			filterExpression: [
				'#pid = :empty',
				'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
				'(#noBefore = :empty OR :now > #noBefore)',
				'(#noAfter = :empty OR :now < #noAfter)'
			].join(' AND '),
			limit: Infinity,
			onChunk: args.onChunk,
			select: ['id', 'namespace'],
			strictChunkLimit: true
		};

		if ('eventPattern' in args && args.eventPattern) {
			queryOptions.index = 'trigger-status-namespace-event-pattern';
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace__eventPattern': 'namespace__eventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace__eventPattern': `${args.namespace}#${args.eventPattern}`
			};

			queryOptions.queryExpression = [
				args.eventPatternPrefix
					? 'begins_with(#namespace__eventPattern, :namespace__eventPattern)'
					: '#namespace__eventPattern = :namespace__eventPattern',
				'#status = :active'
			].join(' AND ');

			return this.db.tasks.query(queryOptions);
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
			queryOptions.queryExpression = '#id = :id AND #namespace = :namespace';

			return this.db.tasks.query(queryOptions);
		}

		// by scheduleTime
		queryOptions.index = 'trigger-status-scheduled-date';
		queryOptions.attributeNames = {
			...queryOptions.attributeNames,
			'#scheduledDate': 'scheduledDate'
		};

		queryOptions.attributeValues = {
			...queryOptions.attributeValues,
			':startOfTimes': '0000-00-00T00:00:00.000Z'
		};

		queryOptions.queryExpression = '#status = :active AND #scheduledDate BETWEEN :startOfTimes AND :now';

		return this.db.tasks.query(queryOptions);
	}

	private async registerForkTask(input: Hooks.RegisterForkTaskInput): Promise<Hooks.Task> {
		const args = await schema.registerForkTaskInput.parseAsync(input);

		if (args.parentTask.status === 'DISABLED') {
			throw new TaskException('Parent task is disabled');
		}

		if (args.parentTask.type !== 'PRIMARY') {
			throw new TaskException('Parent task must be a primary task');
		}

		try {
			return await this.db.tasks.put(
				taskShape({
					concurrency: args.parentTask.concurrency,
					conditionFilter: args.parentTask.conditionFilter,
					description: args.parentTask.description,
					eventPattern: args.parentTask.eventPattern,
					firstScheduledDate: args.parentTask.scheduledDate,
					forkId: args.forkId,
					id: `${args.parentTask.id}#${args.forkId}`,
					namespace: `${args.parentTask.namespace}#FORK`,
					namespace__eventPattern: args.parentTask.namespace__eventPattern,
					noAfter: args.parentTask.noAfter,
					noBefore: args.parentTask.noBefore,
					parentId: args.parentTask.id,
					parentNamespace: args.parentTask.namespace,
					repeatInterval: args.parentTask.repeatInterval,
					repeatMax: args.parentTask.repeatMax,
					repeatUnit: args.parentTask.repeatUnit,
					requestBody: args.parentTask.requestBody,
					requestHeaders: args.parentTask.requestHeaders,
					requestMethod: args.parentTask.requestMethod,
					requestUrl: args.parentTask.requestUrl,
					rescheduleOnEvent: args.parentTask.rescheduleOnEvent,
					retryLimit: args.parentTask.retryLimit,
					scheduledDate: args.parentTask.scheduledDate,
					status: 'ACTIVE',
					type: 'FORK'
				})
			);
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				throw new TaskException('Fork task already exists');
			}

			throw err;
		}
	}

	private async registerScheduledSubTask(input: Hooks.RegisterScheduledSubTaskInput): Promise<Hooks.Task> {
		const args = await schema.registerScheduledSubTaskInput.parseAsync(input);

		if (args.parentTask.status === 'DISABLED') {
			throw new TaskException('Parent task is disabled');
		}

		if (args.parentTask.type !== 'PRIMARY' && args.parentTask.type !== 'FORK') {
			throw new TaskException('Parent task must be a primary or fork task');
		}

		if (args.parentTask.repeatMax > 0 && args.parentTask.totalExecutions >= args.parentTask.repeatMax) {
			throw new TaskException('Parent task has reached the repeat max by totalExecutions');
		}

		const date = new Date();
		const scheduledDate = this.calculateNextSchedule(date.toISOString(), {
			unit: args.delayUnit,
			value: args.delayValue
		});

		let currentTask: Hooks.Task | null = null;

		// update existent debounced task
		if (args.delayDebounce) {
			currentTask = await this.getTask({
				id: [args.parentTask.id, 'DELAY-DEBOUNCE'].join('#'),
				namespace: `${args.parentTask.namespace}#SUBTASK`
			});
		}

		if (!currentTask) {
			const totalSubTasks = await this.countSubTasks(args.parentTask.namespace, args.parentTask.id);

			// check if parent has capacity to register new subtask
			if (args.parentTask.repeatMax > 0 && args.parentTask.totalExecutions + totalSubTasks >= args.parentTask.repeatMax) {
				throw new TaskException('Parent task has reached the repeat max by totalSubTasks');
			}
		}

		let [id, namespace] = [args.parentTask.id, args.parentTask.namespace];

		if (args.delayDebounce) {
			id = [id, 'DELAY-DEBOUNCE'].join('#');
			namespace = `${namespace}#SUBTASK`;
		} else {
			id = [id, 'DELAY', _.now()].join('#');
			namespace = `${namespace}#SUBTASK`;
		}

		const newTask = taskShape({
			eventPattern: '-',
			firstScheduledDate: scheduledDate,
			forkId: args.parentTask.forkId,
			id,
			namespace,
			namespace__eventPattern: '-',
			parentId: args.parentTask.id,
			parentNamespace: args.parentTask.namespace,
			requestBody: args.parentTask.requestBody,
			requestHeaders: args.parentTask.requestHeaders,
			requestMethod: args.parentTask.requestMethod,
			requestUrl: args.parentTask.requestUrl,
			scheduledDate,
			status: 'ACTIVE',
			ttl: Math.floor((new Date(scheduledDate).getTime() + SUBTASK_TTL_IN_MS) / 1000),
			type: 'SUBTASK'
		});

		if (currentTask) {
			newTask.__createdAt = currentTask.__createdAt;
		}

		return this.db.tasks.put(newTask, {
			overwrite: true,
			useCurrentCreatedAtIfExists: true
		});
	}

	async registerTask(input: Hooks.TaskInput): Promise<Hooks.Task> {
		const args = await schema.taskInput.parseAsync(input);
		const scheduledDate = args.scheduledDate ? new Date(args.scheduledDate).toISOString() : '-';

		return this.db.tasks.put(
			taskShape({
				...args,
				eventPattern: args.eventPattern || '-',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: scheduledDate,
				id: this.uuid(),
				lastError: '',
				lastErrorDate: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				namespace__eventPattern: args.eventPattern ? `${args.namespace}#${args.eventPattern}` : '-',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				noAfter: args.noAfter ? new Date(args.noAfter).toISOString() : '',
				noBefore: args.noBefore ? new Date(args.noBefore).toISOString() : '',
				scheduledDate
			})
		);
	}

	async setTaskActive(input: Hooks.SetTaskActiveInput): Promise<Hooks.Task | null> {
		const args = await schema.setTaskActiveInput.parseAsync(input);
		const task = await this.getTask({
			id: args.id,
			namespace: args.namespace
		});

		const status = args.active ? 'ACTIVE' : 'DISABLED';

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (task.type !== 'PRIMARY') {
			throw new TaskException('Task must be a primary task');
		}

		if (task.status === status) {
			throw new TaskException(`Task is already ${status}`);
		}

		if (task.status === 'MAX-ERRORS-REACHED' || task.status === 'PROCESSING') {
			throw new TaskException('Task is not in a valid state');
		}

		const forks = await this.db.tasks.query<Hooks.TaskKeys>({
			attributeNames: {
				'#id': 'id',
				'#namespace': 'namespace'
			},
			attributeValues: {
				':id': task.id,
				':namespace': `${task.namespace}#FORK`
			},
			limit: Infinity,
			queryExpression: '#namespace = :namespace AND begins_with(#id, :id)',
			select: ['id', 'namespace']
		});

		let keys: Hooks.TaskKeys[] = [{ id: task.id, namespace: task.namespace }, ...forks.items];
		let promiseTasks: (() => Promise<TransactWriteCommandOutput[]>)[] = [];

		for (const chunk of _.chunk(keys, 100)) {
			promiseTasks = [
				...promiseTasks,
				() => {
					return this.db.tasks.transaction({
						TransactItems: _.map(chunk, item => {
							return {
								Update: {
									ExpressionAttributeNames: { '#status': 'status' },
									ExpressionAttributeValues: { ':status': status },
									Key: item,
									TableName: this.db.tasks.table,
									UpdateExpression: 'SET #status = :status'
								}
							};
						})
					});
				}
			];
		}

		await promiseAllSettled(promiseTasks, this.maxConcurrency);
		await Promise.all([
			this.db.tasks.deleteMany({
				item: {
					id: task.id,
					namespace: `${task.namespace}#FORK#SUBTASK`
				},
				prefix: true
			}),
			this.db.tasks.deleteMany({
				item: {
					id: task.id,
					namespace: `${task.namespace}#SUBTASK`
				},
				prefix: true
			})
		]);

		return (await this.getTask(
			{
				id: task.id,
				namespace: task.namespace
			},
			true
		))!;
	}

	private async setTaskError(input: Hooks.SetTaskErrorInput): Promise<Hooks.Task> {
		const args = await schema.setTaskErrorInput.parseAsync(input);
		const date = new Date();
		const httpError = HttpError.wrap(args.error);
		const updateOptions: Dynamodb.UpdateOptions<Hooks.Task> = {
			attributeNames: {
				'#lastError': 'lastError',
				'#lastErrorDate': 'lastErrorDate',
				'#lastErrorExecutionType': 'lastErrorExecutionType',
				'#pid': 'pid',
				'#totalErrors': 'totalErrors'
			},
			attributeValues: {
				':empty': '',
				':error': httpError.message,
				':executionType': args.executionType,
				':now': date.toISOString(),
				':one': 1
			},
			filter: {
				item: {
					id: args.task.id,
					namespace: args.task.namespace
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
			`ADD ${['#totalErrors :one'].join(', ')}`,
			`SET ${['#lastErrorExecutionType = :executionType', '#lastError = :error', '#lastErrorDate = :now', '#pid = :empty'].join(', ')}`
		].join(' ');

		if (args.task.firstErrorDate === '') {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#firstErrorDate': 'firstErrorDate'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #firstErrorDate = :now');
		}

		// set MAX-ERRORS-REACHED status if max errors reached
		const nextErrorsCount = args.task.totalErrors + 1;

		if (this.maxErrors > 0 && nextErrorsCount >= this.maxErrors) {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#status': 'status'
			};

			updateOptions.attributeValues = {
				...updateOptions.attributeValues,
				':maxErrorsReached': 'MAX-ERRORS-REACHED'
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
		const args = await schema.setTaskLockInput.parseAsync(input);

		return taskShape(
			await this.db.tasks.update({
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
					':pid': args.pid,
					':processing': 'PROCESSING',
					':ts': args.task.__ts,
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
					item: {
						id: args.task.id,
						namespace: args.task.namespace
					}
				},
				updateExpression: 'SET #status = :processing, #pid = :pid'
			})
		);
	}

	private async setTaskSuccess(input: Hooks.SetTaskSuccessInput) {
		const args = await schema.setTaskSuccessInput.parseAsync(input);
		const date = new Date();
		const updateOptions: Dynamodb.UpdateOptions<Hooks.Task> = {
			attributeNames: {
				'#lastExecutionDate': 'lastExecutionDate',
				'#lastExecutionType': 'lastExecutionType',
				'#lastResponseBody': 'lastResponseBody',
				'#lastResponseHeaders': 'lastResponseHeaders',
				'#lastResponseStatus': 'lastResponseStatus',
				'#pid': 'pid',
				'#totalExecutions': 'totalExecutions',
				'#totalSuccessfulOrFailed': args.log.responseOk ? 'totalSuccessfulExecutions' : 'totalFailedExecutions'
			},
			attributeValues: {
				':empty': '',
				':executionType': args.executionType,
				':now': date.toISOString(),
				':one': 1,
				':responseBody': args.log.responseBody,
				':responseHeaders': args.log.responseHeaders,
				':responseStatus': args.log.responseStatus
			},
			filter: {
				item: {
					id: args.task.id,
					namespace: args.task.namespace
				}
			}
		};

		updateOptions.updateExpression = [
			`ADD ${['#totalExecutions :one', '#totalSuccessfulOrFailed :one'].join(', ')}`,
			`SET ${[
				'#lastExecutionDate = :now',
				'#lastExecutionType = :executionType',
				'#lastResponseBody = :responseBody',
				'#lastResponseHeaders = :responseHeaders',
				'#lastResponseStatus = :responseStatus',
				'#pid = :empty'
			].join(', ')}`
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

		if (args.task.firstExecutionDate === '') {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#firstExecutionDate': 'firstExecutionDate'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #firstExecutionDate = :now');
		}

		const nextExecutionCount = args.task.totalExecutions + 1;
		const repeat = args.task.repeatMax === 0 || nextExecutionCount < args.task.repeatMax;

		if (
			(args.executionType === 'SCHEDULED' || (args.executionType === 'EVENT' && args.task.rescheduleOnEvent)) &&
			repeat &&
			args.task.repeatInterval > 0 &&
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
				':scheduledDate': this.calculateNextSchedule(args.task.scheduledDate, {
					unit: args.task.repeatUnit,
					value: args.task.repeatInterval
				}),
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
				':maxRepeatReached': 'MAX-REPEAT-REACHED'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :maxRepeatReached');
		}

		return taskShape(await this.db.tasks.update(updateOptions));
	}

	async trigger(input?: Hooks.TriggerInput): Promise<{ processed: number; errors: number }> {
		let date = new Date();
		let result = { processed: 0, errors: 0 };
		let queryActiveTasksOptions: Hooks.QueryActiveTasksInput = {
			date,
			onChunk: async () => {}
		};

		if (input) {
			const args = await schema.triggerInput.parseAsync(input);

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
					namespace: args.namespace
				};
			}

			try {
				await this.queryActiveTasks({
					...queryActiveTasksOptions,
					onChunk: async ({ items }) => {
						const callWebhookInput: Hooks.CallWebhookInput = {
							conditionData: args.conditionData || {},
							delayDebounce: args.delayDebounce || false,
							delayUnit: args.delayUnit || 'minutes',
							delayValue: args.delayValue || 0,
							forkId: args.forkId || '',
							date,
							executionType: 'EVENT',
							keys: items,
							requestBody: args.requestBody,
							requestHeaders: args.requestHeaders,
							requestMethod: args.requestMethod,
							requestUrl: args.requestUrl
						};

						if (this.customWebhookCall) {
							const res = await this.customWebhookCall(callWebhookInput);

							result.processed += _.size(res);
						} else {
							const res = await this.callWebhook(callWebhookInput);

							result.processed += _.size(res);
						}
					}
				});
			} catch (err) {
				console.error('Error processing tasks:', err);
				result.errors += 1;

				throw err;
			}

			return result;
		}

		try {
			await this.queryActiveTasks({
				...queryActiveTasksOptions,
				onChunk: async ({ items }) => {
					const callWebhookInput: Hooks.CallWebhookInput = {
						conditionData: {},
						delayDebounce: false,
						delayUnit: 'minutes',
						delayValue: 0,
						forkId: '',
						date,
						executionType: 'SCHEDULED',
						keys: items,
						requestBody: null,
						requestHeaders: null,
						requestMethod: 'GET',
						requestUrl: ''
					};

					if (this.customWebhookCall) {
						const res = await this.customWebhookCall(callWebhookInput);

						result.processed += _.size(res);
					} else {
						const res = await this.callWebhook(callWebhookInput);

						result.processed += _.size(res);
					}
				}
			});
		} catch (err) {
			console.error('Error processing tasks:', err);
			result.errors += 1;

			throw err;
		}

		return result;
	}

	private uuid(): string {
		return crypto.randomUUID();
	}
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, SUBTASK_TTL_IN_MS, TaskException, taskShape };
export default Hooks;
