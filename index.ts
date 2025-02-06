import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAllSettled, promiseMap, promiseReduce } from 'use-async-helpers';
import { TransactWriteCommandOutput } from '@aws-sdk/lib-dynamodb';
import Dynamodb, { concatConditionExpression, concatUpdateExpression } from 'use-dynamodb';
import FilterCriteria from 'use-filter-criteria';
import HttpError from 'use-http-error';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';
import zDefault from 'use-zod-default';

import * as schema from './index.schema';

const DEFAULT_MAX_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;
const SUBTASK_TTL_IN_MS = 15 * DAY_IN_MS;

namespace Hooks {
	export type ConstructorOptions = {
		accessKeyId: string;
		createTable?: boolean;
		filterCriteria?: FilterCriteria;
		logsTableName: string;
		logsTtlInSeconds?: number;
		maxConcurrency?: number;
		maxErrors?: number;
		region: string;
		secretAccessKey: string;
		tasksTableName: string;
		webhookCaller?: (input: Hooks.CallWebhookInput) => Promise<Hooks.Task[]>;
		webhookChunkSize?: number;
	};

	export type CallWebhookInput = z.input<typeof schema.callWebhookInput>;
	export type CheckExecuteTaskInput = z.input<typeof schema.checkExecuteTaskInput>;
	export type DebugConditionInput = z.input<typeof schema.debugConditionInput>;
	export type DeleteInput = z.input<typeof schema.deleteInput>;
	export type FetchLogsInput = z.input<typeof schema.fetchLogsInput>;
	export type FetchTasksInput = z.input<typeof schema.fetchTasksInput>;
	export type FetchTasksResponse = z.infer<typeof schema.fetchTasksResponse>;
	export type GetTaskInput = z.input<typeof schema.getTaskInput>;
	export type Log = z.infer<typeof schema.log>;
	export type QueryActiveTasksInput = z.input<typeof schema.queryActiveTasksInput>;
	export type RegisterForkTaskInput = z.input<typeof schema.registerForkTaskInput>;
	export type SetTaskActiveInput = z.input<typeof schema.setTaskActiveInput>;
	export type SetTaskErrorInput = z.input<typeof schema.setTaskErrorInput>;
	export type SetTaskLockInput = z.input<typeof schema.setTaskLockInput>;
	export type SetTaskSuccessInput = z.input<typeof schema.setTaskSuccessInput>;
	export type Task = z.infer<typeof schema.task>;
	export type TaskExecutionType = z.infer<typeof schema.taskExecutionType>;
	export type TaskInput = z.input<typeof schema.taskInput>;
	export type TaskKeys = z.infer<typeof schema.taskKeys>;
	export type TaskRule = z.infer<typeof schema.taskRule>;
	export type TaskRuleResult = z.infer<typeof schema.taskRuleResult>;
	export type TaskStatus = z.infer<typeof schema.taskStatus>;
	export type TaskType = z.infer<typeof schema.taskType>;
	export type TimeUnit = z.infer<typeof schema.timeUnit>;
	export type TriggerInput = z.input<typeof schema.triggerInput>;
	export type TriggerInputConditionFilter = FilterCriteria.MatchInput;
	export type UpdateTaskInput = z.input<typeof schema.updateTaskInput>;
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
	public filterCriteria: FilterCriteria;
	public maxConcurrency: number;
	public maxErrors: number;
	public rules: Map<string, Hooks.TaskRule>;
	public webhookChunkSize: number;
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
					projection: {
						nonKeyAttributes: ['description', 'eventPattern', 'forkId', 'scheduledDate', 'status', 'title'],
						type: 'INCLUDE'
					},
					sort: 'eventPattern',
					sortType: 'S'
				},
				// used to fetch tasks by namespace / scheduledDate
				{
					name: 'namespace-scheduled-date',
					partition: 'namespace',
					partitionType: 'S',
					projection: {
						nonKeyAttributes: ['description', 'eventPattern', 'forkId', 'scheduledDate', 'status', 'title'],
						type: 'INCLUDE'
					},
					sort: 'scheduledDate',
					sortType: 'S'
				},
				// used to delete/setActive for tasks by primary namespace / primaryId
				{
					name: 'primary-namespace-primary-id',
					partition: 'primaryNamespace',
					partitionType: 'S',
					projection: {
						nonKeyAttributes: ['type'],
						type: 'INCLUDE'
					},
					sort: 'primaryId',
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
		this.filterCriteria = options.filterCriteria || new FilterCriteria();
		this.maxConcurrency = options.maxConcurrency || DEFAULT_MAX_CONCURRENCY;
		this.maxErrors = options.maxErrors || 5;
		this.rules = new Map();
		this.webhookChunkSize = options.webhookChunkSize || 0;
		this.webhooks = webhooks;
	}

	private calculateNextSchedule(currentTime: string, rule: { unit: Hooks.TimeUnit; value: number }): string {
		let current = new Date(currentTime);
		let ms: number = 0;

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
			let task = await this.getTaskInternal({
				id: keys.id,
				namespace: keys.namespace
			});

			if (!task) {
				continue;
			}

			if (task.conditionFilter) {
				const match = await this.filterCriteria.match(args.conditionData, task.conditionFilter);

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

				let delay = {
					eventDelayValue: args.eventDelayValue || task!.eventDelayValue,
					eventDelayUnit: args.eventDelayUnit || task!.eventDelayUnit,
					eventDelayDebounce: args.eventDelayDebounce || task!.eventDelayDebounce
				};

				try {
					if (args.forkId) {
						// forks tasks must be only registered in EVENT mode
						if (args.executionType === 'SCHEDULED') {
							return null;
						}

						task = await this.registerForkTask({
							forkId: args.forkId,
							primaryTask: task!
						});

						type = 'FORK';

						if (args.forkOnly) {
							return null;
						}
					}

					// handle delayed tasks registering subTasks to handle them afterwards
					if (delay.eventDelayValue > 0) {
						// subTasks must be registered in EVENT mode
						if (args.executionType === 'SCHEDULED') {
							return null;
						}

						return await this.registerDelayedSubTask({
							...task!,
							eventDelayDebounce: delay.eventDelayDebounce,
							eventDelayUnit: delay.eventDelayUnit,
							eventDelayValue: delay.eventDelayValue,
							requestBody: request.requestBody,
							requestHeaders: request.requestHeaders,
							requestMethod: request.requestMethod,
							requestUrl: request.requestUrl
						});
					}

					// handle subTasks
					if (type === 'SUBTASK') {
						// subTasks must execute only in SCHEDULED mode
						if (args.executionType === 'EVENT') {
							return null;
						}

						task = await this.getSubTaskParent(task!);
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

					// handle rules
					if (args.ruleId || task!.ruleId) {
						const rules = await this.executeRule(args.ruleId || task!.ruleId, {
							...task!,
							requestBody: request.requestBody,
							requestHeaders: request.requestHeaders,
							requestMethod: request.requestMethod,
							requestUrl: request.requestUrl
						});

						const logs = await promiseMap(
							rules,
							rule => {
								return this.webhooks.trigger({
									metadata: {
										executionType: args.executionType,
										forkId: task!.forkId,
										ruleId: args.ruleId || task!.ruleId,
										taskId: task!.primaryId,
										taskType: type
									},
									namespace: task!.primaryNamespace,
									requestBody: rule.requestBody || request.requestBody,
									requestHeaders: rule.requestHeaders || request.requestHeaders,
									requestMethod: rule.requestMethod || request.requestMethod,
									requestUrl: rule.requestUrl || request.requestUrl,
									retryLimit: task!.retryLimit
								});
							},
							this.maxConcurrency
						);

						const logsStats = _.reduce<Hooks.Log, Record<string, number>>(
							logs,
							(reduction, log) => {
								reduction.totalExecutions += 1;

								if (log.responseOk) {
									reduction.totalSuccessfulExecutions += 1;
								} else {
									reduction.totalFailedExecutions += 1;
								}

								return reduction;
							},
							{
								totalExecutions: 0,
								totalFailedExecutions: 0,
								totalSuccessfulExecutions: 0
							}
						);

						task = await this.setTaskSuccess({
							executionType: args.executionType,
							log: {
								responseBody: JSON.stringify(logsStats),
								responseHeaders: {},
								responseOk: true,
								responseStatus: 200
							},
							pid,
							task: {
								...task!,
								requestBody: request.requestBody,
								requestHeaders: request.requestHeaders,
								requestMethod: request.requestMethod,
								requestUrl: request.requestUrl
							}
						});
					} else {
						const log = await this.webhooks.trigger({
							metadata: {
								executionType: args.executionType,
								forkId: task.forkId,
								ruleId: '',
								taskId: task.primaryId,
								taskType: type
							},
							namespace: task.primaryNamespace,
							requestBody: request.requestBody,
							requestHeaders: request.requestHeaders,
							requestMethod: request.requestMethod,
							requestUrl: request.requestUrl,
							retryLimit: task.retryLimit
						});

						task = await this.setTaskSuccess({
							executionType: args.executionType,
							log: {
								responseBody: log.responseBody,
								responseHeaders: log.responseHeaders,
								responseOk: log.responseOk,
								responseStatus: log.responseStatus
							},
							pid,
							task
						});
					}
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
		const retrievedTask = await this.getTaskInternal({
			id: args.task.id,
			namespace: args.task.namespace
		});

		if (!retrievedTask) {
			throw new TaskException('Task not found');
		}

		const task = taskShape(retrievedTask);

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

	async clearLogs(namespace: string): Promise<{ count: number }> {
		return this.webhooks.clearLogs(namespace);
	}

	async clearTasks(namespace: string): Promise<{ count: number }> {
		const res = await Promise.all([
			this.db.tasks.clear(namespace),
			this.db.tasks.clear(`${namespace}#FORK`),
			this.db.tasks.clear(`${namespace}#SUBTASK`)
		]);

		return {
			count: _.sumBy(res, 'count')
		};
	}

	private async countSubTasks(task: Hooks.Task): Promise<number> {
		const { count } = await this.db.tasks.query({
			item: {
				id: task.id,
				namespace: `${task.primaryNamespace}#SUBTASK`
			},
			prefix: true,
			select: ['id']
		});

		return count;
	}

	async debugCondition(input: Hooks.DebugConditionInput): Promise<FilterCriteria.MatchDetailedResult> {
		const args = await schema.debugConditionInput.parseAsync(input);
		const task = await this.getTaskInternal({
			id: args.id,
			namespace: args.namespace
		});

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (!task.conditionFilter) {
			throw new TaskException('Task has no condition filter');
		}

		return this.filterCriteria.match(args.conditionData, task.conditionFilter, true) as Promise<FilterCriteria.MatchDetailedResult>;
	}

	async deleteTask(input: Hooks.DeleteInput): Promise<Hooks.Task> {
		const args = await schema.deleteInput.parseAsync(input);
		const task = await this.getTaskInternal(
			args.fork
				? {
						id: await this.uuidFromString(args.id),
						namespace: `${args.namespace}#FORK`
					}
				: {
						id: args.id,
						namespace: args.namespace
					}
		);

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (task.type !== 'PRIMARY' && task.type !== 'FORK') {
			throw new TaskException('Task must be a primary or fork task');
		}

		let keys: Hooks.TaskKeys[] = [];

		keys = await promiseReduce(
			task.type === 'FORK'
				? [
						// query all forks
						this.db.tasks.query({
							item: {
								id: task.id,
								namespace: `${task.primaryNamespace}#FORK`
							},
							limit: Infinity,
							select: ['id', 'namespace']
						}),
						// query all forks' subtasks
						this.db.tasks.query({
							item: {
								id: task.id,
								namespace: `${task.primaryNamespace}#SUBTASK`
							},
							limit: Infinity,
							prefix: true,
							select: ['id', 'namespace']
						})
					]
				: [
						// query all tasks' children
						this.db.tasks.query({
							item: {
								primaryId: task.id,
								primaryNamespace: task.primaryNamespace
							},
							limit: Infinity,
							select: ['id', 'namespace']
						})
					],
			(reduction, { items }) => {
				reduction = reduction.concat(
					_.map(items, item => {
						return {
							id: item.id,
							namespace: item.namespace
						};
					})
				);

				return reduction;
			},
			keys
		);

		let promiseTasks: (() => Promise<TransactWriteCommandOutput[]>)[] = [];

		for (const chunk of _.chunk(keys, 100)) {
			promiseTasks = [
				...promiseTasks,
				() => {
					const transactItems = _.map(chunk, ({ id, namespace }) => {
						return {
							Delete: {
								Key: { id, namespace },
								TableName: this.db.tasks.table
							}
						};
					});

					return this.db.tasks.transaction({
						TransactItems: _.compact(transactItems)
					});
				}
			];
		}

		await promiseAllSettled(promiseTasks, this.maxConcurrency);

		return task;
	}

	private async executeRule(key: string, task: Hooks.Task): Promise<Hooks.TaskRuleResult[]> {
		const rule = this.rules.get(key);

		if (!rule) {
			return [];
		}

		return rule({ task });
	}

	async fetchLogs(input: Hooks.FetchLogsInput): Promise<Dynamodb.MultiResponse<Hooks.Log, false>> {
		return this.webhooks.fetchLogs(input);
	}

	async fetchTasks(input: Hooks.FetchTasksInput): Promise<Hooks.FetchTasksResponse> {
		let args = await schema.fetchTasksInput.parseAsync(input);
		let namespace = args.namespace;

		if (args.fork) {
			namespace = `${args.namespace}#FORK`;
		}

		if (args.subTask) {
			namespace = `${args.namespace}#SUBTASK`;
		}

		const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
			attributeNames: { '#namespace': 'namespace' },
			attributeValues: { ':namespace': namespace },
			filterExpression: '',
			limit: args.limit,
			queryExpression: '',
			scanIndexForward: args.desc ? false : true,
			select: ['description', 'eventPattern', 'forkId', 'id', 'namespace', 'scheduledDate', 'status', 'title'],
			startKey: args.startKey
		};

		const filters = {
			eventPattern: '',
			scheduledDate: '',
			status: ''
		};

		const query = async (options: Dynamodb.QueryOptions<Hooks.Task>) => {
			options.filterExpression = _.values(filters).filter(Boolean).join(' AND ');

			return this.db.tasks.query(options);
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

		// QUERY BY ID
		if (args.id) {
			let id = args.id;

			if (args.fork) {
				id = await this.uuidFromString(args.id);
			}

			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': id
			};

			queryOptions.queryExpression = '#namespace = :namespace AND #id = :id';

			if (args.idPrefix) {
				queryOptions.queryExpression = '#namespace = :namespace AND begins_with(#id, :id)';
			}

			return query(queryOptions);
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

	private async getSubTaskParent(input: Hooks.Task): Promise<Hooks.Task> {
		const args = await schema.task.parseAsync(input);

		if (args.type !== 'SUBTASK') {
			throw new TaskException('Task must be a subtask');
		}

		let namespace = args.primaryNamespace;

		if (args.forkId) {
			namespace = `${args.primaryNamespace}#FORK`;
		}

		const parentTask = await this.getTaskInternal({
			id: args.id.replace(/#DELAY.*$/, ''),
			namespace
		});

		if (!parentTask) {
			throw new TaskException('Parent task not found');
		}

		if (parentTask.type !== 'PRIMARY' && parentTask.type !== 'FORK') {
			throw new TaskException('Parent task must be a primary or fork task');
		}

		return parentTask;
	}

	async getTask(input: Hooks.GetTaskInput): Promise<Hooks.Task> {
		const args = await schema.getTaskInput.parseAsync(input);
		const res = await this.db.tasks.get({
			item: args.fork
				? {
						id: await this.uuidFromString(args.id),
						namespace: `${args.namespace}#FORK`
					}
				: {
						id: args.id,
						namespace: args.namespace
					}
		});

		if (!res) {
			throw new TaskException('Task not found');
		}

		return taskShape(res);
	}

	private async getTaskInternal(input: Hooks.GetTaskInput, consistentRead: boolean = false): Promise<Hooks.Task | null> {
		const res = await this.db.tasks.get({
			consistentRead,
			item: {
				id: input.id,
				namespace: input.namespace
			}
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

	private async registerDelayedSubTask(input: Hooks.Task): Promise<Hooks.Task> {
		const args = await schema.task.parseAsync(input);

		if (args.repeatMax > 0 && args.totalExecutions >= args.repeatMax) {
			throw new TaskException('Primary task has reached the repeat max by totalExecutions');
		}

		if (args.status === 'DISABLED') {
			throw new TaskException('Primary task is disabled');
		}

		if (args.type !== 'PRIMARY' && args.type !== 'FORK') {
			throw new TaskException('Task must be a primary or fork task');
		}

		const date = new Date();
		const scheduledDate = this.calculateNextSchedule(date.toISOString(), {
			unit: args.eventDelayUnit,
			value: args.eventDelayValue
		});

		let currentTask: Hooks.Task | null = null;

		// update existent debounced task
		if (args.eventDelayDebounce) {
			currentTask = await this.getTaskInternal({
				id: [args.id, 'DELAY-DEBOUNCE'].join('#'),
				namespace: `${args.primaryNamespace}#SUBTASK`
			});
		}

		if (!currentTask) {
			const totalSubTasks = await this.countSubTasks(args);

			// check if primary task has capacity to register new subtask
			if (args.repeatMax > 0 && args.totalExecutions + totalSubTasks >= args.repeatMax) {
				throw new TaskException('Primary task has reached the repeat max by totalSubTasks');
			}
		}

		let id = args.id;

		if (args.eventDelayDebounce) {
			id = [id, 'DELAY-DEBOUNCE'].join('#');
		} else {
			id = [id, 'DELAY', _.now()].join('#');
		}

		const newTask = taskShape({
			eventPattern: '-',
			firstScheduledDate: scheduledDate,
			id,
			forkId: args.forkId,
			namespace: `${args.primaryNamespace}#SUBTASK`,
			namespace__eventPattern: '-',
			primaryId: args.primaryId,
			primaryNamespace: args.primaryNamespace,
			requestBody: args.requestBody,
			requestHeaders: args.requestHeaders,
			requestMethod: args.requestMethod,
			requestUrl: args.requestUrl,
			ruleId: args.ruleId,
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

	private async registerForkTask(input: Hooks.RegisterForkTaskInput, overwrite: boolean = false): Promise<Hooks.Task> {
		const args = await schema.registerForkTaskInput.parseAsync(input);

		if (args.primaryTask.status === 'DISABLED') {
			throw new TaskException('Primary task is disabled');
		}

		if (args.primaryTask.type !== 'PRIMARY') {
			throw new TaskException('Task must be a primary task');
		}

		const id = await this.uuidFromString(args.forkId);
		const namespace = `${args.primaryTask.primaryNamespace}#FORK`;
		const currentFork = !overwrite ? await this.getTaskInternal({ id, namespace }) : null;

		if (currentFork) {
			return currentFork;
		}

		return await this.db.tasks.put(
			taskShape({
				concurrency: args.primaryTask.concurrency,
				conditionFilter: args.primaryTask.conditionFilter,
				description: args.primaryTask.description,
				eventDelayDebounce: args.primaryTask.eventDelayDebounce,
				eventDelayUnit: args.primaryTask.eventDelayUnit,
				eventDelayValue: args.primaryTask.eventDelayValue,
				eventPattern: args.primaryTask.eventPattern,
				firstScheduledDate: args.primaryTask.scheduledDate,
				forkId: args.forkId,
				id,
				namespace,
				namespace__eventPattern: args.primaryTask.namespace__eventPattern,
				noAfter: args.primaryTask.noAfter,
				noBefore: args.primaryTask.noBefore,
				primaryId: args.primaryTask.primaryId,
				primaryNamespace: args.primaryTask.primaryNamespace,
				repeatInterval: args.primaryTask.repeatInterval,
				repeatMax: args.primaryTask.repeatMax,
				repeatUnit: args.primaryTask.repeatUnit,
				requestBody: args.primaryTask.requestBody,
				requestHeaders: args.primaryTask.requestHeaders,
				requestMethod: args.primaryTask.requestMethod,
				requestUrl: args.primaryTask.requestUrl,
				rescheduleOnEvent: args.primaryTask.rescheduleOnEvent,
				retryLimit: args.primaryTask.retryLimit,
				ruleId: args.primaryTask.ruleId,
				scheduledDate: args.primaryTask.scheduledDate,
				status: 'ACTIVE',
				type: 'FORK'
			}),
			{ overwrite }
		);
	}

	registerRule(name: string, rule: Hooks.TaskRule): void {
		this.rules.set(name, rule);
	}

	async registerTask(input: Hooks.TaskInput): Promise<Hooks.Task> {
		const args = await schema.taskInput.parseAsync(input);
		const scheduledDate = args.scheduledDate ? new Date(args.scheduledDate).toISOString() : '-';
		const id = args.id || this.uuid();

		return this.db.tasks.put(
			taskShape({
				...args,
				eventPattern: args.eventPattern || '-',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: scheduledDate,
				id,
				lastError: '',
				lastErrorDate: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: null,
				lastResponseStatus: 0,
				namespace__eventPattern: args.eventPattern ? `${args.namespace}#${args.eventPattern}` : '-',
				primaryId: id,
				primaryNamespace: args.namespace,
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

	async setTaskActive(input: Hooks.SetTaskActiveInput): Promise<Hooks.Task> {
		const args = await schema.setTaskActiveInput.parseAsync(input);
		const date = new Date();
		const now = _.now();
		const task = await this.getTaskInternal(
			args.fork
				? {
						id: await this.uuidFromString(args.id),
						namespace: `${args.namespace}#FORK`
					}
				: {
						id: args.id,
						namespace: args.namespace
					}
		);

		const status = args.active ? 'ACTIVE' : 'DISABLED';

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (task.type !== 'PRIMARY' && task.type !== 'FORK') {
			throw new TaskException('Task must be a primary or fork task');
		}

		if (task.status === status) {
			throw new TaskException(`Task is already ${status}`);
		}

		if (task.status === 'MAX-ERRORS-REACHED' || task.status === 'PROCESSING') {
			throw new TaskException('Task is not in a valid state');
		}

		if (status === 'ACTIVE' && task.repeatMax > 0 && task.totalExecutions >= task.repeatMax) {
			throw new TaskException('Task has reached the repeat max');
		}

		let keys: (Hooks.TaskKeys & { type: Hooks.TaskType })[] = [];

		keys = await promiseReduce(
			task.type === 'FORK'
				? [
						// query all forks
						this.db.tasks.query({
							item: {
								id: task.id,
								namespace: `${task.primaryNamespace}#FORK`
							},
							limit: Infinity,
							select: ['id', 'namespace', 'type']
						}),
						// query all forks' subtasks
						this.db.tasks.query({
							item: {
								id: task.id,
								namespace: `${task.primaryNamespace}#SUBTASK`
							},
							limit: Infinity,
							prefix: true,
							select: ['id', 'namespace', 'type']
						})
					]
				: [
						// query all tasks' children
						this.db.tasks.query({
							item: {
								primaryId: task.id,
								primaryNamespace: task.primaryNamespace
							},
							limit: Infinity,
							select: ['id', 'namespace', 'type']
						})
					],
			(reduction, { items }) => {
				reduction = reduction.concat(
					_.map(items, item => {
						return {
							id: item.id,
							namespace: item.namespace,
							type: item.type
						};
					})
				);

				return reduction;
			},
			keys
		);

		let promiseTasks: (() => Promise<TransactWriteCommandOutput[]>)[] = [];

		for (const chunk of _.chunk(keys, 100)) {
			promiseTasks = [
				...promiseTasks,
				() => {
					const transactItems = _.map(chunk, ({ id, namespace, type }) => {
						if (type === 'SUBTASK') {
							if (status !== 'DISABLED') {
								return null;
							}

							return {
								Delete: {
									Key: { id, namespace },
									TableName: this.db.tasks.table
								}
							};
						}

						return {
							Update: {
								ExpressionAttributeNames: {
									'#status': 'status',
									'#ts': '__ts',
									'#updatedAt': '__updatedAt'
								},
								ExpressionAttributeValues: {
									':status': status,
									':ts': now,
									':updatedAt': date.toISOString()
								},
								Key: { id, namespace },
								TableName: this.db.tasks.table,
								UpdateExpression: 'SET #status = :status, #ts = :ts, #updatedAt = :updatedAt'
							}
						};
					});

					return this.db.tasks.transaction({
						TransactItems: _.compact(transactItems)
					});
				}
			];
		}

		await promiseAllSettled(promiseTasks, this.maxConcurrency);

		return (await this.getTaskInternal(
			{
				id: task.id,
				namespace: task.namespace
			},
			true
		))!;
	}

	private async setTaskError(input: Hooks.SetTaskErrorInput): Promise<Hooks.Task> {
		try {
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
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				throw new TaskException('Task is not in a valid state');
			}

			throw err;
		}
	}

	private async setTaskLock(input: Hooks.SetTaskLockInput) {
		try {
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
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				throw new TaskException('Task is not in a valid state');
			}

			throw err;
		}
	}

	private async setTaskSuccess(input: Hooks.SetTaskSuccessInput) {
		try {
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
		} catch (err) {
			if (err instanceof ConditionalCheckFailedException) {
				throw new TaskException('Task is not in a valid state');
			}

			throw err;
		}
	}

	async trigger(input?: Hooks.TriggerInput, date = new Date()): Promise<{ processed: number; errors: number }> {
		let result = { processed: 0, errors: 0 };
		let queryActiveTasksOptions: Hooks.QueryActiveTasksInput = {
			date,
			onChunk: async () => {}
		};

		if (_.size(input) > 0) {
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
						if (_.size(items) === 0) {
							return;
						}

						const callWebhookInput: Hooks.CallWebhookInput = {
							conditionData: args.conditionData || null,
							eventDelayDebounce: args.eventDelayDebounce || null,
							eventDelayUnit: args.eventDelayUnit || null,
							eventDelayValue: args.eventDelayValue || null,
							forkId: args.forkId || null,
							forkOnly: args.forkOnly || false,
							date,
							executionType: 'EVENT',
							keys: items,
							requestBody: args.requestBody || null,
							requestHeaders: args.requestHeaders || null,
							requestMethod: args.requestMethod || null,
							requestUrl: args.requestUrl || null,
							ruleId: args.ruleId || null
						};

						if (this.webhookChunkSize > 0) {
							// Process in chunks if webhookChunkSize is set
							let promiseTasks: (() => Promise<void>)[] = [];

							for (const chunk of _.chunk(items, this.webhookChunkSize)) {
								const promiseTask = async () => {
									if (this.customWebhookCall) {
										const res = await this.customWebhookCall({
											...callWebhookInput,
											keys: chunk
										});
										result.processed += _.size(res);
									} else {
										const res = await this.callWebhook({
											...callWebhookInput,
											keys: chunk
										});
										result.processed += _.size(res);
									}
								};

								promiseTasks = [...promiseTasks, promiseTask];
							}

							await promiseAllSettled(promiseTasks, this.maxConcurrency);
						} else {
							// Original behavior - process all items at once
							if (this.customWebhookCall) {
								const res = await this.customWebhookCall(callWebhookInput);
								result.processed += _.size(res);
							} else {
								const res = await this.callWebhook(callWebhookInput);
								result.processed += _.size(res);
							}
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
						conditionData: null,
						eventDelayDebounce: null,
						eventDelayUnit: null,
						eventDelayValue: null,
						forkId: null,
						forkOnly: false,
						date,
						executionType: 'SCHEDULED',
						keys: items,
						requestBody: null,
						requestHeaders: null,
						requestMethod: null,
						requestUrl: null,
						ruleId: null
					};

					if (this.webhookChunkSize > 0) {
						let promiseTasks: (() => Promise<void>)[] = [];

						// Process in chunks if webhookChunkSize is set
						for (const chunk of _.chunk(items, this.webhookChunkSize)) {
							const promiseTask = async () => {
								if (this.customWebhookCall) {
									const res = await this.customWebhookCall({
										...callWebhookInput,
										keys: chunk
									});
									result.processed += _.size(res);
								} else {
									const res = await this.callWebhook({
										...callWebhookInput,
										keys: chunk
									});
									result.processed += _.size(res);
								}
							};

							promiseTasks = [...promiseTasks, promiseTask];
						}

						await promiseAllSettled(promiseTasks, this.maxConcurrency);
					} else {
						// Original behavior - process all items at once
						if (this.customWebhookCall) {
							const res = await this.customWebhookCall(callWebhookInput);
							result.processed += _.size(res);
						} else {
							const res = await this.callWebhook(callWebhookInput);
							result.processed += _.size(res);
						}
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

	async updateTask(input: Hooks.UpdateTaskInput): Promise<Hooks.Task> {
		const args = await schema.updateTaskInput.parseAsync(input);
		const date = new Date();
		const now = _.now();
		const task = await this.getTaskInternal(
			args.fork
				? {
						id: await this.uuidFromString(args.id),
						namespace: `${args.namespace}#FORK`
					}
				: {
						id: args.id,
						namespace: args.namespace
					}
		);

		if (!task) {
			throw new TaskException('Task not found');
		}

		if (task.type !== 'PRIMARY' && task.type !== 'FORK') {
			throw new TaskException('Task must be a primary or fork task');
		}

		let keys: Dynamodb.MultiResponse<Hooks.TaskKeys> = {
			count: 0,
			items: [],
			lastEvaluatedKey: null
		};

		if (task.type === 'FORK') {
			// query all forks
			keys = await this.db.tasks.query({
				item: {
					id: task.id,
					namespace: `${task.primaryNamespace}#FORK`
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});
		} else {
			// query all tasks' children
			keys = await this.db.tasks.query({
				attributeNames: { '#type': 'type' },
				attributeValues: {
					':fork': 'FORK',
					':primary': 'PRIMARY'
				},
				filterExpression: '#type = :fork OR #type = :primary',
				item: {
					primaryId: task.id,
					primaryNamespace: task.primaryNamespace
				},
				limit: Infinity,
				select: ['id', 'namespace']
			});
		}

		let promiseTasks: (() => Promise<TransactWriteCommandOutput[]>)[] = [];
		let updateProperties = _.pick(args, [
			'concurrency',
			'conditionFilter',
			'description',
			'eventDelayDebounce',
			'eventDelayUnit',
			'eventDelayValue',
			'eventPattern',
			'noAfter',
			'noBefore',
			'repeatInterval',
			'repeatMax',
			'repeatUnit',
			'requestBody',
			'requestHeaders',
			'requestMethod',
			'requestUrl',
			'rescheduleOnEvent',
			'retryLimit',
			'ruleId',
			'scheduledDate',
			'title'
		]);

		let updateAttributes = _.reduce(
			updateProperties,
			(reduction, value, key) => {
				if (key === 'noAfter' || key === 'noBefore' || key === 'scheduledDate') {
					value = new Date(value as string).toISOString();
				}

				reduction.attributeNames[`#${key}`] = key;
				reduction.attributeValues[`:${key}`] = value;
				reduction.expression = reduction.expression.concat(`#${key} = :${key}`);

				if (key === 'eventPattern') {
					reduction.attributeNames['#namespace__eventPattern'] = 'namespace__eventPattern';
					reduction.attributeValues[':namespace__eventPattern'] = `${task.primaryNamespace}#${value}`;
					reduction.expression = reduction.expression.concat('#namespace__eventPattern = :namespace__eventPattern');
				}

				return reduction;
			},
			{
				attributeNames: {
					'#ts': '__ts',
					'#updatedAt': '__updatedAt'
				},
				attributeValues: {
					':ts': now,
					':updatedAt': date.toISOString()
				},
				expression: ['#ts = :ts', '#updatedAt = :updatedAt']
			} as {
				attributeNames: Record<string, string>;
				attributeValues: Record<string, any>;
				expression: string[];
			}
		);

		for (const chunk of _.chunk(keys.items, 25)) {
			promiseTasks = [
				...promiseTasks,
				() => {
					const transactItems = _.map(chunk, ({ id, namespace }) => {
						return {
							Update: {
								ExpressionAttributeNames: updateAttributes.attributeNames,
								ExpressionAttributeValues: updateAttributes.attributeValues,
								Key: { id, namespace },
								TableName: this.db.tasks.table,
								UpdateExpression: `SET ${updateAttributes.expression.join(', ')}`
							}
						};
					});

					return this.db.tasks.transaction({
						TransactItems: _.compact(transactItems)
					});
				}
			];
		}

		await promiseAllSettled(promiseTasks, this.maxConcurrency);

		return (await this.getTaskInternal(
			{
				id: task.id,
				namespace: task.namespace
			},
			true
		))!;
	}

	private uuid(): string {
		return crypto.randomUUID();
	}

	async uuidFromString(input: string): Promise<string> {
		const encoder = new TextEncoder();
		const data = encoder.encode(input);

		const hashBuffer = await crypto.subtle.digest('SHA-256', data);
		const hashArray = Array.from(new Uint8Array(hashBuffer));
		const hashHex = hashArray
			.map(b => {
				return b.toString(16).padStart(2, '0');
			})
			.join('');
		const uuidBytes = hashHex.slice(0, 32);

		// Format to 8-4-4-4-12
		const segments = [
			uuidBytes.slice(0, 8),
			uuidBytes.slice(8, 12),
			'4' + uuidBytes.slice(13, 16),
			((parseInt(uuidBytes[16], 16) & 0x3) | 0x8).toString(16) + uuidBytes.slice(17, 20),
			uuidBytes.slice(20, 32)
		];

		return segments.join('-');
	}
}

export { MINUTE_IN_MS, HOUR_IN_MS, DAY_IN_MS, SUBTASK_TTL_IN_MS, TaskException, taskShape };
export default Hooks;
