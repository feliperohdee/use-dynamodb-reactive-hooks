import _ from 'lodash';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { promiseAllSettled } from 'use-async-helpers';
import Dynamodb, { concatConditionExpression, concatUpdateExpression } from 'use-dynamodb';
import HttpError from 'use-http-error';
import UseFilterCriteria from 'use-filter-criteria';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';
import zDefault from 'zod-default-instance';

import schema from './index.schema';

const DEFAULT_CONCURRENCY = 25;
const MINUTE_IN_MS = 60 * 1000;
const HOUR_IN_MS = 60 * MINUTE_IN_MS;
const DAY_IN_MS = 24 * HOUR_IN_MS;

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
	export type DeleteInput = z.input<typeof schema.deleteInput>;
	export type FetchInput = z.input<typeof schema.fetchInput>;
	export type FetchLogsInput = z.input<typeof schema.fetchLogsInput>;
	export type GetInput = z.input<typeof schema.getTaskInput>;
	export type Log = z.infer<typeof schema.log>;
	export type QueryActiveTasksInput = z.input<typeof schema.queryActiveTasksInput>;
	export type SetTaskErrorInput = z.input<typeof schema.setTaskErrorInput>;
	export type SetTaskLockInput = z.input<typeof schema.setTaskLockInput>;
	export type SetTaskSuccessInput = z.input<typeof schema.setTaskSuccessInput>;
	export type SubTaskInput = z.input<typeof schema.subTaskInput>;
	export type Task = z.infer<typeof schema.task>;
	export type TaskExecutionType = z.infer<typeof schema.taskExecutionType>;
	export type TaskInput = z.input<typeof schema.taskInput>;
	export type TaskStatus = z.infer<typeof schema.taskStatus>;
	export type TaskType = z.infer<typeof schema.taskType>;
	export type TimeUnit = z.infer<typeof schema.timeUnit>;
	export type TriggerInput = z.input<typeof schema.triggerInput>;
	export type TriggerInputConditionFilter = UseFilterCriteria.MatchInput;
}

const taskShape = (input: Partial<Hooks.Task | Hooks.TaskInput>): Hooks.Task => {
	return zDefault(schema.task, input);
};

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
				// used to fetch tasks by namespace / manualEventPattern
				{
					name: 'namespace-manual-event-pattern',
					partition: 'namespace',
					partitionType: 'S',
					sort: 'manualEventPattern',
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
				// used to trigger tasks by status / namespace#manualEventPattern
				{
					name: 'status-namespace-manual-event-pattern',
					partition: 'status',
					partitionType: 'S',
					sort: '__namespace__manualEventPattern',
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

		this.customWebhookCall = options.webhookCaller;
		this.db = { tasks };
		this.maxConcurrency = options.maxConcurrency || DEFAULT_CONCURRENCY;
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

		for (const item of args.tasks) {
			let pid = this.uuid();
			let task: Hooks.Task = taskShape(item);

			const promiseTask = async () => {
				try {
					// handle delay
					if (args.executionType === 'MANUAL' && task.manualDelayValue > 0) {
						return await this.registerSubTask({
							delayDebounce: task.manualDelayDebounce,
							delayUnit: task.manualDelayUnit,
							delayValue: task.manualDelayValue,
							id: task.id,
							namespace: task.namespace,
							requestBody: task.requestBody,
							requestHeaders: task.requestHeaders,
							requestMethod: task.requestMethod,
							requestUrl: task.requestUrl
						});
					}

					// handle subTasks
					let subTaskRequest: null | {
						requestBody: Hooks.Task['requestBody'];
						requestHeaders: Hooks.Task['requestHeaders'];
						requestMethod: Hooks.Task['requestMethod'];
						requestUrl: Hooks.Task['requestUrl'];
						type: Hooks.Task['type'];
					} = null;

					if (task.parentId && task.parentNamespace) {
						// delete subTask
						await this.db.tasks.delete({
							filter: {
								item: {
									namespace: task.namespace,
									id: task.id
								}
							}
						});

						const parentTask = await this.getTask({
							id: task.parentId,
							namespace: task.parentNamespace
						});

						if (!parentTask) {
							return null;
						}

						subTaskRequest = {
							requestBody: task.requestBody,
							requestHeaders: task.requestHeaders,
							requestMethod: task.requestMethod,
							requestUrl: task.requestUrl,
							type: task.type
						};

						task = parentTask;
					}

					// concurrency is disabled by default (exclusive execution)
					if (!task.concurrency) {
						// update task status to processing and set pid disallowing concurrency
						task = await this.setTaskLock({
							date: args.date,
							pid,
							task
						});
					}

					const log = await this.webhooks.trigger({
						idPrefix: _.compact([task.idPrefix, args.executionType, subTaskRequest?.type]).join('#'),
						namespace: task.namespace,
						requestBody: subTaskRequest?.requestBody || task.requestBody,
						requestHeaders: subTaskRequest?.requestHeaders || task.requestHeaders,
						requestMethod: subTaskRequest?.requestMethod || task.requestMethod,
						requestUrl: subTaskRequest?.requestUrl || task.requestUrl,
						retryLimit: task.retryLimit
					});

					task = await this.setTaskSuccess({
						executionType: args.executionType,
						log,
						pid,
						task
					});
				} catch (err) {
					if (err instanceof ConditionalCheckFailedException) {
						return null;
					}

					task = await this.setTaskError({
						error: err as Error,
						executionType: args.executionType,
						pid,
						task
					});
				}

				return task;
			};

			if (promiseTask) {
				promiseTasks = [...promiseTasks, promiseTask];
			}
		}

		const res = await promiseAllSettled(promiseTasks, this.maxConcurrency);

		return _.compact(_.map(res, 'value'));
	}

	async clearTasks(namespace: string): Promise<{ count: number }> {
		return this.db.tasks.clear(namespace);
	}

	async deleteTask(input: Hooks.DeleteInput): Promise<Hooks.Task | null> {
		const args = await schema.deleteInput.parseAsync(input);
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
		args = await schema.fetchInput.parseAsync(args);

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
		const args = await schema.fetchInput.parseAsync(input);

		if (args.type === 'DEBOUNCED') {
			args.namespace = `${args.namespace}#DEBOUNCED`;
		}

		if (args.type === 'DELAYED') {
			args.namespace = `${args.namespace}#DELAYED`;
		}

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
			manualEventPattern: '',
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
		if (args.manualEventPattern) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#manualEventPattern': 'manualEventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':manualEventPattern': args.manualEventPattern
			};

			filters.manualEventPattern = args.manualEventPatternPrefix
				? 'begins_with(#manualEventPattern, :manualEventPattern)'
				: '#manualEventPattern = :manualEventPattern';
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
		if (args.manualEventPattern && !args.status) {
			// omit [manualEventPattern] filter
			filters.manualEventPattern = '';

			queryOptions.index = 'namespace-manual-event-pattern';
			queryOptions.queryExpression = [
				'#namespace = :namespace',
				args.manualEventPatternPrefix
					? 'begins_with(#manualEventPattern, :manualEventPattern)'
					: '#manualEventPattern = :manualEventPattern'
			].join(' AND ');

			return query(queryOptions);
		}

		// QUERY BY STATUS -> EVENT_PATTERN INDEX
		if (args.manualEventPattern && args.status) {
			// omit [manualEventPattern, status] filters
			filters.manualEventPattern = '';
			filters.status = '';
			queryOptions.attributeNames = _.omit(queryOptions.attributeNames, ['#manualEventPattern', '#namespace']);
			queryOptions.attributeValues = _.omit(queryOptions.attributeValues, [':manualEventPattern', ':namespace']);

			queryOptions.index = 'status-namespace-manual-event-pattern';
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace__manualEventPattern': '__namespace__manualEventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace__manualEventPattern': `${args.namespace}#${args.manualEventPattern}`
			};

			queryOptions.queryExpression = [
				'#status = :status',
				args.manualEventPatternPrefix
					? 'begins_with(#namespace__manualEventPattern, :namespace__manualEventPattern)'
					: '#namespace__manualEventPattern = :namespace__manualEventPattern'
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

	async getTask(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const args = await schema.getTaskInput.parseAsync(input);

		let [id, namespace] = [args.id, args.namespace];
		let prefix = false;
		
		if (args.type === 'DEBOUNCED') {
			namespace = `${args.namespace}#DEBOUNCED`;

			if (args.delayDebounceId) {
				id = _.compact([args.id, args.delayDebounceId]).join('#');
			}
		}

		if (args.type === 'DELAYED') {
			namespace = `${args.namespace}#DELAYED`;
			prefix = true;
		}

		const res = await this.db.tasks.get({
			item: { namespace, id },
			prefix
		});

		return res ? taskShape(res) : null;
	}

	private async queryActiveTasks(args: Hooks.QueryActiveTasksInput): Promise<Dynamodb.MultiResponse<Hooks.Task, false>> {
		args = await schema.queryActiveTasksInput.parseAsync(args);

		const query = async (options: Dynamodb.QueryOptions<Hooks.Task>) => {
			const res = await this.db.tasks.query(options);

			return {
				...res,
				items: _.map(res.items, taskShape)
			};
		};

		const queryOptions: Dynamodb.QueryOptions<Hooks.Task> = {
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
				'attribute_not_exists(#pid)',
				'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
				'(#noBefore = :empty OR :now > #noBefore)',
				'(#noAfter = :empty OR :now < #noAfter)'
			].join(' AND '),
			limit: Infinity,
			onChunk: args.onChunk
		};

		if ('manualEventPattern' in args && args.manualEventPattern) {
			queryOptions.index = 'status-namespace-manual-event-pattern';
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#namespace__manualEventPattern': '__namespace__manualEventPattern'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':namespace__manualEventPattern': `${args.namespace}#${args.manualEventPattern}`
			};

			queryOptions.queryExpression = [
				'#status = :active',
				args.manualEventPatternPrefix
					? 'begins_with(#namespace__manualEventPattern, :namespace__manualEventPattern)'
					: '#namespace__manualEventPattern = :namespace__manualEventPattern'
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

	private async registerSubTask(input: Hooks.SubTaskInput): Promise<Hooks.Task> {
		const args = await schema.subTaskInput.parseAsync(input);

		let [id, namespace] = [args.id, args.namespace];
		let type: 'DELAYED' | 'DEBOUNCED' = 'DELAYED';

		if (args.delayDebounce) {
			id = _.compact([args.id, args.delayDebounceId]).join('#');
			namespace = `${args.namespace}#DEBOUNCED`;
			type = 'DEBOUNCED';
		} else {
			id = [args.id, _.now()].join('#');
			namespace = `${args.namespace}#DELAYED`;
			type = 'DELAYED';
		}

		const scheduledDate = this.calculateNextSchedule(new Date().toISOString(), {
			unit: args.delayUnit,
			value: args.delayValue
		});

		const res = await this.db.tasks.put(
			taskShape({
				...args,
				__namespace__manualEventPattern: '-',
				firstScheduledDate: scheduledDate,
				parentId: args.id,
				parentNamespace: args.namespace,
				id,
				manualEventPattern: '-',
				namespace,
				scheduledDate,
				type
			}),
			{ overwrite: true }
		);

		return _.omit(res, ['__ts']);
	}

	async registerTask(input: Hooks.TaskInput): Promise<Hooks.Task> {
		const args = await schema.taskInput.parseAsync(input);
		const scheduledDate = args.scheduledDate ? new Date(args.scheduledDate).toISOString() : '-';
		const res = await this.db.tasks.put(
			taskShape({
				...args,
				__namespace__manualEventPattern: args.manualEventPattern ? `${args.namespace}#${args.manualEventPattern}` : '-',
				firstErrorDate: '',
				firstExecutionDate: '',
				firstScheduledDate: scheduledDate,
				id: this.uuid(args.idPrefix),
				lastError: '',
				lastErrorDate: '',
				lastExecutionDate: '',
				lastExecutionType: '',
				lastResponseBody: '',
				lastResponseHeaders: {},
				lastResponseStatus: 0,
				manualEventPattern: args.manualEventPattern || '-',
				totalErrors: 0,
				totalExecutions: 0,
				totalFailedExecutions: 0,
				totalSuccessfulExecutions: 0,
				noAfter: args.noAfter ? new Date(args.noAfter).toISOString() : '',
				noBefore: args.noBefore ? new Date(args.noBefore).toISOString() : '',
				scheduledDate
			})
		);

		return _.omit(res, ['__ts']);
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
			`ADD ${['#totalErrors :one'].join(', ')}`,
			`SET ${['#lastErrorExecutionType = :executionType', '#lastError = :error', '#lastErrorDate = :now'].join(', ')}`,
			`REMOVE #pid`
		].join(' ');

		if (args.task.firstErrorDate === '') {
			updateOptions.attributeNames = {
				...updateOptions.attributeNames,
				'#firstErrorDate': 'firstErrorDate'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #firstErrorDate = :now');
		}

		// set MAX_ERRORS_REACHED status if max errors reached
		const nextErrorsCount = args.task.totalErrors + 1;

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
		const args = await schema.setTaskLockInput.parseAsync(input);

		return taskShape(
			await this.db.tasks.update({
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
					':pid': args.pid,
					':processing': 'PROCESSING',
					':zero': 0
				},
				// in case of other process already picked the task while it was being processed
				conditionExpression: [
					'attribute_not_exists(#pid)',
					'#status = :active',
					'(#repeatMax = :zero OR #totalExecutions < #repeatMax)',
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
				':executionType': args.executionType,
				':now': date.toISOString(),
				':one': 1,
				':responseBody': args.log.responseBody,
				':responseHeaders': args.log.responseHeaders,
				':responseStatus': args.log.responseStatus
			},
			filter: {
				item: {
					namespace: args.task.namespace,
					id: args.task.id
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
				'#lastResponseStatus = :responseStatus'
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
			(args.executionType === 'SCHEDULED' || (args.executionType === 'MANUAL' && args.task.manualReschedule)) &&
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
				':maxRepeatReached': 'MAX_REPEAT_REACHED'
			};

			updateOptions.updateExpression = concatUpdateExpression(updateOptions.updateExpression || '', 'SET #status = :maxRepeatReached');
		}

		return taskShape(await this.db.tasks.update(updateOptions));
	}

	async suspendTask(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const task = await this.getTask(input);

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

	async trigger(input?: Hooks.TriggerInput): Promise<{ processed: number; errors: number }> {
		const date = new Date();
		const result = { processed: 0, errors: 0 };

		try {
			let queryActiveTasksOptions: Hooks.QueryActiveTasksInput = {
				date,
				onChunk: async () => {}
			};

			let request: {
				body: Record<string, any>;
				delayDebounceId: string;
				headers: Record<string, any>;
				method: Webhooks.Request['method'] | '';
				url: string;
			} = {
				body: {},
				delayDebounceId: '',
				headers: {},
				method: '',
				url: ''
			};

			if (input) {
				const args = await schema.triggerInput.parseAsync(input);

				if (args.conditionData && args.conditionFilter) {
					if (!(await UseFilterCriteria.match(args.conditionData, args.conditionFilter))) {
						return result;
					}
				}

				request = {
					body: args.requestBody || {},
					headers: args.requestHeaders || {},
					method: args.requestMethod || '',
					url: args.requestUrl || ''
				};

				if ('manualEventPattern' in args && args.manualEventPattern) {
					queryActiveTasksOptions = {
						...queryActiveTasksOptions,
						manualEventPattern: args.manualEventPattern,
						manualEventPatternPrefix: args.manualEventPatternPrefix,
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

			const executionType = _.some(['manualEventPattern', 'id'], key => {
				return key in queryActiveTasksOptions;
			})
				? 'MANUAL'
				: 'SCHEDULED';

			await this.queryActiveTasks({
				...queryActiveTasksOptions,
				onChunk: async ({ items }) => {
					if (executionType === 'MANUAL') {
						items = _.map(items, item => {
							return {
								...item,
								requestBody: {
									...item.requestBody,
									...request.body
								},
								requestHeaders: {
									...item.requestHeaders,
									...request.headers
								},
								requestMethod: request.method || item.requestMethod,
								requestUrl: request.url || item.requestUrl
							};
						});
					}

					if (this.customWebhookCall) {
						const res = await this.customWebhookCall({
							delayDebounceId: queryActiveTasksOptions.delayDebounceId,
							date,
							executionType,
							tasks: items
						});

						result.processed += _.size(res);
					} else {
						const res = await this.callWebhook({
							delayDebounceId: queryActiveTasksOptions.delayDebounceId,
							date,
							executionType,
							tasks: items
						});

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

	async unsuspendTask(input: Hooks.GetInput): Promise<Hooks.Task | null> {
		const task = await this.getTask(input);

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
