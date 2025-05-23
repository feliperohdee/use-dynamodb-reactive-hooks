import _ from 'lodash';
import FilterCriteria from 'use-filter-criteria';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';

const isFutureDate = (date: string) => {
	return new Date(date) > new Date(_.now() - 1000);
};

const date1IsAfterDate2 = (date1: string, date2: string) => {
	return new Date(date1) > new Date(date2);
};

const optionalRequestInput = z
	.object({
		requestBody: z.record(z.any()).nullable(),
		requestHeaders: z.record(z.string()).nullable(),
		requestMethod: Webhooks.schema.request.shape.method.nullable(),
		requestUrl: z.string().url().nullable()
	})
	.partial();

const taskExecutionType = z.enum(['EVENT', 'SCHEDULED']);
const taskKeys = z.object({ id: z.string(), namespace: z.string() });
const taskStatus = z.enum(['ACTIVE', 'DISABLED', 'MAX-ERRORS-REACHED', 'MAX-REPEAT-REACHED', 'PROCESSING']);
const taskType = z.enum(['PRIMARY', 'FORK', 'SUBTASK']);
const timeUnit = z.enum(['minutes', 'hours', 'days']);

const task = z.object({
	__createdAt: z.string().datetime(),
	__ts: z.number(),
	__updatedAt: z.string().datetime(),
	concurrency: z.boolean(),
	conditionFilter: FilterCriteria.schema.matchInput.nullable(),
	description: z.string(),
	eventDelayDebounce: z.boolean(),
	eventDelayUnit: timeUnit,
	eventDelayValue: z.number().min(0),
	eventPattern: z.string(),
	firstErrorDate: z.union([z.string().datetime(), z.literal('')]),
	firstExecutionDate: z.union([z.string().datetime(), z.literal('')]),
	firstScheduledDate: z.union([z.string().datetime(), z.literal('')]),
	forkId: z.string(),
	id: z.string(),
	lastError: z.string(),
	lastErrorDate: z.union([z.string().datetime(), z.literal('')]),
	lastErrorExecutionType: z.union([z.literal(''), taskExecutionType]),
	lastExecutionDate: z.union([z.string().datetime(), z.literal('')]),
	lastExecutionType: z.union([z.literal(''), taskExecutionType]),
	lastResponseBody: z.string(),
	lastResponseHeaders: z.record(z.string()).nullable(),
	lastResponseStatus: z.number(),
	namespace: z.string(),
	namespace__eventPattern: z.union([z.string(), z.literal('-')]),
	noAfter: z.union([z.string().datetime(), z.literal('')]),
	noBefore: z.union([z.string().datetime(), z.literal('')]),
	pid: z.string(),
	primaryId: z.string(),
	primaryNamespace: z.string(),
	repeatInterval: z.number().min(0),
	repeatMax: z.number().min(0),
	repeatUnit: timeUnit,
	requestBody: z.record(z.any()).nullable(),
	requestHeaders: z.record(z.string()).nullable(),
	requestMethod: Webhooks.schema.request.shape.method.default('GET'),
	requestUrl: z.string().url(),
	rescheduleOnEvent: z.boolean().default(true),
	retryLimit: z.number().min(0).default(3),
	ruleId: z.string(),
	scheduledDate: z.union([z.string().datetime(), z.literal('-')]),
	status: taskStatus.default('ACTIVE'),
	totalErrors: z.number(),
	totalExecutions: z.number(),
	totalFailedExecutions: z.number(),
	totalSuccessfulExecutions: z.number(),
	title: z.string(),
	ttl: z.number().min(0),
	type: taskType,
	webhookLogPrefix: z.string()
});

const taskRuleResult = z.object({
	requestBody: z.record(z.any()).nullable(),
	requestHeaders: z.record(z.string()).nullable(),
	requestMethod: Webhooks.schema.request.shape.method.nullable(),
	requestUrl: z.string().url().nullable()
});

const taskRule = z
	.function()
	.args(z.object({ task }))
	.returns(z.union([z.array(taskRuleResult), z.promise(z.array(taskRuleResult))]));

const callWebhookInput = z
	.object({
		conditionData: z.record(z.any()).nullable(),
		date: z.date(),
		eventDelayDebounce: z.boolean().nullable(),
		eventDelayUnit: timeUnit.nullable(),
		eventDelayValue: z.number().min(0).nullable(),
		executionType: taskExecutionType,
		forkId: z.string().nullable(),
		forkOnly: z.boolean(),
		forkOverwrite: z.boolean().default(false),
		keys: z.array(taskKeys),
		ruleId: z.string().nullable()
	})
	.merge(optionalRequestInput);

const checkExecuteTaskInput = z.object({
	date: z.date(),
	task
});

const debugConditionInput = z.object({
	conditionData: z.record(z.any()).nullable(),
	id: z.string(),
	namespace: z.string()
});

const deleteInput = z.object({
	fork: z.boolean().default(false),
	id: z.string(),
	namespace: z.string()
});

const fetchLogsInput = Webhooks.schema.fetchLogsInput;
const fetchTasksByPrimaryTaskInput = z.object({
	chunkLimit: z.number().min(1).optional(),
	desc: z.boolean().default(false),
	limit: z.number().min(1).default(100),
	onChunk: z
		.function()
		.args(
			z.object({
				count: z.number(),
				items: z.array(
					task.pick({
						id: true,
						namespace: true,
						primaryId: true,
						primaryNamespace: true,
						type: true
					})
				)
			})
		)
		.returns(z.promise(z.void()))
		.optional(),
	primaryId: z.string().optional(),
	primaryIdPrefix: z.boolean().default(false),
	primaryNamespace: z.string(),
	startKey: z.record(z.any()).nullable().default(null)
});

const fetchTasksInput = z
	.object({
		chunkLimit: z.number().min(1).optional(),
		desc: z.boolean().default(false),
		eventPattern: z.string().optional(),
		eventPatternPrefix: z.boolean().default(false),
		fork: z.boolean().default(false),
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
					items: z.array(
						task.pick({
							description: true,
							eventPattern: true,
							id: true,
							namespace: true,
							scheduledDate: true,
							status: true,
							title: true
						})
					)
				})
			)
			.returns(z.promise(z.void()))
			.optional(),
		startKey: z.record(z.any()).nullable().default(null),
		status: taskStatus.nullable().optional(),
		subTask: z.boolean().default(false),
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

const fetchTasksResponse = z.object({
	count: z.number(),
	items: z.array(
		task.pick({
			description: true,
			eventPattern: true,
			id: true,
			namespace: true,
			scheduledDate: true,
			status: true,
			title: true
		})
	),
	lastEvaluatedKey: z.record(z.any()).nullable()
});

const getTaskInput = z.object({
	fork: z.boolean().default(false),
	id: z.string(),
	namespace: z.string()
});

const log = Webhooks.schema.log;
const queryActiveTasksInputBase = z.object({
	date: z.date(),
	onChunk: z
		.function()
		.args(
			z.object({
				count: z.number(),
				items: z.array(taskKeys)
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
		namespace: z.string()
	}),
	queryActiveTasksInputBase
]);

const registerForkTaskInput = z.object({
	forkId: z.string(),
	overwrite: z.boolean().default(false),
	primaryTask: task
});

const registerTask = task
	.extend({
		noAfter: z.union([z.literal(''), z.string().datetime({ offset: true }).refine(isFutureDate, 'noAfter cannot be in the past')]),
		noBefore: z.union([z.literal(''), z.string().datetime({ offset: true }).refine(isFutureDate, 'noBefore cannot be in the past')]),
		overwrite: z.boolean().default(false),
		scheduledDate: z.union([
			z.literal(''),
			z.string().datetime({ offset: true }).refine(isFutureDate, 'scheduledDate cannot be in the past')
		])
	})
	.omit({
		__createdAt: true,
		__ts: true,
		__updatedAt: true,
		firstErrorDate: true,
		firstExecutionDate: true,
		firstScheduledDate: true,
		forkId: true,
		lastError: true,
		lastErrorDate: true,
		lastErrorExecutionType: true,
		lastExecutionDate: true,
		lastExecutionType: true,
		lastResponseBody: true,
		lastResponseHeaders: true,
		lastResponseStatus: true,
		namespace__eventPattern: true,
		pid: true,
		primaryId: true,
		primaryNamespace: true,
		status: true,
		totalErrors: true,
		totalExecutions: true,
		totalFailedExecutions: true,
		totalSuccessfulExecutions: true,
		ttl: true,
		type: true
	})
	.partial({
		concurrency: true,
		conditionFilter: true,
		description: true,
		eventDelayDebounce: true,
		eventDelayUnit: true,
		eventDelayValue: true,
		eventPattern: true,
		id: true,
		noAfter: true,
		noBefore: true,
		repeatInterval: true,
		repeatMax: true,
		repeatUnit: true,
		requestBody: true,
		requestHeaders: true,
		requestMethod: true,
		rescheduleOnEvent: true,
		ruleId: true,
		scheduledDate: true,
		title: true,
		webhookLogPrefix: true
	})
	.refine(
		data => {
			if (data.noAfter && data.noBefore) {
				return date1IsAfterDate2(data.noAfter, data.noBefore);
			}

			return true;
		},
		{
			message: 'noAfter must be after noBefore',
			path: ['noAfter']
		}
	);

const setTaskActiveInput = z.object({
	active: z.boolean(),
	fork: z.boolean().default(false),
	id: z.string(),
	namespace: z.string()
});

const setTaskErrorInput = z.object({
	error: z.instanceof(Error),
	executionType: taskExecutionType,
	pid: z.string(),
	task
});

const setTaskLockInput = z.object({
	pid: z.string(),
	task
});

const setTaskSuccessInput = z.object({
	executionType: taskExecutionType,
	log: Webhooks.schema.log.pick({
		responseBody: true,
		responseHeaders: true,
		responseOk: true,
		responseStatus: true
	}),
	pid: z.string(),
	task
});

const triggerInput = z.union([
	z
		.object({
			conditionData: z.record(z.any()).optional(),
			eventDelayDebounce: z.boolean().optional(),
			eventDelayUnit: timeUnit.optional(),
			eventDelayValue: z.number().min(0).optional(),
			forkId: z.string().optional(),
			forkOnly: z.boolean().optional(),
			forkOverwrite: z.boolean().default(false),
			id: z.string(),
			namespace: z.string(),
			ruleId: z.string().optional()
		})
		.merge(optionalRequestInput),
	z
		.object({
			conditionData: z.record(z.any()).optional(),
			eventDelayDebounce: z.boolean().optional(),
			eventDelayUnit: timeUnit.optional(),
			eventDelayValue: z.number().min(0).optional(),
			eventPattern: z.string(),
			eventPatternPrefix: z.boolean().default(false),
			forkId: z.string().optional(),
			forkOnly: z.boolean().optional(),
			forkOverwrite: z.boolean().default(false),
			namespace: z.string(),
			ruleId: z.string().optional()
		})
		.merge(optionalRequestInput)
]);

const updateTaskInput = z
	.object({
		fork: z.boolean().default(false),
		id: z.string(),
		namespace: z.string(),
		concurrency: z.boolean().optional(),
		conditionFilter: FilterCriteria.schema.matchInput.optional(),
		description: z.string().optional(),
		eventDelayDebounce: z.boolean().optional(),
		eventDelayUnit: timeUnit.optional(),
		eventDelayValue: z.number().min(0).optional(),
		eventPattern: z.string().optional(),
		noAfter: z.string().datetime({ offset: true }).refine(isFutureDate, 'noAfter cannot be in the past').optional(),
		noBefore: z.string().datetime({ offset: true }).refine(isFutureDate, 'noBefore cannot be in the past').optional(),
		repeatInterval: z.number().min(0).optional(),
		repeatMax: z.number().min(0).optional(),
		repeatUnit: timeUnit.optional(),
		requestBody: z.record(z.any()).optional(),
		requestHeaders: z.record(z.string()).optional(),
		requestMethod: Webhooks.schema.request.shape.method.optional(),
		requestUrl: z.string().url().optional(),
		rescheduleOnEvent: z.boolean().optional(),
		retryLimit: z.number().min(0).optional(),
		ruleId: z.string().optional(),
		scheduledDate: z.string().datetime({ offset: true }).refine(isFutureDate, 'scheduledDate cannot be in the past').optional(),
		title: z.string().optional(),
		webhookLogPrefix: z.string().optional()
	})
	.refine(
		data => {
			if (data.noAfter && data.noBefore) {
				return date1IsAfterDate2(data.noAfter, data.noBefore);
			}

			return true;
		},
		{
			message: 'noAfter must be after noBefore',
			path: ['noAfter']
		}
	);

export {
	callWebhookInput,
	checkExecuteTaskInput,
	debugConditionInput,
	deleteInput,
	fetchLogsInput,
	fetchTasksByPrimaryTaskInput,
	fetchTasksInput,
	fetchTasksResponse,
	getTaskInput,
	log,
	queryActiveTasksInput,
	registerForkTaskInput,
	registerTask,
	setTaskErrorInput,
	setTaskLockInput,
	setTaskSuccessInput,
	setTaskActiveInput,
	task,
	taskExecutionType,
	taskKeys,
	taskRule,
	taskRuleResult,
	taskStatus,
	taskType,
	timeUnit,
	triggerInput,
	updateTaskInput
};
