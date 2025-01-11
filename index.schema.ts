import _ from 'lodash';
import UseFilterCriteria from 'use-filter-criteria';
import Webhooks from 'use-dynamodb-webhooks';
import z from 'zod';

const timeUnit = z.enum(['minutes', 'hours', 'days']);
const taskExecutionType = z.enum(['MANUAL', 'SCHEDULED']);
const taskStatus = z.enum(['ACTIVE', 'MAX_ERRORS_REACHED', 'MAX_REPEAT_REACHED', 'SUSPENDED', 'PROCESSING']);
const taskType = z.enum(['REGULAR', 'DELAY-DEBOUNCE', 'DELAY']);

const task = z.object({
	__createdAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	__namespace__manualEventPattern: z.union([z.string(), z.literal('-')]),
	__ts: z.number().default(() => {
		return _.now();
	}),
	__updatedAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	concurrency: z.boolean(),
	firstErrorDate: z.union([z.string().datetime(), z.literal('')]),
	firstExecutionDate: z.union([z.string().datetime(), z.literal('')]),
	firstScheduledDate: z.union([z.string().datetime(), z.literal('')]),
	id: z.string(),
	idPrefix: z.string(),
	lastError: z.string(),
	lastErrorDate: z.union([z.string().datetime(), z.literal('')]),
	lastErrorExecutionType: z.union([z.literal(''), taskExecutionType]),
	lastExecutionDate: z.union([z.string().datetime(), z.literal('')]),
	lastExecutionType: z.union([z.literal(''), taskExecutionType]),
	lastResponseBody: z.string(),
	lastResponseHeaders: z.record(z.string()),
	lastResponseStatus: z.number(),
	manualDelayDebounce: z.boolean(),
	manualDelayUnit: timeUnit,
	manualDelayValue: z.number().min(0),
	manualEventPattern: z.string(),
	manualReschedule: z.boolean().default(true),
	namespace: z.string(),
	noAfter: z.union([z.string().datetime(), z.literal('')]),
	noBefore: z.union([z.string().datetime(), z.literal('')]),
	parentId: z.string(),
	parentNamespace: z.string(),
	repeatInterval: z.number().min(0),
	repeatMax: z.number().min(0).default(0),
	repeatUnit: timeUnit,
	requestBody: z.record(z.any()).nullable(),
	requestHeaders: z.record(z.string()).nullable(),
	requestMethod: Webhooks.schema.request.shape.method.default('GET'),
	requestUrl: z.string().url(),
	retryLimit: z.number().min(0).default(3),
	scheduledDate: z.union([z.string().datetime(), z.literal('-')]),
	status: taskStatus.default('ACTIVE'),
	totalErrors: z.number(),
	totalExecutions: z.number(),
	totalFailedExecutions: z.number(),
	totalSuccessfulExecutions: z.number(),
	type: taskType
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
		__namespace__manualEventPattern: true,
		__ts: true,
		__updatedAt: true,
		id: true,
		firstErrorDate: true,
		firstExecutionDate: true,
		firstScheduledDate: true,
		lastError: true,
		lastErrorDate: true,
		lastErrorExecutionType: true,
		lastExecutionDate: true,
		lastExecutionType: true,
		lastResponseBody: true,
		lastResponseHeaders: true,
		lastResponseStatus: true,
		parentId: true,
		parentNamespace: true,
		status: true,
		totalErrors: true,
		totalExecutions: true,
		totalFailedExecutions: true,
		totalSuccessfulExecutions: true,
		type: true
	})
	.partial({
		concurrency: true,
		idPrefix: true,
		manualDelayDebounce: true,
		manualDelayUnit: true,
		manualDelayValue: true,
		manualEventPattern: true,
		manualReschedule: true,
		noAfter: true,
		noBefore: true,
		repeatInterval: true,
		repeatMax: true,
		repeatUnit: true,
		requestBody: true,
		requestHeaders: true,
		requestMethod: true,
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
	date: z.date(),
	delayDebounceId: z.string().optional(),
	executionType: taskExecutionType,
	tasks: z.array(task)
});

const deleteInput = z.object({
	id: z.string(),
	namespace: z.string()
});

const fetchInput = z
	.object({
		chunkLimit: z.number().min(1).optional(),
		delayDebounceId: z.string().optional(),
		desc: z.boolean().default(false),
		manualEventPattern: z.string().optional(),
		manualEventPatternPrefix: z.boolean().default(false),
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
		toScheduledDate: z.string().datetime({ offset: true }).optional(),
		type: taskType.optional()
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
const getTaskInput = z.object({
	delayDebounceId: z.string().optional(),
	id: z.string(),
	namespace: z.string(),
	type: taskType.optional()
});

const log = Webhooks.schema.log;
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
		manualEventPattern: z.string(),
		manualEventPatternPrefix: z.boolean().default(false),
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
	executionType: taskExecutionType,
	pid: z.string(),
	task
});

const setTaskLockInput = z.object({
	date: z.date(),
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

const subTaskInput = z.object({
	delayDebounce: z.boolean().optional(),
	delayDebounceId: z.string().optional(),
	delayUnit: timeUnit,
	delayValue: z.number().min(0),
	id: z.string(),
	namespace: z.string(),
	requestBody: z.record(z.any()).nullable(),
	requestHeaders: z.record(z.string()).nullable(),
	requestMethod: Webhooks.schema.request.shape.method.default('GET'),
	requestUrl: z.string().url()
});

const triggerInput = z.union([
	z.object({
		conditionData: z.record(z.any()).optional(),
		conditionFilter: UseFilterCriteria.schema.matchInput.optional(),
		delayDebounceId: z.string().optional(),
		id: z.string(),
		idPrefix: z.boolean().default(false),
		namespace: z.string(),
		requestBody: z.record(z.any()).optional(),
		requestHeaders: z.record(z.string()).optional(),
		requestMethod: Webhooks.schema.request.shape.method.optional(),
		requestUrl: z.string().url().optional()
	}),
	z.object({
		conditionData: z.record(z.any()).optional(),
		conditionFilter: UseFilterCriteria.schema.matchInput.optional(),
		delayDebounceId: z.string().optional(),
		manualEventPattern: z.string(),
		manualEventPatternPrefix: z.boolean().default(false),
		namespace: z.string(),
		requestBody: z.record(z.any()).optional(),
		requestHeaders: z.record(z.string()).optional(),
		requestMethod: Webhooks.schema.request.shape.method.optional(),
		requestUrl: z.string().url().optional()
	})
]);

export default {
	callWebhookInput,
	deleteInput,
	fetchInput,
	fetchLogsInput,
	getTaskInput,
	log,
	queryActiveTasksInput,
	setTaskErrorInput,
	setTaskLockInput,
	setTaskSuccessInput,
	subTaskInput,
	task,
	taskExecutionType,
	taskInput,
	taskStatus,
	taskType,
	timeUnit,
	triggerInput
};
