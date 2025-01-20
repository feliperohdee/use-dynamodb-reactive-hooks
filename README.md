# Use DynamoDB Reactive Hooks

A TypeScript library that provides a webhook scheduling system using Amazon DynamoDB as the backend. Schedule HTTP webhooks to be triggered at specific times or intervals, with full management and monitoring capabilities.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🚀 Features

- ✅ Register webhooks to be triggered at specific times or intervals.
- 🔁 Support for recurring webhooks with flexible intervals and configurable repeat limits.
- 📊 Webhook status tracking (ACTIVE, PROCESSING, MAX-REPEAT-REACHED, MAX-ERRORS-REACHED, DISABLED).
- 🔄 Automatic retries with configurable max attempts.
- 🕒 Built-in timestamp management.
- 📝 Comprehensive error tracking with detailed error messages and timestamps.
- 🔎 Rich querying capabilities by namespace, status, time ranges, event patterns, and IDs.
- 🏃 Concurrent webhook execution with configurable limits.
- 📦 Batch operations support for efficient updates and deletions.
- 📊 Detailed execution logs and metrics, including response body, headers, and status.
- 🔀 Forking mechanism to create new tasks based on existing ones, with customizable forking behavior.
- ⏱️ Configurable delay mechanism to postpone task execution, with optional debouncing for event-driven delays.
- 🔒 Concurrency control to prevent simultaneous execution of the same task.
- 🧪 Debugging capabilities to test task conditions against provided data.
- 🆔 UUID generation and hashing utilities for task and fork IDs.

## 📦 Installation

```bash
npm install use-dynamodb-reactive-hooks
# or
yarn add use-dynamodb-reactive-hooks
```

## 🛠️ Usage

### Initialization

```typescript
import Hooks from 'use-dynamodb-reactive-hooks';

const hooks = new Hooks({
	accessKeyId: 'YOUR_ACCESS_KEY',
	createTable: true, // Optional: automatically create table
	logsTableName: 'YOUR_LOGS_TABLE_NAME',
	logsTtlInSeconds: 86400, // Optional: TTL for log entries (default: forever)
	maxConcurrency: 25, // Optional: concurrent webhook execution limit (default: 25)
	maxErrors: 5, // Optional: max errors before marking task as MAX-ERRORS-REACHED (default: 5)
	region: 'us-east-1',
	secretAccessKey: 'YOUR_SECRET_KEY',
	tasksTableName: 'YOUR_TABLE_NAME',
	webhookCaller: async (input: Hooks.CallWebhookInput) => {
		// Optional: custom webhook caller implementation
	}
});
```

### Registering Webhooks

```typescript
// Register a simple webhook
const task = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/endpoint',
	scheduledDate: new Date(Date.now() + 60_000).toISOString(), // Execute in 1 minute
	requestMethod: 'POST',
	requestHeaders: {
		'Content-Type': 'application/json'
	},
	requestBody: {
		key: 'value'
	}
});

// Register a recurring webhook
const recurringTask = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/status',
	scheduledDate: new Date(Date.now() + 60_000).toISOString(), // Start in 1 minute
	repeatInterval: 30, // Every 30 minutes
	repeatMax: 5, // Repeat up to 5 times
	repeatUnit: 'minutes'
});

// Register a webhook with event pattern for event-driven triggering
const eventTask = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/event',
	eventPattern: 'user-created'
});

// Register a webhook with forking to create new tasks on trigger
const forkTask = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/fork',
	eventPattern: 'data-updated',
	eventDelayValue: 5, // Delay forked tasks by 5 minutes
	eventDelayUnit: 'minutes'
});

// Register a delayed task with debouncing
const delayedTask = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/delayed',
	eventPattern: 'process-later',
	eventDelayDebounce: true, // Debounce delay registration
	eventDelayValue: 10, // Delay task execution by 10 minutes
	eventDelayUnit: 'minutes'
});
```

### Triggering Webhooks

```typescript
// Trigger due webhooks (scheduled tasks)
const { processed, errors } = await hooks.trigger();

// Trigger webhooks matching an event pattern (event-driven tasks)
const { processed, errors } = await hooks.trigger({
	namespace: 'my-app',
	eventPattern: 'user-created',
	requestBody: {
		userId: 123
	}
});

// Trigger a specific task by ID
const { processed, errors } = await hooks.trigger({
	namespace: 'my-app',
	id: 'task-id'
});

// Trigger a fork task by ID and fork ID
const { processed, errors } = await hooks.trigger({
	namespace: 'my-app',
	id: 'task-id',
	forkId: 'fork-id-1',
	forkOnly: true // Only register the fork, do not execute it
});
```

### Querying Webhooks

```typescript
// Fetch webhooks by namespace
const { items, count, lastEvaluatedKey } = await hooks.fetchTasks({
	namespace: 'my-app',
	limit: 100
});

// Fetch webhooks with filters
const filteredTasks = await hooks.fetchTasks({
	namespace: 'my-app',
	status: 'ACTIVE',
	fromScheduledDate: new Date(Date.now() - 86400_000).toISOString(), // Last 24 hours
	toScheduledDate: new Date().toISOString(),
	desc: true // Sort by scheduledDate descending
});

// Fetch webhooks by event pattern
const eventTasks = await hooks.fetchTasks({
	namespace: 'my-app',
	eventPattern: 'user-',
	eventPatternPrefix: true
});

// Get a specific webhook
const task = await hooks.getTask({
	namespace: 'my-app',
	id: 'task-id'
});

// Get a specific fork webhook
const forkTask = await hooks.getTask({
	namespace: 'my-app',
	id: 'fork-id-1',
	fork: true
});

// Fetch execution logs
const logs = await hooks.fetchLogs({
	namespace: 'my-app',
	from: new Date(Date.now() - 86400_000).toISOString(), // Last 24 hours
	to: new Date().toISOString()
});
```

### Managing Webhooks

```typescript
// Delete a single webhook
const deletedTask = await hooks.deleteTask({
	namespace: 'my-app',
	id: 'task-id'
});

// Delete a fork and its associated subtasks
const deletedForkTask = await hooks.deleteTask({
	namespace: 'my-app',
	id: 'fork-id-1',
	fork: true
});

// Update a webhook
const updatedTask = await hooks.updateTask({
	id: 'task-id',
	namespace: 'my-app',
	requestBody: {
		key: 'updated-value'
	},
	repeatInterval: 60
});

// Update a fork and its associated subtasks
const updatedForkTask = await hooks.updateTask({
	id: 'fork-id-1',
	namespace: 'my-app',
	fork: true,
	requestHeaders: {
		'X-Custom-Header': 'newValue'
	}
});

// Disable a webhook
const disabledTask = await hooks.setTaskActive({
	namespace: 'my-app',
	id: 'task-id',
	active: false
});

// Enable a webhook
const enabledTask = await hooks.setTaskActive({
	namespace: 'my-app',
	id: 'task-id',
	active: true
});

// Clear all webhooks in a namespace
const { count } = await hooks.clearTasks('my-app');

// Clear all logs in a namespace
const { count } = await hooks.clearLogs('my-app');
```

### Debugging

```typescript
// Debug a task's condition filter against provided data
const debugResult = await hooks.debugCondition({
	namespace: 'my-app',
	id: 'task-id',
	conditionData: {
		key: 'value'
	}
});

console.log(debugResult);
```

## 📋 Task Schema

```typescript
type Task = {
	__createdAt: string;
	__updatedAt: string;
	__ts: number;
	concurrency: boolean;
	conditionFilter: null | {
		defaultValue: string;
		normalize: boolean;
		operator: string;
		path: string[];
		type: string;
		value: string;
	};
	description: string;
	eventDelayDebounce: boolean;
	eventDelayUnit: 'minutes' | 'hours' | 'days';
	eventDelayValue: number;
	eventPattern: string;
	firstErrorDate: string;
	firstExecutionDate: string;
	firstScheduledDate: string;
	forkId: string;
	id: string;
	lastError: string;
	lastErrorDate: string;
	lastErrorExecutionType: 'EVENT' | 'SCHEDULED';
	lastExecutionDate: string;
	lastExecutionType: 'EVENT' | 'SCHEDULED';
	lastResponseBody: string;
	lastResponseHeaders: Record<string, string> | null;
	lastResponseStatus: number;
	namespace: string;
	namespace__eventPattern: string;
	noAfter: string;
	noBefore: string;
	pid: string;
	primaryId: string;
	primaryNamespace: string;
	repeatInterval: number;
	repeatMax: number;
	repeatUnit: 'minutes' | 'hours' | 'days';
	requestBody: any;
	requestHeaders: Record<string, string> | null;
	requestMethod: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
	requestUrl: string;
	rescheduleOnEvent: boolean;
	retryLimit: number;
	scheduledDate: string;
	status: 'ACTIVE' | 'PROCESSING' | 'MAX-REPEAT-REACHED' | 'MAX-ERRORS-REACHED' | 'DISABLED';
	totalErrors: number;
	totalExecutions: number;
	totalFailedExecutions: number;
	totalSuccessfulExecutions: number;
	ttl: number;
	type: 'PRIMARY' | 'FORK' | 'SUBTASK';
};
```

## 🧪 Testing

```bash
# Set environment variables
export AWS_ACCESS_KEY='YOUR_ACCESS_KEY'
export AWS_SECRET_KEY='YOUR_SECRET_KEY'
export AWS_REGION='YOUR_REGION'

# Run tests
yarn test

# Run tests with coverage
yarn test:coverage
```

## 📝 Notes

- Failed webhooks will be retried according to the `retryLimit` configuration.
- Tasks are marked as `MAX-ERRORS-REACHED` after reaching `maxErrors` threshold (default: 5).
- Recurring webhooks are automatically rescheduled after successful execution.
- The scheduler uses DynamoDB's GSI capabilities for efficient webhook querying.
- All timestamps are in ISO 8601 format and stored in UTC.
- Webhook execution is concurrent with configurable limits.
- Batch operations support chunked processing for better performance.
- The `chunkLimit` parameter in batch operations allows you to control the size of each processing batch.
- Execution logs are stored in a separate table with configurable TTL.
- Forking creates new tasks based on existing ones, copying relevant properties and appending `#FORK` to the namespace.
- Subtasks are created for delayed tasks, with `type` set to `SUBTASK` and `namespace` appended with `#SUBTASK`.
- Debouncing for delayed tasks is achieved by updating the existing subtask instead of creating a new one.
- Concurrency control is implemented using the `pid` field and conditional updates.
- Debugging capabilities allow testing task conditions without triggering actual webhooks.
- UUID generation and hashing utilities are provided for task and fork IDs.

## 📄 License

MIT © [Felipe Rohde](mailto:feliperohdee@gmail.com)

## 👨‍💻 Author

**Felipe Rohde**

- Twitter: [@felipe_rohde](https://twitter.com/felipe_rohde)
- Github: [@feliperohdee](https://github.com/feliperohdee)
- Email: feliperohdee@gmail.com

## 🙏 Acknowledgements

- [use-dynamodb](https://github.com/feliperohdee/use-dynamodb)
- [AWS SDK for JavaScript v3](https://github.com/aws/aws-sdk-js-v3)
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
