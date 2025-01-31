# ⚡️ Use DynamoDB Reactive Hooks ⚡️

A powerful TypeScript library for orchestrating dynamic, event-driven, and scheduled webhook workflows, leveraging the speed and scalability of DynamoDB. Go beyond basic cron-like scheduling with advanced features like delayed execution, event-based triggering, forking, debouncing, concurrency control, and robust error handling. Build reactive, resilient, and scalable applications with ease.

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
- 🔍 Filtering capabilities with configurable filter criteria.
- 🏃 Concurrent webhook execution with configurable limits.
- 📦 Batch operations support for efficient updates and deletions.
- 🔄 Configurable webhook chunk processing for optimal performance and resource management.
- 📊 Detailed execution logs and metrics, including response body, headers, and status.
- 🔀 Forking mechanism to create new tasks based on existing ones, with customizable forking behavior.
- ⏱️ Configurable delay mechanism to postpone task execution, with optional debouncing for event-driven delays.
- 🔒 Concurrency control to prevent simultaneous execution of the same task.
- 🧪 Debugging capabilities to test task conditions against provided data.
- 🆔 UUID generation and hashing utilities for task and fork IDs.
- 🎯 Rules system for dynamic webhook transformation and multiplexing based on custom logic.

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
	filterCriteria: new FilterCriteria(), // Optional: custom filter criteria implementation
	logsTableName: 'YOUR_LOGS_TABLE_NAME',
	logsTtlInSeconds: 86400, // Optional: TTL for log entries (default: forever)
	maxConcurrency: 25, // Optional: concurrent webhook execution limit (default: 25)
	maxErrors: 5, // Optional: max errors before marking task as MAX-ERRORS-REACHED (default: 5)
	region: 'us-east-1',
	secretAccessKey: 'YOUR_SECRET_KEY',
	tasksTableName: 'YOUR_TABLE_NAME',
	webhookCaller: async (input: Hooks.CallWebhookInput) => {
		// Optional: custom webhook caller implementation
	},
	webhookChunkSize: 10 // Optional: process webhooks in chunks of specified size
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

### Rules System

```typescript
// Register a rule that transforms webhook requests
hooks.registerRule('rule-id', async ({ task }) => {
	// Return an array of transformed webhook requests
	return [
		{
			requestBody: { key: 'value1' },
			requestHeaders: { 'X-Custom': 'header1' },
			requestMethod: 'POST',
			requestUrl: 'https://api.example.com/endpoint1'
		},
		{
			requestBody: { key: 'value2' },
			requestHeaders: { 'X-Custom': 'header2' },
			requestMethod: 'POST',
			requestUrl: 'https://api.example.com/endpoint2'
		}
	];
});

// Use the rule when registering a task
const task = await hooks.registerTask({
	namespace: 'my-app',
	requestUrl: 'https://api.example.com/endpoint',
	ruleId: 'rule-id' // Reference the rule
});

// Or trigger a specific rule
const { processed, errors } = await hooks.trigger({
	namespace: 'my-app',
	id: 'task-id',
	ruleId: 'rule-id'
});
```

The rules system allows you to:

- Transform webhook requests dynamically based on custom logic
- Split a single webhook into multiple requests with different configurations
- Apply complex transformations to request bodies, headers, methods and URLs
- Chain multiple rules together for advanced webhook orchestration
- Override rule behavior at trigger time with custom configurations

### Webhook Chunk Processing

```typescript
// Initialize hooks with chunk processing
const hooks = new Hooks({
	// ... other options
	webhookChunkSize: 10, // Process webhooks in batches of 10
	maxConcurrency: 25 // Control concurrent execution of chunks
});

// The webhookChunkSize parameter provides several benefits:
// - Better memory management for large numbers of webhooks
// - Improved error isolation between chunks
// - Fine-grained control over processing rate
// - Reduced network load through controlled batch sizes
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
- Webhook execution can be processed in chunks using the `webhookChunkSize` parameter for better control over batch processing.
- When `webhookChunkSize` is set, webhooks are processed in batches of the specified size while respecting `maxConcurrency`.
- Setting `webhookChunkSize` to 0 (default) processes all webhooks in a single batch.

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

## 📊 Flowchart

<img style="background:#fff;padding:10px" src="https://mermaid.ink/img/pako:eNqVGOly2kb4VXaU8TiZkTOAOQzTSYcAbpiJHReI0xb4sUgLaBArIq1iE-CF-hp9se6hvSRkt_ywd7_73l0dHC_ykdNxLi4OIMAB6YADmGFAf5dkjbbosgMuFzBBl64JfYRxABchSij6IBAcuYuDLYz3vSiMYsb5Zsl_ktmkmaBnoukq_HeO7mMU-yh-kTIMMHqRIEFehP3XDCMoJsFrRAvobVZxlGL_RbItDPDHzaoMvYwwuYXbINwzChbM8FKgTzN8AqeLixme4WUYPXlrGBMw6QvsxQUYYBLvwUMUYJII4JhQkrdT_m_-DlxdfQCTOFitUDzZ71ByyDaA7U6CxcQzhuMdxCkMjxLxLSDrId6lZCqZnygEcNC8RMTYWyM_DZGvpCjIVK0kas78yzz6PUXUo65Hgh8ITGCySSwNyhbuGSf-uH-AhKYLTwXvYg8GPxAmGXT-Ov_Q16zDvs2gjTUYJFCzKbI-JMj0h7kAHuLIQ0kS4BUYI5LuBNa2nsv_hhbrKNr01immf5C3OTwZkHHwE4EPoPLryRIw9M_zWkTSwDJSZTAHGhYLRIGDZ_lPlBwFw5CgbTIVvHzNgpK3PYusZuDGZKo4NJlmO0GU_LKIP_Bi28LnXoS9NI4R9vbzl4y6j45SZjcMhWFSKgVk5kECvmDPSlUPhmEmEtzSbgNXoBdtAZ0WJI5o5HzE1l4Ux__8jb0ACkbLfO6QIWdqrJkvZ93IGZuXYVpIB1dAgggDI8Gm2ZxVEokC-gQTZrYA3QYhrTVZPzalzugjDAOf1rHCT-8g8daafl7Kz4LPSp5NAsPGgsRszlCp_4P-PgIZy3gT7BjblC24gDn3fYD9fO8NnpGX5mNmqTxwOnMgmliu-UEcPzo0XLnACwViWM3LBNxG8eYI2F9lz1QyMujL3ON0wYBH8BuiY43WDuH66Q6IrWS3Xc-MpbrsqHIkLxWmge3ocUHQQSjkWA5gFXsluDggTQSEDdEAAzbqMshXTI9lEKMdguQOPgvgfUTAKMWYDhFZcJY-7hqXfgRGVwwwu0z4pRxD_EPwTNZx9MRwgziO4infZjlngHMVYUSrMOKs2HJeGS8BPmTMHCpbyCYxhyK1O0uwUSs9iGW9zEsF8B5izph-5V1SwTGU_OeA5nlKQmpPHikOfILYD1XYiooOBsgYNTkqHat-ECOPFPuClVlET-rPkceHJw0RHXUo1C09f1E4iyM9bpk7TMR0zNpl2Ffyxuh7SmNOr1sFiQabbhS2G6ceK5oDx3S97ym1XRVDnkq7aFbCmXlxlpGbT8ebDgwfdtpUuxhyURRIrdbIJZ83-eq3RpP2mYHv6KPgoHFsazosSbjRg8fB_eQIRmgVJPSkYbip3BQGXZF93Ps06H_9POgL1zk795qtcg6bOjiGLb7gcC-GOgezrcyOhTZSg32uhv63IpPTVuRm-TnXZSrOfRTCfLfkOk_Q6IOab6XBGmcXkj64dFKM0yvHZlyH5KVE3oay_VybZh87uWRmp5DOJ9dEmy1D5CJWlJfLLmd__RQfsVtrvlxtjzgXo9ORZDsZSIUpNCTDqIZkm_k5DhbCjEiGUM0oFUJlr7RJJcfK_Ch3By-Ovbs0JMEuVLJLL8HCa5RQ-rNFZgZnTJEhMnQqdQKjHDkrVOeCIwrZsOkP2RRT4bewOgdfd-zgy4inYpfdPgRsXsrPMiIYxBlpMouT0px3AaYjnl8gssekpVnMDkbDSUQJsYN6xK8zanzYFNoN-bK6R89EvW0B25XO6nOystOKvjho7gnip9Vd94-r0eBh0J3Qf13WPTlBRgwEgq2EB_QaJrYqDxppGI-IiKBUNxiNvozGJepyEjKTxWOdS-j2JsPHQaGL6fItG7Dzd0ZaxmSvS9YLYZL00RLsstm0DMKw8wZdL2tL303oA2yDOm-qjWbDq2Tbq6fAJ-tObffseuzrS-dNxa-3YDUn0EdekLARmUm8WTZQW0msoZZ_XSuVWF00UK2Sk5jwq6gQt7xGjWVDiWvC6qINS8XVYbV-4-XEIZ69TNwSLRBS4rxm7aZ2Uypu0ap61bw4T04IomRSIytKJmo2qpXyGAq0TBOXar4wXeu56-aHmJu_hWhrTIGFh7trvXnc4rHq6lPNVcPZtSeDm2srV5eraz9VXftZ4dqXcNc-uWQBmQ5YA8Q1utA1WtiVzeWqJhG1Y0rSF33XvnyLujD6pev7AGF_Z3zsEyL4tz6X3V_oobylMSBR7LjOlm5g4Dsdh3-UnTn8Y-3M6dClD-PNzJnhE6WDKYnGe-w5HRKnyHXiKF2tnc4Shgndpdy3fgBXMdxKkh3Ef0WRuXU6B-fZ6bRq72vtertWr9eazfp103X2TueqWn9faV1Xaq2bZvumXW23Tq7zk_NXKXmtVa_W2jfNVqNx3XIdRPMUxXfiQzT_Hn36F5x8sBA?type=png"/>
