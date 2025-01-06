# Use DynamoDB Reactive Hooks

A TypeScript library that provides a webhook scheduling system using Amazon DynamoDB as the backend. Schedule HTTP webhooks to be triggered at specific times or intervals, with full management and monitoring capabilities.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🚀 Features

- ✅ Register webhooks to be triggered at specific times
- 🔁 Support for recurring webhooks with flexible intervals
- 📊 Webhook status tracking (ACTIVE, PROCESSING, DONE, FAILED, SUSPENDED)
- 🔄 Automatic retries with configurable max attempts
- 🕒 Built-in timestamp management
- 📝 Comprehensive error tracking
- 🔎 Rich querying capabilities by namespace, status, and time ranges
- 🏃 Concurrent webhook execution with configurable limits
- 📦 Batch operations support
- 📊 Detailed execution logs and metrics

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
	concurrency: 25, // Optional: default concurrent webhook execution limit
	createTable: true, // Optional: automatically create table
	logsTableName: 'YOUR_LOGS_TABLE_NAME',
	logsTtlInSeconds: 86400, // Optional: TTL for log entries
	maxErrors: 5, // Optional: max errors before marking task as FAILED
	region: 'us-east-1',
	secretAccessKey: 'YOUR_SECRET_KEY',
	tasksTableName: 'YOUR_TABLE_NAME'
});
```

### Registering Webhooks

```typescript
// Register a simple webhook
const webhook = await hooks.register({
	namespace: 'my-app',
	method: 'POST',
	url: 'https://api.example.com/endpoint',
	scheduledDate: new Date('2024-03-20T10:00:00Z').toISOString(),
	request: {
		headers: {
			'Content-Type': 'application/json'
		},
		body: {
			key: 'value'
		}
	}
});

// Register a recurring webhook
const recurringWebhook = await hooks.register({
	namespace: 'my-app',
	method: 'GET',
	url: 'https://api.example.com/status',
	scheduledDate: new Date('2024-03-20T10:00:00Z').toISOString(),
	repeat: {
		interval: 30,
		max: 5,
		unit: 'minutes'
	}
});
```

### Triggering Webhooks

```typescript
// Trigger due webhooks
const { processed, errors } = await hooks.trigger();

// Dry run to see what would be triggered
const dryrun = await hooks.triggerDryrun({
	namespace: 'my-app',
	limit: 100,
	date: new Date().toISOString() // Optional: specific date
});
```

### Querying Webhooks

```typescript
// Fetch webhooks by namespace
const { items, count, lastEvaluatedKey } = await hooks.fetch({
	namespace: 'my-app',
	limit: 100
});

// Fetch webhooks with filters
const filteredWebhooks = await hooks.fetch({
	namespace: 'my-app',
	status: 'ACTIVE',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});

// Get specific webhook
const webhook = await hooks.get({
	namespace: 'my-app',
	id: 'webhook-id'
});

// Fetch execution logs
const logs = await hooks.fetchLogs({
	namespace: 'my-app',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});
```

### Managing Webhooks

```typescript
// Delete a single webhook
const deletedWebhook = await hooks.delete({
	namespace: 'my-app',
	id: 'webhook-id'
});

// Delete multiple webhooks based on criteria
const { count, items } = await hooks.deleteMany({
	namespace: 'my-app',
	status: 'DONE',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});

// Clear all webhooks in a namespace
const { count } = await hooks.clear('my-app');

// Suspend a single webhook (prevent it from being triggered)
const suspendedWebhook = await hooks.suspend({
	namespace: 'my-app',
	id: 'webhook-id'
});

// Suspend multiple webhooks based on criteria
const { count, items } = await hooks.suspendMany({
	namespace: 'my-app',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z',
	chunkLimit: 100 // Optional: process in chunks
});

// Unsuspend a webhook (allow it to be triggered again)
const unsuspendedWebhook = await hooks.unsuspend({
	namespace: 'my-app',
	id: 'webhook-id'
});
```

## 📋 Task Schema

```typescript
type Task = {
	// Required fields
	namespace: string;
	url: string;
	scheduledDate: string; // ISO date string
	method: 'GET' | 'POST' | 'PUT';

	// Optional fields
	request: {
		headers?: Record<string, string>;
		body?: unknown;
		method: 'GET' | 'POST' | 'PUT';
		url: string;
	};

	// Recurring task configuration
	repeat?: {
		interval: number;
		max: number; // default: 1, 0 for infinite
		unit: 'minutes' | 'hours' | 'days';
	};

	// System-managed fields
	id: string;
	idPrefix?: string;
	status: 'ACTIVE' | 'PROCESSING' | 'DONE' | 'FAILED' | 'SUSPENDED';
	retryLimit: number; // default: 3

	errors: {
		count: number;
		firstErrorDate: string | null;
		lastErrorDate: string | null;
		lastError: string | null;
	};

	execution: {
		count: number;
		failed: number;
		successful: number;
		firstExecutionDate: string | null;
		firstScheduledDate: string | null;
		lastExecutionDate: string | null;
		lastResponseBody: string;
		lastResponseHeaders: Record<string, string>;
		lastResponseStatus: number;
	};

	__createdAt: string;
	__updatedAt: string;
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

- Failed webhooks will be retried according to the `retryLimit` configuration
- Tasks are marked as FAILED after reaching `maxErrors` threshold (default: 5)
- Recurring webhooks are automatically rescheduled after successful execution
- The scheduler uses DynamoDB's GSI capabilities for efficient webhook querying
- All timestamps are in ISO 8601 format and stored in UTC
- Webhook execution is concurrent with configurable limits
- Batch operations (`deleteMany` and `suspendMany`) support chunked processing for better performance
- The `chunkLimit` parameter in batch operations allows you to control the size of each processing batch
- Execution logs are stored in a separate table with configurable TTL

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
