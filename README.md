# Use DynamoDB Scheduler

A TypeScript library that provides a webhook scheduling system using Amazon DynamoDB as the backend. Schedule HTTP webhooks to be triggered at specific times or intervals, with full management and monitoring capabilities.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🚀 Features

- ✅ Register webhooks to be triggered at specific times
- 🔁 Support for recurring webhooks with flexible intervals
- 📊 Webhook status tracking (PENDING, PROCESSING, COMPLETED, FAILED, SUSPENDED)
- 🔄 Automatic retries with configurable max attempts
- 🕒 Built-in timestamp management
- 📝 Comprehensive error tracking
- 🔎 Rich querying capabilities by namespace, status, and time ranges
- 🏃 Concurrent webhook execution with configurable limits
- 📦 Batch operations support

## 📦 Installation

```bash
yarn add use-dynamodb-scheduler
```

## 🛠️ Usage

### Initialization

```typescript
import Scheduler from 'use-dynamodb-scheduler';

const scheduler = new Scheduler({
	accessKeyId: 'YOUR_ACCESS_KEY',
	concurrency: 25, // Optional: default concurrent webhook execution limit
	createTable: true, // Optional: automatically create table
	idPrefix: '', // Optional: custom webhook ID prefix
	region: 'us-east-1',
	secretAccessKey: 'YOUR_SECRET_KEY',
	tableName: 'YOUR_TABLE_NAME'
});
```

### Registering Webhooks

```typescript
// Register a simple webhook
const webhook = await scheduler.register({
	namespace: 'my-app',
	method: 'POST',
	url: 'https://api.example.com/endpoint',
	schedule: new Date('2024-03-20T10:00:00Z').toISOString(),
	headers: {
		'Content-Type': 'application/json'
	},
	body: {
		key: 'value'
	}
});

// Register a recurring webhook
const recurringWebhook = await scheduler.register({
	namespace: 'my-app',
	method: 'GET',
	url: 'https://api.example.com/status',
	schedule: new Date('2024-03-20T10:00:00Z').toISOString(),
	repeat: {
		enabled: true,
		rule: {
			interval: 30,
			max: 5,
			unit: 'minutes'
		}
	}
});
```

### Triggering Webhooks

```typescript
// Trigger due webhooks
const { processed, errors } = await scheduler.trigger();

// Dry run to see what would be triggered
const dryrun = await scheduler.triggerDryrun({
	namespace: 'my-app',
	limit: 100
});
```

### Querying Webhooks

```typescript
// Fetch webhooks by namespace
const { items, count, lastEvaluatedKey } = await scheduler.fetch({
	namespace: 'my-app',
	limit: 100
});

// Fetch webhooks with filters
const filteredWebhooks = await scheduler.fetch({
	namespace: 'my-app',
	status: 'COMPLETED',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});

// Get specific webhook
const webhook = await scheduler.get({
	namespace: 'my-app',
	id: 'webhook-id'
});
```

### Managing Webhooks

```typescript
// Delete a single webhook
const deletedWebhook = await scheduler.delete({
	namespace: 'my-app',
	id: 'webhook-id'
});

// Delete multiple webhooks based on criteria
const { count, items } = await scheduler.deleteMany({
	namespace: 'my-app',
	status: 'COMPLETED',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});

// Clear all webhooks in a namespace
const { count } = await scheduler.clear('my-app');

// Suspend a single webhook (prevent it from being triggered)
const suspendedWebhook = await scheduler.suspend({
	namespace: 'my-app',
	id: 'webhook-id'
});

// Suspend multiple webhooks based on criteria
const { count, items } = await scheduler.suspendMany({
	namespace: 'my-app',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z',
	chunkLimit: 100 // Optional: process in chunks
});

// Unsuspend a webhook (allow it to be triggered again)
const unsuspendedWebhook = await scheduler.unsuspend({
	namespace: 'my-app',
	id: 'webhook-id'
});
```

## 📋 Webhook Schema

```typescript
type Webhook = {
	// Required fields
	namespace: string;
	url: string;
	schedule: string; // ISO date string
	method: 'GET' | 'POST' | 'PUT';

	// Optional fields
	headers?: Record<string, string>;
	body?: unknown;

	// Recurring webhook configuration
	repeat?: {
		enabled: boolean;
		rule: {
			interval: number;
			max: number;
			unit: 'minutes' | 'hours' | 'days';
		};
	};

	// System-managed fields
	id: string;
	status: 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'SUSPENDED';
	retries: {
		count: number;
		last: string | null;
		max: number; // default: 3
	};
	errors: string[];
	response?: {
		body: string;
		headers: Record<string, string>;
		status: number;
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

- Failed webhooks will be retried according to the `maxRetries` configuration
- Recurring webhooks are automatically rescheduled after successful execution
- The scheduler uses DynamoDB's GSI capabilities for efficient webhook querying
- All timestamps are in ISO 8601 format and stored in UTC
- Webhook execution is concurrent with configurable limits
- Batch operations (`deleteMany` and `suspendMany`) support chunked processing for better performance
- The `chunkLimit` parameter in batch operations allows you to control the size of each processing batch

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
