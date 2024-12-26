# Use DynamoDB Scheduler

A TypeScript library that provides a scheduler implementation using Amazon DynamoDB as the backend, built on top of use-dynamodb.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🚀 Features

- ✅ Schedule HTTP tasks with customizable timing
- 🔁 Support for recurring tasks with flexible intervals
- 📊 Task status tracking (PENDING, PROCESSING, COMPLETED, FAILED, SUSPENDED)
- 🔄 Automatic retries with configurable max attempts
- 🕒 Built-in timestamp management
- 📝 Comprehensive error tracking
- 🔎 Rich querying capabilities by namespace, status, and time ranges
- 🏃 Concurrent task processing with configurable limits
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
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	tableName: 'YOUR_TABLE_NAME',
	createTable: true, // Optional: automatically create table
	concurrency: 25 // Optional: default concurrent task processing limit
});
```

### Scheduling Tasks

```typescript
// Schedule a simple task
const task = await scheduler.schedule({
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

// Schedule a recurring task
const recurringTask = await scheduler.schedule({
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

### Processing Tasks

```typescript
// Process due tasks
const { processed, errors } = await scheduler.process();

// Dry run to see what would be processed
const dryrun = await scheduler.processDryrun({
	namespace: 'my-app',
	limit: 100
});
```

### Querying Tasks

```typescript
// Fetch tasks by namespace
const { items, count, lastEvaluatedKey } = await scheduler.fetch({
	namespace: 'my-app',
	limit: 100
});

// Fetch tasks with filters
const filteredTasks = await scheduler.fetch({
	namespace: 'my-app',
	status: 'COMPLETED',
	from: '2024-03-01T00:00:00Z',
	to: '2024-03-31T23:59:59Z'
});

// Get specific task
const task = await scheduler.get({
	namespace: 'my-app',
	id: 'task-id'
});
```

### Managing Tasks

```typescript
// Delete a task
const deletedTask = await scheduler.delete({
	namespace: 'my-app',
	id: 'task-id'
});

// Clear all tasks in a namespace
const { count } = await scheduler.clear('my-app');

// Suspend a task (prevent it from being processed)
const suspendedTask = await scheduler.suspend({
	namespace: 'my-app',
	id: 'task-id'
});

// Unsuspend a task (allow it to be processed again)
const unsuspendedTask = await scheduler.unsuspend({
	namespace: 'my-app',
	id: 'task-id'
});
```

## 📋 Task Schema

```typescript
type Task = {
	// Required fields
	namespace: string;
	url: string;
	schedule: string; // ISO date string
	method: 'GET' | 'POST' | 'PUT';

	// Optional fields
	headers?: Record<string, string>;
	body?: unknown;
	maxRetries?: number; // default: 3

	// Recurring task configuration
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
	retries: number;
	errors: string[];
	response?: {
		body: string;
		headers: Record<string, string>;
		status: number;
	};
	__createdAt: string;
	__updatedAt: string;
	__ts: number;
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

- The scheduler automatically handles optimistic locking using the `__ts` field
- Failed tasks will be retried according to the `maxRetries` configuration
- Recurring tasks are automatically rescheduled after successful completion
- The scheduler uses DynamoDB's GSI capabilities for efficient task querying
- All timestamps are in ISO 8601 format and stored in UTC
- Task processing is concurrent with configurable limits

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
