# hf-dataset

A Node.js library for streaming HuggingFace datasets with support for Parquet, CSV, and JSONL formats.

## Installation

```bash
npm install hf-dataset
```

## Quick Start

```javascript
import { HFDataset } from 'hf-dataset';

// Load a dataset and iterate through it
const dataset = await HFDataset.create('Salesforce/wikitext');

for await (const row of dataset) {
  console.log(row.text);
  break; // Just show the first row
}
```

## Features

- **Multiple Formats**: Supports Parquet, CSV, and JSONL files
- **Gzipped Files**: Automatically handles `.gz` compressed files  
- **Streaming**: Memory-efficient iteration over large datasets
- **TypeScript**: Full TypeScript support with generics
- **Authentication**: Support for private/gated datasets with HF tokens

## API Reference

### `HFDataset.create(dataset, options?)`

Creates a new dataset instance.

**Parameters:**
- `dataset` (string): HuggingFace dataset identifier (e.g., `'Salesforce/wikitext'`)
- `options` (object, optional):
  - `token` (string): HuggingFace token for private datasets (defaults to `process.env.HF_TOKEN`)
  - `revision` (string): Git revision or tag (defaults to `'main'`)

**Returns:** Promise<HFDataset<T>>

```javascript
// Public dataset
const dataset = await HFDataset.create('Salesforce/wikitext');

// Private dataset with token
const dataset = await HFDataset.create('my-org/private-dataset', {
  token: 'hf_xxxxxxxxxxxxx'
});

// Specific revision
const dataset = await HFDataset.create('Salesforce/wikitext', {
  revision: 'v1.0'
});
```

### Iteration

The dataset implements `AsyncIterable`, so you can use `for await` loops:

```javascript
const dataset = await HFDataset.create('Salesforce/wikitext');

// Process all rows
for await (const row of dataset) {
  console.log(row);
}

// Process first N rows
let count = 0;
for await (const row of dataset) {
  console.log(row);
  if (++count >= 100) break;
}
```

### `listFiles()`

Returns information about discovered files in the dataset.

```javascript
const dataset = await HFDataset.create('Salesforce/wikitext');
const files = dataset.listFiles();

console.log(files);
// [
//   { path: 'train.parquet', type: 'parquet', gz: false },
//   { path: 'test.csv.gz', type: 'csv', gz: true }
// ]
```

## Authentication

For private or gated datasets, provide your HuggingFace token:

### Environment Variable (Recommended)
```bash
export HF_TOKEN=hf_xxxxxxxxxxxxx
```

### Explicit Token
```javascript
const dataset = await HFDataset.create('my-org/private-dataset', {
  token: 'hf_xxxxxxxxxxxxx'
});
```

## Examples

### Working with Different File Formats

**Parquet Files:**
```javascript
const dataset = await HFDataset.create('Salesforce/wikitext');

for await (const row of dataset) {
  console.log(row.text); // Parquet preserves column types
}
```

**CSV Files:**
```javascript
const dataset = await HFDataset.create('lvwerra/red-wine');

for await (const row of dataset) {
  console.log(row); // CSV columns as string values
}
```

**JSONL Files:**
```javascript
const dataset = await HFDataset.create('BeIR/scifact');

for await (const row of dataset) {
  console.log(row._id, row.title); // JSON structure preserved
}
```

### TypeScript Usage

```typescript
interface WikiTextRow {
  text: string;
}

const dataset = await HFDataset.create<WikiTextRow>('Salesforce/wikitext');

for await (const row of dataset) {
  console.log(row.text); // TypeScript knows this is a string
}
```

### Processing Large Datasets

```javascript
const dataset = await HFDataset.create('large-dataset');

let processedCount = 0;
const batchSize = 1000;
const batch = [];

for await (const row of dataset) {
  batch.push(row);
  
  if (batch.length === batchSize) {
    await processBatch(batch);
    batch.length = 0; // Clear batch
    processedCount += batchSize;
    console.log(`Processed ${processedCount} rows`);
  }
}

// Process remaining rows
if (batch.length > 0) {
  await processBatch(batch);
}
```

## Requirements

- Node.js >= 24.3.0

## License

MIT - see [LICENSE](LICENSE) file for details.