import assert from 'node:assert';
import { describe, it } from 'node:test';
import { HFDataset } from './hfDataset.js';

describe('HFDataset', () => {
    describe('Different file formats', () => {
        it('should read Parquet files with proper data structure', async () => {
            const dataset = await HFDataset.create('Salesforce/wikitext');

            let count = 0;
            const rows: unknown[] = [];
            for await (const row of dataset) {
                assert.ok(row, 'Row should not be null or undefined');
                assert.strictEqual(typeof row, 'object', 'Row should be an object');
                assert.ok(row !== null, 'Row should not be null');

                // Check for expected structure in wikitext dataset
                const typedRow = row as { text?: string };
                assert.ok('text' in typedRow, 'Row should have a text field');
                assert.strictEqual(typeof typedRow.text, 'string', 'Text field should be a string');

                rows.push(row);
                count++;
                if (count >= 5) break; // Test first 5 rows
            }

            assert.strictEqual(count, 5, 'Should have read exactly 5 rows');
            assert.strictEqual(rows.length, 5, 'Should have collected 5 rows');

            // Verify that rows are different
            const firstRowText = (rows[0] as Record<string, unknown>).text;
            const secondRowText = (rows[1] as Record<string, unknown>).text;
            assert.notStrictEqual(firstRowText, secondRowText, 'First two rows should have different content');
        });

        it('should read CSV files with proper data structure', async () => {
            // Using a dataset that has CSV files
            const dataset = await HFDataset.create('lvwerra/red-wine', {
                revision: 'main',
            });

            const files = dataset.listFiles();
            const csvFiles = files.filter((f) => f.type === 'csv');

            if (csvFiles.length > 0) {
                let count = 0;
                const rows: unknown[] = [];

                for await (const row of dataset) {
                    assert.ok(row, 'Row should not be null or undefined');
                    assert.strictEqual(typeof row, 'object', 'Row should be an object');
                    assert.ok(row !== null, 'Row should not be null');

                    // CSV rows should have properties (column names)
                    const keys = Object.keys(row as object);
                    assert.ok(keys.length > 0, 'CSV row should have at least one column');

                    // Verify all values are defined
                    for (const key of keys) {
                        assert.ok(key in (row as Record<string, unknown>), `Row should have property ${key}`);
                    }

                    rows.push(row);
                    count++;
                    if (count >= 5) break; // Test first 5 rows
                }

                assert.ok(count > 0, 'Should have read at least one CSV row');
                assert.strictEqual(rows.length, count, 'Should have collected all read rows');

                // Verify consistent structure across rows
                if (rows.length > 1) {
                    const firstKeys = Object.keys(rows[0] as object).sort();
                    const secondKeys = Object.keys(rows[1] as object).sort();
                    assert.deepStrictEqual(firstKeys, secondKeys, 'All CSV rows should have the same columns');
                }
            }
        });

        it('should read JSONL files with proper data structure', async () => {
            // Using a dataset with JSONL files
            const dataset = await HFDataset.create('BeIR/scifact');

            const files = dataset.listFiles();
            const jsonlFiles = files.filter((f) => f.type === 'jsonl');

            if (jsonlFiles.length > 0) {
                let count = 0;
                const rows: unknown[] = [];

                for await (const row of dataset) {
                    assert.ok(row, 'Row should not be null or undefined');
                    assert.strictEqual(typeof row, 'object', 'Row should be an object');
                    assert.ok(row !== null, 'Row should not be null');

                    // JSONL files typically have JSON objects on each line
                    const keys = Object.keys(row as object);
                    assert.ok(keys.length > 0, 'JSONL row should have at least one property');

                    // Check common fields in BeIR/scifact
                    const typedRow = row as { _id?: string; title?: string; text?: string };
                    if ('_id' in typedRow) {
                        assert.strictEqual(typeof typedRow._id, 'string', '_id should be a string');
                    }
                    if ('title' in typedRow) {
                        assert.strictEqual(typeof typedRow.title, 'string', 'title should be a string');
                    }
                    if ('text' in typedRow) {
                        assert.strictEqual(typeof typedRow.text, 'string', 'text should be a string');
                    }

                    rows.push(row);
                    count++;
                    if (count >= 5) break; // Test first 5 rows
                }

                assert.ok(count > 0, 'Should have read at least one JSONL row');
                assert.strictEqual(rows.length, count, 'Should have collected all read rows');

                // Verify that rows can have different structures in JSONL (unlike CSV)
                // but each row should be a valid JSON object
                for (const row of rows) {
                    const jsonStr = JSON.stringify(row);
                    const reparsed = JSON.parse(jsonStr);
                    assert.deepStrictEqual(row, reparsed, 'Each row should be valid JSON');
                }
            }
        });

        it('should handle gzipped files correctly', async () => {
            // Using a dataset that might have gzipped files
            const dataset = await HFDataset.create('BeIR/scifact');

            const files = dataset.listFiles();
            const gzippedFiles = files.filter((f) => f.gz);

            if (gzippedFiles.length > 0) {
                console.log(`Found ${gzippedFiles.length} gzipped files`);

                // For gzipped JSONL/CSV files, they should still be readable
                let count = 0;
                for await (const row of dataset) {
                    assert.ok(row, 'Row from gzipped file should not be null');
                    assert.strictEqual(typeof row, 'object', 'Row from gzipped file should be an object');
                    count++;
                    if (count >= 3) break;
                }

                if (count > 0) {
                    assert.ok(count > 0, 'Should have successfully read from gzipped files');
                }
            }
        });

        it('should list files correctly with type information', async () => {
            const dataset = await HFDataset.create('Salesforce/wikitext');

            const files = dataset.listFiles();
            assert.ok(Array.isArray(files), 'listFiles should return an array');
            assert.ok(files.length > 0, 'Should have at least one file');

            for (const file of files) {
                assert.ok(file.path, 'File should have a path');
                assert.strictEqual(typeof file.path, 'string', 'File path should be a string');
                assert.ok(file.type, 'File should have a type');
                assert.ok(['parquet', 'csv', 'jsonl'].includes(file.type), 'File type should be valid');
                assert.strictEqual(typeof file.gz, 'boolean', 'File gz flag should be a boolean');
            }

            // Files should be sorted lexicographically
            const paths = files.map((f) => f.path);
            const sortedPaths = [...paths].sort((a, b) => a.localeCompare(b));
            assert.deepStrictEqual(paths, sortedPaths, 'Files should be sorted lexicographically');
        });

        it('should handle early iteration termination correctly', async () => {
            const dataset = await HFDataset.create('Salesforce/wikitext');

            let count = 0;
            for await (const row of dataset) {
                assert.ok(row, 'Row should exist');
                count++;
                if (count >= 2) break; // Early termination
            }

            assert.strictEqual(count, 2, 'Should stop at exactly 2 rows');

            // Should be able to iterate again
            let secondCount = 0;
            for await (const row of dataset) {
                assert.ok(row, 'Row should exist in second iteration');
                secondCount++;
                if (secondCount >= 3) break;
            }

            assert.strictEqual(secondCount, 3, 'Should be able to iterate again after early termination');
        });
    });
});
