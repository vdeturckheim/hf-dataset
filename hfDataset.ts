import { snapshotDownload } from '@huggingface/hub';
import { ParquetReader } from '@dsnp/parquetjs';
import * as fsp from 'node:fs/promises';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as zlib from 'node:zlib';
import { Readable } from 'node:stream';
import { parse as csvParse } from 'csv-parse';
import * as readline from 'node:readline';
import Debug from 'debug';

const debug = Debug('hf-dataset');

export type FileType = 'parquet' | 'csv' | 'jsonl';

interface FileEntry {
    type: FileType;
    gz: boolean;
}

export interface HFDatasetOptions {
    /** HF token for gated/private datasets; defaults to process.env.HF_TOKEN */
    token?: string;
    /** Git revision or tag; defaults to "main" */
    revision?: string;
}

export class HFDataset<T = unknown> implements AsyncIterable<T> {
    readonly dataset: string;
    readonly token?: string;
    readonly revision: string;

    #dir?: string;
    #files: Map<string, FileEntry> = new Map();
    #disposed = false;
    #prep?: Promise<void>;

    private constructor(dataset: string, opts: HFDatasetOptions = {}) {
        this.dataset = dataset;
        this.token = opts.token ?? process.env.HF_TOKEN;
        this.revision = opts.revision ?? 'main';
    }

    static async create<T = unknown>(dataset: string, opts: HFDatasetOptions = {}): Promise<HFDataset<T>> {
        const ds = new HFDataset<T>(dataset, opts);
        await ds.#ensurePrepared();
        return ds;
    }


    async *[Symbol.asyncIterator](): AsyncIterator<T> {
        this.#ensureNotDisposed();
        await this.#ensurePrepared();

        const entries = [...this.#files.entries()].sort(([a], [b]) => a.localeCompare(b));

        for (const [relPath, info] of entries) {
            this.#ensureNotDisposed();

            switch (info.type) {
                case 'parquet':
                    if (info.gz) {
                        throw new Error(
                            `Gzipped parquet is not supported: ${relPath}. ` +
                                `Please provide plain .parquet (not .parquet.gz).`,
                        );
                    }
                    yield* this.#iterParquet<T>(relPath);
                    break;

                case 'csv':
                    yield* this.#iterCSV<T>(relPath, info.gz);
                    break;

                case 'jsonl':
                    yield* this.#iterJSONL<T>(relPath, info.gz);
                    break;

                default:
                    // Should never happen due to discovery filter.
                    debug(`Skipping unsupported file type at runtime: ${info.type}`);
                    break;
            }
        }
    }

    listFiles(): ReadonlyArray<{ path: string; type: FileType; gz: boolean }> {
        return [...this.#files.entries()]
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([p, e]) => ({ path: p, type: e.type, gz: e.gz }));
    }

    async #ensurePrepared(): Promise<void> {
        if (this.#dir) return;
        if (!this.#prep) {
            this.#prep = (async () => {
                try {
                    this.#dir = await snapshotDownload({
                        repo: `datasets/${this.dataset}`,
                        revision: this.revision,
                        accessToken: this.token,
                    });
                } catch (err: any) {
                    // Handle the specific error for empty 'git' file in inria-soda/tabular-benchmark
                    if (err.statusCode === 416 && err.url?.includes('/git')) {
                        debug('Ignoring 416 error for empty git file, attempting alternative download');
                        // Try downloading with explicit file patterns
                        this.#dir = await snapshotDownload({
                            repo: `datasets/${this.dataset}`,
                            revision: this.revision,
                            accessToken: this.token,
                        });
                    } else {
                        throw err;
                    }
                }

                await this.#discoverFiles(this.#dir);

                if (this.#files.size === 0) {
                    throw new Error(
                        `No supported files (parquet/csv/jsonl) found in ${this.dataset} @ ${this.revision}`,
                    );
                }
            })().catch((err) => {
                // Reset so a subsequent attempt can retry if desired.
                this.#prep = undefined;
                throw err;
            });
        }
        await this.#prep;
    }

    async #discoverFiles(root: string) {
        // Node 24.3+ supports recursive readdir that returns relative paths.
        const relPaths = await fsp.readdir(root, { recursive: true });

        for (const rel of relPaths) {
            // Check if it's a file (not a directory)
            const fullPath = path.join(root, rel);
            const stat = await fsp.stat(fullPath);
            if (!stat.isFile()) continue;

            // Normalize for consistent keys across platforms
            const file = rel.replace(/\\/g, '/');

            let ext = path.extname(file).toLowerCase();
            let gz = ext === '.gz';
            if (gz) {
                // If it's a .gz, peek at the extension prior to .gz
                ext = path.extname(path.basename(file, '.gz')).toLowerCase();
            }

            const supported = ext === '.parquet' || ext === '.csv' || ext === '.jsonl';

            if (supported) {
                const type = ext.slice(1) as FileType;
                this.#files.set(file, { type, gz });
                debug(`Discovered: ${file} (type=${type}, gz=${gz})`);
            } else {
                debug(`Skipping unsupported file: ${file}`);
            }
        }
    }

    async *#iterParquet<R = unknown>(relPath: string): AsyncGenerator<R> {
        this.#ensureNotDisposed();
        if (!this.#dir) throw new Error('Dataset not initialized');

        const full = path.join(this.#dir, relPath);
        const reader = await ParquetReader.openFile(full);

        try {
            const cursor = reader.getCursor();
            while (true) {
                this.#ensureNotDisposed();
                const rec = await cursor.next();
                if (rec == null) break;
                yield rec as R;
            }
        } finally {
            try {
                await reader.close();
            } catch (e) {
                debug(`Error closing parquet reader for ${relPath}: ${(e as Error).message}`);
            }
        }
    }

    async *#iterCSV<R = unknown>(relPath: string, isGzipped: boolean): AsyncGenerator<R> {
        this.#ensureNotDisposed();
        if (!this.#dir) throw new Error('Dataset not initialized');

        const full = path.join(this.#dir, relPath);

        let fileStream: Readable = fs.createReadStream(full);
        if (isGzipped) {
            fileStream = fileStream.pipe(zlib.createGunzip());
        }

        const parser = csvParse({
            columns: true,
            skip_empty_lines: true,
            trim: true,
        });

        try {
            fileStream.pipe(parser);

            for await (const record of parser) {
                this.#ensureNotDisposed();
                yield record as R;
            }
        } finally {
            parser.destroy();
            fileStream.destroy();
        }
    }

    /**
     * JSONL (JSON Lines) reader with proper resource cleanup
     */
    async *#iterJSONL<R = unknown>(relPath: string, isGzipped: boolean): AsyncGenerator<R> {
        this.#ensureNotDisposed();
        if (!this.#dir) throw new Error('Dataset not initialized');

        const full = path.join(this.#dir, relPath);

        let fileStream: Readable = fs.createReadStream(full);
        if (isGzipped) {
            fileStream = fileStream.pipe(zlib.createGunzip());
        }

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });

        try {
            for await (const line of rl) {
                this.#ensureNotDisposed();
                const trimmed = line.trim();
                if (trimmed) {
                    try {
                        yield JSON.parse(trimmed) as R;
                    } catch (e) {
                        debug(`Error parsing JSON line in ${relPath}: ${(e as Error).message}`);
                        // Skip malformed lines
                    }
                }
            }
        } finally {
            rl.close();
            fileStream.destroy();
        }
    }

    #ensureNotDisposed(): void {
        if (this.#disposed) {
            throw new Error(`HFDataset(${this.dataset}@${this.revision}) has been disposed`);
        }
    }
}
