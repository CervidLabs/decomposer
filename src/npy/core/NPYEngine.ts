import fs from 'fs';
import os from 'os';
import { type Transferable, Worker } from 'worker_threads';
import { type NPYMetadata, NPYHeaderParser } from './NPYHeaderParser.js';
import { getTypedArray } from '../lib/dtypes.js';

/**
 * Interface para el esquema de columnas de Cervid
 */
interface ColumnSchema {
  name: string;
  dtype: string;
  offset: number;
  length: number;
}

/**
 * Formato de salida estandarizado para Cervid/Data
 */
export interface NPYProcessResult {
  version: number;
  source: {
    format: 'npy';
    path: string;
  };
  shape: number[];
  rowCount: number;
  columnCount: number;
  schema: ColumnSchema[];
  buffer: SharedArrayBuffer;
  metadata: {
    fortranOrder: boolean;
    originalShape: number[];
    ndim: number;
    elementSize: number;
  };
}

interface EngineConfig {
  workers?: number;
  chunkSizeMB?: number;
  smallFileMB?: number;
}

interface WorkerTask {
  buffer: Uint8Array;
  dtype: string;
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
}

export class NPYEngine {
  private readonly config: Required<EngineConfig>;
  private readonly pool: Worker[] = [];
  private readonly idle: Worker[] = [];
  private readonly queue: WorkerTask[] = [];
  private readonly activeTasks = new Map<Worker, WorkerTask>();

  constructor(config: EngineConfig = {}) {
    this.config = {
      workers: config.workers ?? os.cpus().length,
      chunkSizeMB: config.chunkSizeMB ?? 64,
      smallFileMB: config.smallFileMB ?? 16,
    };

    this._initWorkers();
  }

  public async process(filePath: string): Promise<NPYProcessResult> {
    const stats = fs.statSync(filePath);
    const sizeMB = stats.size / (1024 * 1024);

    if (sizeMB < this.config.smallFileMB) {
      return this._processDirect(filePath);
    }

    return this._processParallel(filePath);
  }

  public async destroy(): Promise<void> {
    await Promise.all(this.pool.map(async (w) => w.terminate()));
    this.pool.length = 0;
    this.idle.length = 0;
    this.activeTasks.clear();
  }

  private async _processDirect(filePath: string): Promise<NPYProcessResult> {
    const buffer = fs.readFileSync(filePath);
    const parser = new NPYHeaderParser(buffer);
    const { metadata, dataOffset } = parser.parse();

    const TA = getTypedArray(metadata.descr);
    const totalElements = metadata.shape.reduce((acc, dim) => acc * dim, 1);
    const totalBytes = buffer.length - dataOffset;

    const data = buffer.subarray(dataOffset);
    const sharedBuffer = new SharedArrayBuffer(totalBytes);
    new Uint8Array(sharedBuffer).set(data);

    return new Promise((resolve) => resolve(this._buildResult(filePath, metadata, sharedBuffer, totalElements, TA.BYTES_PER_ELEMENT)));
  }

  private async _processParallel(filePath: string): Promise<NPYProcessResult> {
    const fd = fs.openSync(filePath, 'r');

    try {
      const headerSize = 4096;
      const headerBuffer = Buffer.alloc(headerSize);
      const bytesRead = fs.readSync(fd, headerBuffer, 0, headerSize, 0);

      if (bytesRead === 0) {
        throw new Error(`Could not read header: ${filePath}`);
      }

      const parser = new NPYHeaderParser(headerBuffer);
      const { metadata, dataOffset } = parser.parse();

      const TA = getTypedArray(metadata.descr);
      const totalElements = metadata.shape.reduce((acc, dim) => acc * dim, 1);
      const fileSize = fs.statSync(filePath).size;
      const totalBytes = fileSize - dataOffset;

      const sharedBuffer = new SharedArrayBuffer(totalBytes);
      const sharedView = new Uint8Array(sharedBuffer);

      const chunkBytes = this.config.chunkSizeMB * 1024 * 1024;
      let filePosition = dataOffset;
      let sharedPosition = 0;

      while (filePosition < fileSize) {
        const remaining = fileSize - filePosition;
        const size = Math.min(chunkBytes, remaining);

        const chunk = Buffer.allocUnsafe(size);
        const bytesReadChunk = fs.readSync(fd, chunk, 0, size, filePosition);

        if (bytesReadChunk <= 0) {
          break;
        }

        sharedView.set(chunk.subarray(0, bytesReadChunk), sharedPosition);

        filePosition += bytesReadChunk;
        sharedPosition += bytesReadChunk;
      }

      return new Promise((resolve) => resolve(this._buildResult(filePath, metadata, sharedBuffer, totalElements, TA.BYTES_PER_ELEMENT)));
    } finally {
      fs.closeSync(fd);
    }
  }

  private _buildResult(path: string, meta: NPYMetadata, buffer: SharedArrayBuffer, totalElements: number, elementSize: number): NPYProcessResult {
    return {
      version: 1,
      source: { format: 'npy', path },
      shape: meta.shape,
      rowCount: totalElements,
      columnCount: 1,
      schema: [
        {
          name: 'values',
          dtype: meta.descr,
          offset: 0,
          length: totalElements,
        },
      ],
      buffer,
      metadata: {
        fortranOrder: false, // El parser actual debería extraer esto si es necesario
        originalShape: meta.shape,
        ndim: meta.shape.length,
        elementSize,
      },
    };
  }

  private _initWorkers(): void {
    const workerUrl = new URL('../workers/npy-worker.js', import.meta.url);

    for (let i = 0; i < this.config.workers; i++) {
      const worker = new Worker(workerUrl);

      worker.on('message', (msg: unknown) => {
        const task = this.activeTasks.get(worker);
        if (task) {
          this.activeTasks.delete(worker);
          this.idle.push(worker);
          task.resolve(msg);
          this._next();
        }
      });

      worker.on('error', (err: Error) => {
        const task = this.activeTasks.get(worker);
        if (task) {
          this.activeTasks.delete(worker);
          this.idle.push(worker);
          task.reject(err);
          this._next();
        }
      });

      this.pool.push(worker);
      this.idle.push(worker);
    }
  }

  private _next(): void {
    if (this.queue.length === 0 || this.idle.length === 0) {
      return;
    }

    const worker = this.idle.pop();
    const task = this.queue.shift();

    if (worker && task) {
      this.activeTasks.set(worker, task);

      // El buffer subyacente puede ser ArrayBuffer o SharedArrayBuffer
      const rawBuffer = task.buffer.buffer;

      // Casting seguro: convertimos a unknown y luego a Transferable
      // para satisfacer al compilador de TS
      const transferList: Transferable[] = [rawBuffer as unknown as Transferable];

      worker.postMessage({ buffer: task.buffer, dtype: task.dtype }, transferList);
    }
  }
}
