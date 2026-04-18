import fs from "fs";
import os from "os";
import { Worker } from "worker_threads";
import { NPYHeaderParser } from "./NPYHeaderParser.js";
import { getTypedArray } from "../lib/dtypes.js";

export class CervidEngine {
  constructor(config = {}) {
    this.config = {
      workers: config.workers || os.cpus().length,
      chunkSizeMB: config.chunkSizeMB || 64,
      smallFileMB: config.smallFileMB || 16
    };

    this.pool = [];
    this.idle = [];
    this.queue = [];

    this._initWorkers();
  }

  // ─────────────────────────────────────────────
  // 🧠 PUBLIC API
  // ─────────────────────────────────────────────
  async process(filePath) {
    const stats = fs.statSync(filePath);
    const sizeMB = stats.size / (1024 * 1024);

    if (sizeMB < this.config.smallFileMB) {
      return this._processDirect(filePath);
    }

    return this._processParallel(filePath);
  }

  async destroy() {
    for (const w of this.pool) {
      await w.terminate();
    }
  }

  // ─────────────────────────────────────────────
  // ⚡ DIRECT PATH (archivos pequeños)
  // ─────────────────────────────────────────────
  async _processDirect(filePath) {
    const buffer = fs.readFileSync(filePath);

    const parser = new NPYHeaderParser(buffer, true);
    const { metadata, dataOffset } = parser.parse();

    const TA = getTypedArray(metadata.descr);
    const totalElements = metadata.shape.reduce((acc, dim) => acc * dim, 1);    const totalBytes = buffer.length - dataOffset;

    // Solo datos, sin header
    const data = buffer.subarray(dataOffset);

    // Shared final para Cervid/Data
    const sharedBuffer = new SharedArrayBuffer(totalBytes);
    new Uint8Array(sharedBuffer).set(data);

    return {
      version: 1,
      source: {
        format: "npy",
        path: filePath
      },
      shape: metadata.shape,
      rowCount: totalElements,
      columnCount: 1,
      schema: [
        {
          name: "values",
          dtype: metadata.descr,
          offset: 0,
          length: totalElements
        }
      ],
      buffer: sharedBuffer,
      metadata: {
        fortranOrder: metadata.fortran_order,
        originalShape: metadata.shape,
        ndim: metadata.shape.length,
        elementSize: TA.BYTES_PER_ELEMENT
      }
    };
  }

  // ─────────────────────────────────────────────
  // 🔥 PARALLEL PATH (archivos grandes)
  // ─────────────────────────────────────────────
  async _processParallel(filePath) {
    const fd = fs.openSync(filePath, "r");

    try {
      // Leer una parte pequeña suficiente para el header
      const headerBuffer = Buffer.alloc(4096);
      const { bytesRead: headerBytesRead } = fs.readSync(fd, headerBuffer, 0, 4096, 0)
        ? { bytesRead: 4096 }
        : { bytesRead: 0 };

      if (headerBytesRead === 0) {
        throw new Error(`Could not read header from file: ${filePath}`);
      }

      const parser = new NPYHeaderParser(headerBuffer, true);
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
        const bytesRead = fs.readSync(fd, chunk, 0, size, filePosition);

        if (bytesRead <= 0) break;

        sharedView.set(chunk.subarray(0, bytesRead), sharedPosition);

        filePosition += bytesRead;
        sharedPosition += bytesRead;
      }

      return {
        version: 1,
        source: {
          format: "npy",
          path: filePath
        },
        shape: metadata.shape,
        rowCount: totalElements,
        columnCount: 1,
        schema: [
          {
            name: "values",
            dtype: metadata.descr,
            offset: 0,
            length: totalElements
          }
        ],
        buffer: sharedBuffer,
        metadata: {
          fortranOrder: metadata.fortran_order,
          originalShape: metadata.shape,
          ndim: metadata.shape.length,
          elementSize: TA.BYTES_PER_ELEMENT
        }
      };
    } finally {
      fs.closeSync(fd);
    }
  }

  // ─────────────────────────────────────────────
  // 🧵 WORKER POOL
  // ─────────────────────────────────────────────
  // Se mantiene para futuras operaciones de transformación
  // o validación, aunque este flujo ya no depende de sum/mean.
  // ─────────────────────────────────────────────
  _initWorkers() {
    for (let i = 0; i < this.config.workers; i++) {
      const worker = new Worker(
        new URL("../workers/npy-worker.js", import.meta.url)
      );

      worker.on("message", (msg) => {
        if (!worker._task) return;

        const { resolve } = worker._task;
        worker._task = null;
        this.idle.push(worker);
        resolve(msg);
        this._next();
      });

      worker.on("error", (err) => {
        if (worker._task?.reject) {
          worker._task.reject(err);
        }
        worker._task = null;
        this.idle.push(worker);
        this._next();
      });

      this.pool.push(worker);
      this.idle.push(worker);
    }
  }

  _runWorker(buffer, dtype) {
    return new Promise((resolve, reject) => {
      this.queue.push({ buffer, dtype, resolve, reject });
      this._next();
    });
  }

  _next() {
    if (this.queue.length === 0 || this.idle.length === 0) return;

    const worker = this.idle.pop();
    const task = this.queue.shift();

    worker._task = task;

    worker.postMessage(
      { buffer: task.buffer, dtype: task.dtype },
      [task.buffer.buffer]
    );
  }
}