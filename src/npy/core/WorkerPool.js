import { Worker } from "worker_threads";

export class WorkerPool {
  constructor(workerPath, size = 4) {
    this.workers = [];
    this.queue = [];
    this.active = new Set();

    for (let i = 0; i < size; i++) {
      const worker = new Worker(workerPath, { type: "module" });

      worker.on("message", (msg) => this._done(worker, msg));
      worker.on("error", (err) => console.error(err));

      this.workers.push(worker);
    }
  }

  run(task) {
    return new Promise((resolve, reject) => {
      this.queue.push({ task, resolve, reject });
      this._next();
    });
  }

  _next() {
    const worker = this.workers.find(w => !this.active.has(w));
    if (!worker || this.queue.length === 0) return;

    const job = this.queue.shift();
    this.active.add(worker);

    worker._resolve = job.resolve;
    worker._reject = job.reject;

    worker.postMessage(job.task);
  }

  _done(worker, msg) {
    this.active.delete(worker);
    worker._resolve(msg);
    this._next();
  }
  async destroy() {
  await Promise.all(this.workers.map(w => w.terminate()));
}
}