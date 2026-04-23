import { Worker } from 'worker_threads';

/**
 * Representa la estructura de un error controlado
 */
interface WorkerError extends Error {
  code?: string;
}

/**
 * Representa una tarea pendiente en la cola con tipado estricto
 */
interface Job<TTask, TResult> {
  task: TTask;
  resolve: (value: TResult) => void;
  reject: (reason: WorkerError | Error) => void;
}

export class WorkerPool<TTask, TResult> {
  private workers: Worker[] = [];
  private queue: Job<TTask, TResult>[] = [];

  /**
   * Mapa para asociar un Worker con su Job actual.
   * Se usa Map para evitar extender el objeto Worker.
   */
  private activeJobs = new Map<Worker, Job<TTask, TResult>>();

  constructor(workerPath: string | URL, size = 4) {
    for (let i = 0; i < size; i++) {
      const worker = new Worker(workerPath);

      // Usamos una función anónima para capturar el mensaje y asegurar el tipo
      worker.on('message', (msg: TResult) => {
        this._handleWorkerDone(worker, msg);
      });

      worker.on('error', (err: Error) => {
        this._handleWorkerError(worker, err);
      });

      this.workers.push(worker);
    }
  }

  /**
   * Envía una tarea al pool de workers.
   */
  public async run(task: TTask): Promise<TResult> {
    return new Promise<TResult>((resolve, reject) => {
      this.queue.push({ task, resolve, reject });
      this._processNext();
    });
  }

  /**
   * Intenta asignar la siguiente tarea de la cola a un worker disponible
   */
  private _processNext(): void {
    if (this.queue.length === 0) {
      return;
    }

    // Buscamos un worker que no esté procesando nada actualmente
    const idleWorker = this.workers.find((w) => !this.activeJobs.has(w));

    if (!idleWorker) {
      return;
    }

    const job = this.queue.shift();
    if (!job) {
      return;
    }

    this.activeJobs.set(idleWorker, job);
    idleWorker.postMessage(job.task);
  }

  /**
   * Se ejecuta cuando un worker termina su tarea con éxito
   */
  private _handleWorkerDone(worker: Worker, msg: TResult): void {
    const job = this.activeJobs.get(worker);

    if (job) {
      this.activeJobs.delete(worker);
      job.resolve(msg);
    }

    this._processNext();
  }

  /**
   * Se ejecuta cuando un worker encuentra un error
   */
  private _handleWorkerError(worker: Worker, err: Error): void {
    const job = this.activeJobs.get(worker);

    if (job) {
      this.activeJobs.delete(worker);
      job.reject(err);
    }

    this._processNext();
  }

  /**
   * Finaliza todos los workers y limpia las estructuras de datos
   */
  public async destroy(): Promise<void> {
    const terminations = this.workers.map(async (w) => w.terminate());
    await Promise.all(terminations);

    this.workers = [];
    this.activeJobs.clear();
    this.queue = [];
  }
}
