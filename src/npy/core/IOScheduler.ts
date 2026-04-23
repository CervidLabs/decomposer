/**
 * Tipo para una tarea asíncrona que retorna una promesa de tipo T
 */
type Task<T> = () => Promise<T> | T;

export class IOScheduler {
  private limit: number;
  private active: number;
  private queue: Array<() => void>;

  constructor(limit = 1) {
    this.limit = limit;
    this.active = 0;
    this.queue = [];
  }

  /**
   * Ejecuta una tarea respetando el límite de concurrencia.
   * @param task Función asíncrona a ejecutar.
   * @returns El resultado de la tarea.
   */
  async schedule<T>(task: Task<T>): Promise<T> {
    if (this.active >= this.limit) {
      // Esperamos a que haya un espacio disponible en la cola
      await new Promise<void>((resolve) => {
        this.queue.push(resolve);
      });
    }

    this.active++;

    try {
      return await task();
    } finally {
      this.active--;

      // Si hay tareas esperando, resolvemos la siguiente en la cola
      if (this.queue.length > 0) {
        const next = this.queue.shift();
        if (next) {
          next();
        }
      }
    }
  }
}
