export class IOScheduler {
  constructor(limit = 1) {
    this.limit = limit;
    this.active = 0;
    this.queue = [];
  }

  async schedule(task) {
    if (this.active >= this.limit) {
      await new Promise(res => this.queue.push(res));
    }

    this.active++;

    try {
      return await task();
    } finally {
      this.active--;
      if (this.queue.length > 0) {
        this.queue.shift()();
      }
    }
  }
}