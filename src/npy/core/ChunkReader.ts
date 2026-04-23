import { type FileHandle, open } from 'node:fs/promises';

export class ChunkReader {
  private readonly filePath: string;
  private readonly offset: number;
  private readonly chunkBytes: number;

  constructor(filePath: string, offset: number, elementSize: number, chunkElements = 1_000_000) {
    this.filePath = filePath;
    this.offset = offset;
    this.chunkBytes = chunkElements * elementSize;
  }

  /**
   * Lee el archivo por trozos (chunks) de forma asíncrona.
   * Utiliza un generador para no cargar todo el archivo en memoria.
   */
  async *readChunks(): AsyncGenerator<Buffer, void, unknown> {
    let file: FileHandle | null = null;

    try {
      file = await open(this.filePath, 'r');
      let position = this.offset;

      while (true) {
        // Buffer.allocUnsafe es más rápido para alto rendimiento,
        // pero hay que asegurarse de usar subarray(0, bytesRead)
        const buffer = Buffer.allocUnsafe(this.chunkBytes);

        const { bytesRead } = await file.read(buffer, 0, this.chunkBytes, position);

        if (bytesRead === 0) {
          break;
        }

        // Emitimos solo la porción del buffer que contiene datos reales
        yield buffer.subarray(0, bytesRead);

        position += bytesRead;
      }
    } finally {
      // El bloque finally asegura que el descriptor de archivo
      // se libere aunque la iteración falle o se detenga
      if (file) {
        await file.close();
      }
    }
  }
}
