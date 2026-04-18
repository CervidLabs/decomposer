import { open } from "fs/promises";

export class ChunkReader {
  constructor(filePath, offset, elementSize, chunkElements = 1_000_000) {
    this.filePath = filePath;
    this.offset = offset;
    this.chunkBytes = chunkElements * elementSize;
  }

  async *readChunks() {
    const file = await open(this.filePath, "r");

    let position = this.offset;

    while (true) {
      const buffer = Buffer.allocUnsafe(this.chunkBytes);

      const { bytesRead } = await file.read(buffer, 0, this.chunkBytes, position);

      if (bytesRead === 0) break;

      yield buffer.subarray(0, bytesRead);

      position += bytesRead;
    }

    await file.close();
  }
}