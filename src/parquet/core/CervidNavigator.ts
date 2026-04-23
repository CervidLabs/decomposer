import fs from 'fs';
import { type FileMetaData, ThriftReader } from './ThriftReader.js';

export class CervidNavigator {
  public path: string;
  public size: number;
  public metadataBuffer: Buffer | null = null;
  public metadataOffset = 0;
  public fileBuffer: SharedArrayBuffer | null = null;
  public metadata: FileMetaData | null = null;

  constructor(filePath: string) {
    this.path = filePath;
    this.size = fs.statSync(filePath).size;
  }

  scanFooter(): void {
    const fd = fs.openSync(this.path, 'r');

    try {
      const magic = Buffer.alloc(4);
      fs.readSync(fd, magic, 0, 4, this.size - 4);
      if (magic.toString() !== 'PAR1') {throw new Error('Not a Parquet file');}

      const lenBuf = Buffer.alloc(4);
      fs.readSync(fd, lenBuf, 0, 4, this.size - 8);
      const metadataLen = lenBuf.readInt32LE(0);

      this.metadataOffset = this.size - 8 - metadataLen;
      this.metadataBuffer = Buffer.alloc(metadataLen);
      fs.readSync(fd, this.metadataBuffer, 0, metadataLen, this.metadataOffset);

      const sab = new SharedArrayBuffer(this.size);
      const uint8View = new Uint8Array(sab);
      fs.readSync(fd, uint8View, 0, this.size, 0);
      this.fileBuffer = sab;

      const reader = new ThriftReader(this.metadataBuffer);
      this.metadata = reader.readFileMetaData();
    } finally {
      fs.closeSync(fd);
    }
  }

  discoverAllColumns(): Record<string, number> {
    if (!this.metadataBuffer) {return {};}
    const data = this.metadataBuffer;
    const results: Record<string, number> = {};
    const foundOffsets: number[] = [];

    for (let i = 0; i < data.length - 8; i++) {
      const ptr = Number(data.readBigInt64LE(i));
      if (ptr > 4 && ptr < this.metadataOffset) {
        if (!foundOffsets.includes(ptr)) {foundOffsets.push(ptr);}
      }
    }
    foundOffsets.sort((a, b) => a - b);

    const foundNames: string[] = [];
    const forbiddenNames = new Set(['parquet', 'pandas', 'schema']);

    for (let i = 0; i < data.length - 1; i++) {
      const len = data[i];
      if (len >= 2 && len <= 32 && i + 1 + len <= data.length) {
        const name = data.subarray(i + 1, i + 1 + len).toString('utf8');
        if (/^[a-z0-9_]+$/i.test(name) && !forbiddenNames.has(name.toLowerCase())) {
          if (!foundNames.includes(name)) {foundNames.push(name);}
          i += len;
        }
      }
    }

    foundNames.forEach((name, idx) => {
      const offset = foundOffsets[idx];
      if (offset !== undefined) {results[name] = offset;}
    });

    return results;
  }

  getBufferAt(offset: number, length: number): ArrayBuffer {
    const fd = fs.openSync(this.path, 'r');
    try {
      const buffer = Buffer.alloc(Math.min(length, this.size - offset));
      fs.readSync(fd, buffer, 0, buffer.length, offset);
      return buffer.buffer;
    } finally {
      fs.closeSync(fd);
    }
  }

  getDictionary(offset: number): string[] {
    const fd = fs.openSync(this.path, 'r');
    try {
      const searchSize = 4096;
      const startSearch = Math.max(0, offset - searchSize);
      const buffer = Buffer.alloc(searchSize);
      fs.readSync(fd, buffer, 0, searchSize, startSearch);

      const strings: string[] = [];
      for (let i = 0; i < buffer.length - 4; i++) {
        const strLen = buffer.readInt32LE(i);
        if (strLen > 0 && strLen < 32 && i + 4 + strLen <= buffer.length) {
          const candidate = buffer.subarray(i + 4, i + 4 + strLen).toString('utf8');
          if (/^[A-D]$/.test(candidate)) {
            if (!strings.includes(candidate)) {strings.push(candidate);}
          }
        }
      }
      return strings.sort();
    } finally {
      fs.closeSync(fd);
    }
  }
}
