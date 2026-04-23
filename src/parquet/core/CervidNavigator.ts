import fs from 'fs';
import { type FileMetaData, ThriftReader } from './ThriftReader.js';

/**
 * CervidNavigator se encarga de mapear la estructura física del archivo Parquet.
 * Utiliza SharedArrayBuffer para permitir que los Workers accedan a los datos
 * sin copias de memoria (Zero-Copy).
 */
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

  /**
   * Escanea el footer del archivo Parquet, extrae los metadatos Thrift
   * y carga el archivo completo en un SharedArrayBuffer.
   */
  scanFooter(): void {
    const fd = fs.openSync(this.path, 'r');

    try {
      // 1. Validar Magic Number (PAR1)
      const magic = Buffer.alloc(4);
      fs.readSync(fd, magic, 0, 4, this.size - 4);
      if (magic.toString() !== 'PAR1') {
        throw new Error('Not a Parquet file');
      }

      // 2. Leer longitud de metadatos (4 bytes antes del magic final)
      const lenBuf = Buffer.alloc(4);
      fs.readSync(fd, lenBuf, 0, 4, this.size - 8);
      const metadataLen = lenBuf.readInt32LE(0);

      // 3. Extraer metadatos Thrift
      this.metadataOffset = this.size - 8 - metadataLen;
      this.metadataBuffer = Buffer.alloc(metadataLen);
      fs.readSync(fd, this.metadataBuffer, 0, metadataLen, this.metadataOffset);

      // 4. Cargar archivo completo en SharedArrayBuffer para acceso multi-hilo
      const sab = new SharedArrayBuffer(this.size);
      const uint8View = new Uint8Array(sab);
      fs.readSync(fd, uint8View, 0, this.size, 0);
      this.fileBuffer = sab;

      // 5. Parsear metadatos
      const reader = new ThriftReader(this.metadataBuffer);
      this.metadata = reader.readFileMetaData();
    } finally {
      fs.closeSync(fd);
    }
  }

  /**
   * Busca heurísticamente columnas y offsets basándose en patrones de bytes.
   * Útil cuando el esquema Thrift está incompleto o corrupto.
   */
  discoverAllColumns(): Record<string, number> {
    if (!this.metadataBuffer) {
      return {};
    }

    const data = this.metadataBuffer;
    const results: Record<string, number> = {};
    const foundOffsets: number[] = [];

    // 1. Buscar BigInt64 que parezcan offsets válidos (punteros a datos)
    for (let i = 0; i < data.length - 8; i++) {
      const ptr = Number(data.readBigInt64LE(i));
      if (ptr > 4 && ptr < this.metadataOffset) {
        if (!foundOffsets.includes(ptr)) {
          foundOffsets.push(ptr);
        }
      }
    }
    foundOffsets.sort((a, b) => a - b);

    // 2. Buscar strings que parezcan nombres de columna (Pascal strings 2-32 chars)
    const foundNames: string[] = [];
    const forbiddenNames = new Set(['parquet', 'pandas', 'schema']);

    for (let i = 0; i < data.length - 1; i++) {
      const len = data[i];
      if (len >= 2 && len <= 32 && i + 1 + len <= data.length) {
        const name = data.subarray(i + 1, i + 1 + len).toString('utf8');
        if (/^[a-z0-9_]+$/i.test(name) && !forbiddenNames.has(name.toLowerCase())) {
          if (!foundNames.includes(name)) {
            foundNames.push(name);
          }
          i += len;
        }
      }
    }

    // 3. Mapeo por proximidad y orden secuencial
    foundNames.forEach((name, idx) => {
      const offset = foundOffsets[idx];
      if (offset !== undefined) {
        results[name] = offset;
      }
    });

    return results;
  }

  /**
   * Obtiene una rebanada de memoria del archivo.
   */
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

  /**
   * Recupera el diccionario de strings buscando patrones de longitud prefijada.
   */
  getDictionary(offset: number): string[] {
    const fd = fs.openSync(this.path, 'r');
    try {
      // Escaneamos un bloque de 4KB previo al offset de datos
      const searchSize = 4096;
      const startSearch = Math.max(0, offset - searchSize);
      const buffer = Buffer.alloc(searchSize);
      fs.readSync(fd, buffer, 0, searchSize, startSearch);

      const strings: string[] = [];

      // Parquet Plain Dictionary: [4 bytes Length][UTF-8 Bytes]
      for (let i = 0; i < buffer.length - 4; i++) {
        const strLen = buffer.readInt32LE(i);
        if (strLen > 0 && strLen < 32 && i + 4 + strLen <= buffer.length) {
          const candidate = buffer.subarray(i + 4, i + 4 + strLen).toString('utf8');

          // Filtro categórico para Octopus (A-D)
          if (/^[A-D]$/.test(candidate)) {
            if (!strings.includes(candidate)) {
              strings.push(candidate);
            }
          }
        }
      }
      return strings.sort();
    } finally {
      fs.closeSync(fd);
    }
  }
}
