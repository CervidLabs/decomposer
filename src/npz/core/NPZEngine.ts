import fs from 'node:fs';
import zlib from 'node:zlib';
import { NPYHeaderParser } from '../../npy/core/NPYHeaderParser.js';
import { getTypedArray } from '../../npy/lib/dtypes.js';
import type { NPYProcessResult } from '../../npy/core/NPYEngine.js'; // Reutilizamos la interfaz anterior

const ZIP_LOCAL_FILE_HEADER = 0x04034b50;

/**
 * Resultado del procesamiento de un archivo .npz
 */
export interface NPZProcessResult {
  version: number;
  source: {
    format: 'npz';
    path: string;
  };
  names: string[];
  arrays: Record<string, NPYProcessResult>;
  metadata: {
    entryCount: number;
  };
}

interface NPZEngineConfig {
  // Posibles futuras opciones de configuración
  [key: string]: unknown;
}

export class NPZEngine {
  private readonly config: NPZEngineConfig;

  constructor(config: NPZEngineConfig = {}) {
    this.config = config;
  }

  /**
   * Procesa un archivo NPZ recorriendo sus headers locales de ZIP
   */
  public async process(filePath: string): Promise<NPZProcessResult> {
    const fileBuffer: Buffer = fs.readFileSync(filePath);

    const arrays: Record<string, NPYProcessResult> = {};
    const names: string[] = [];

    let offset = 0;

    while (offset < fileBuffer.length) {
      // Validar si el buffer restante es suficiente para leer la firma
      if (offset + 4 > fileBuffer.length) {
        break;
      }

      const signature = fileBuffer.readUInt32LE(offset);

      // Si ya no hay más local headers (p.ej. llegamos al Central Directory), terminamos
      if (signature !== ZIP_LOCAL_FILE_HEADER) {
        break;
      }

      const compressionMethod = fileBuffer.readUInt16LE(offset + 8);
      const compressedSize = fileBuffer.readUInt32LE(offset + 18);
      const uncompressedSize = fileBuffer.readUInt32LE(offset + 22);
      const fileNameLength = fileBuffer.readUInt16LE(offset + 26);
      const extraFieldLength = fileBuffer.readUInt16LE(offset + 28);

      const fileNameStart = offset + 30;
      const fileNameEnd = fileNameStart + fileNameLength;
      const entryName = fileBuffer.toString('utf8', fileNameStart, fileNameEnd);

      const dataStart = fileNameEnd + extraFieldLength;
      const dataEnd = dataStart + compressedSize;

      const compressedData = fileBuffer.subarray(dataStart, dataEnd);

      let entryData: Buffer;

      // 0 = stored (sin compresión), 8 = deflate
      if (compressionMethod === 0) {
        entryData = compressedData;
      } else if (compressionMethod === 8) {
        // zlib.inflateRawSync devuelve un Buffer
        entryData = zlib.inflateRawSync(compressedData);
      } else {
        throw new Error(`Unsupported ZIP compression method ${compressionMethod} in entry ${entryName}`);
      }

      if (entryData.length !== uncompressedSize) {
        throw new Error(`Size mismatch in ${entryName}: expected ${uncompressedSize}, got ${entryData.length}`);
      }

      if (entryName.endsWith('.npy')) {
        const parsed = this._parseNPYEntry(entryData, entryName);
        const baseName = entryName.replace(/\.npy$/i, '');
        arrays[baseName] = parsed;
        names.push(baseName);
      }

      // El siguiente header comienza después de los datos comprimidos
      offset = dataEnd;
    }

    return new Promise((resolve) => {
      resolve({
        version: 1,
        source: {
          format: 'npz',
          path: filePath,
        },
        names,
        arrays,
        metadata: {
          entryCount: names.length,
        },
      });
    });
  }

  /**
   * Parsea un archivo .npy interno extraído del ZIP
   */
  private _parseNPYEntry(buffer: Buffer, entryName: string): NPYProcessResult {
    const parser = new NPYHeaderParser(buffer);
    const { metadata, dataOffset } = parser.parse();

    const TA = getTypedArray(metadata.descr);
    const totalElements = metadata.shape.reduce((acc, dim) => acc * dim, 1);
    const totalBytes = buffer.length - dataOffset;

    const data = buffer.subarray(dataOffset);

    // Creamos el SharedArrayBuffer para que Cervid/Data lo use eficientemente
    const sharedBuffer = new SharedArrayBuffer(totalBytes);
    new Uint8Array(sharedBuffer).set(data);

    return {
      version: 1,
      source: {
        format: 'npy',
        path: entryName,
      },
      shape: metadata.shape,
      rowCount: totalElements,
      columnCount: 1,
      schema: [
        {
          name: 'values',
          dtype: metadata.descr,
          offset: 0,
          length: totalElements,
        },
      ],
      buffer: sharedBuffer,
      metadata: {
        fortranOrder: false, // El parser actual debería reportar esto en metadata
        originalShape: metadata.shape,
        ndim: metadata.shape.length,
        elementSize: TA.BYTES_PER_ELEMENT,
      },
    };
  }
}
