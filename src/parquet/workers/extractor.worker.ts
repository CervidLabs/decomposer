import { parentPort, type Transferable } from 'worker_threads';
import zlib from 'zlib';

import { snappyDecompress, findAndDecompress } from '../lib/snappy.js';
import { ThriftPage } from '../lib/thrift.js';
import { makeOutput, readPlainInto } from '../lib/plain.js';
import { decodePage } from '../lib/encodings.js';

// ── Interfaces ───────────────────────────────────────────────────────────────

type ParquetTypeStr = 'BOOLEAN' | 'INT32' | 'INT64' | 'INT96' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'FIXED_LEN_BYTE_ARRAY';
type SupportedTypedArray = Int32Array | Float64Array | Float32Array | Uint8Array;

interface ParquetTask {
  id: string | number;
  fileBuffer: ArrayBufferLike;
  offset: number;
  length: number;
  compression: string;
  encoding: string;
  type: ParquetTypeStr;
  numRows: number;
  dictOffset?: number;
  sampleOnly?: boolean;
}
// ── Helpers ──────────────────────────────────────────────────────────────────

function typeStride(type: ParquetTypeStr): number {
  if (type === 'INT32' || type === 'FLOAT') {
    return 4;
  }
  if (type === 'INT64' || type === 'DOUBLE') {
    return 8;
  }
  return 0;
}

function fmt(v: unknown): string {
  if (v === null || v === undefined) {
    return 'null';
  }
  if (typeof v === 'number') {
    if (Number.isNaN(v)) {
      return 'null';
    }
    return Number.isFinite(v) ? v.toFixed(6) : String(v);
  }
  if (typeof v === 'boolean') {
    return String(v);
  }
  // Manejo explícito de BigInt (común en INT64 de Parquet)
  if (typeof v === 'bigint') {
    return v.toString();
  }
  // Si es un string, lo devolvemos tal cual
  if (typeof v === 'string') {
    return v;
  }
  return JSON.stringify(v);
}

function sliceFileBuffer(fileBuffer: ArrayBufferLike, start: number, end: number): Buffer {
  const safeStart = Math.max(0, start);
  const safeEnd = Math.min(fileBuffer.byteLength, end);
  if (safeEnd <= safeStart) {
    return Buffer.alloc(0);
  }
  return Buffer.from(fileBuffer, safeStart, safeEnd - safeStart);
}

function trimToStride(buf: Buffer, stride: number): Buffer {
  if (!buf || stride <= 0) {
    return buf;
  }
  const usable = buf.length - (buf.length % stride);
  return usable > 0 ? buf.subarray(0, usable) : buf;
}

// ── Dictionary Logic ─────────────────────────────────────────────────────────

function readDictionaryPage(params: {
  fileBuffer: ArrayBufferLike;
  dictOffset: number;
  dataOffset: number;
  compression: string;
  type: ParquetTypeStr;
}) {
  const { fileBuffer, dictOffset, dataOffset, compression, type } = params;
  if (dictOffset < 0 || dictOffset >= dataOffset) {
    return null;
  }

  const dictChunk = sliceFileBuffer(fileBuffer, dictOffset, dataOffset);
  if (dictChunk.length === 0) {
    return null;
  }

  const pageReader = new ThriftPage(dictChunk);
  const pageHdr = pageReader.readPageHeader();

  if (pageHdr.headerSize <= 0 || pageHdr.pageType !== 2) {
    return null;
  }

  const payloadStart = pageHdr.headerSize;
  const payloadEnd = Math.min(dictChunk.length, pageHdr.headerSize + pageHdr.compressedSize);
  const payload = dictChunk.subarray(payloadStart, payloadEnd);

  let dictPlain: Buffer | null;
  try {
    if (compression === 'UNCOMPRESSED') {
      dictPlain = payload;
    } else if (compression === 'SNAPPY') {
      dictPlain = snappyDecompress(payload);
    } else if (compression === 'GZIP') {
      dictPlain = zlib.gunzipSync(payload);
    } else {
      dictPlain = findAndDecompress(dictChunk, pageHdr.headerSize, pageHdr.uncompressedSize, compression, zlib);
    }
  } catch {
    return null;
  }

  if (!dictPlain || dictPlain.length === 0) {
    return null;
  }

  const stride = typeStride(type);
  const effectiveDictPlain = stride > 0 ? trimToStride(dictPlain, stride) : dictPlain;
  const effectiveCount = stride > 0 ? Math.floor(effectiveDictPlain.length / stride) : pageHdr.numValues;

  const dictionary = new Array<string | number>(effectiveCount);
  readPlainInto(effectiveDictPlain, 0, type, effectiveCount, dictionary as string[], 0);

  return { dictionary };
}

// ── Worker Main ──────────────────────────────────────────────────────────────

if (parentPort) {
  parentPort.on('message', (task: ParquetTask) => {
    const { id, fileBuffer, offset, length, compression, type, numRows, dictOffset, sampleOnly } = task;

    try {
      const maxRows = sampleOnly ? 5 : numRows;
      const out = makeOutput(type, maxRows);
      let outPos = 0;
      let actualEncoding = task.encoding ?? 'PLAIN';
      let dictionary: (string | number)[] | null = null;

      // 1) Carga de Diccionario
      if (dictOffset !== undefined && dictOffset !== -1) {
        const dictResult = readDictionaryPage({ fileBuffer, dictOffset, dataOffset: offset, compression, type });
        if (dictResult) {
          dictionary = dictResult.dictionary;
        }
      }

      // 2) Lectura de Páginas de Datos
      const dataChunkEnd = Math.min(fileBuffer.byteLength, offset + length);
      const dataChunk = sliceFileBuffer(fileBuffer, offset, dataChunkEnd);
      let pos = 0;

      while (pos < dataChunk.length && outPos < maxRows) {
        const remaining = dataChunk.subarray(pos);
        const pageReader = new ThriftPage(remaining);
        const pageHdr = pageReader.readPageHeader();

        if (pageHdr.headerSize <= 0) {
          break;
        }

        const compSize = Math.max(0, pageHdr.compressedSize);

        // Saltos de página (Dict accidental o no-data)
        if (pageHdr.pageType === 2 || (pageHdr.pageType !== 0 && pageHdr.pageType !== 3)) {
          pos += pageHdr.headerSize + compSize;
          continue;
        }

        const afterHdr = pageHdr.headerSize;
        actualEncoding = pageHdr.dataEncoding ?? task.encoding ?? 'PLAIN';

        // Obtener datos de la página (Zero-copy subarray)
        const pageRaw = dataChunk.subarray(pos, pos + afterHdr + compSize + (compression !== 'UNCOMPRESSED' ? 32 : 0));

        let data: Buffer;
        if (compression === 'UNCOMPRESSED') {
          data = pageRaw;
        } else {
          const payload = findAndDecompress(pageRaw, afterHdr, pageHdr.uncompressedSize, compression, zlib);
          data = Buffer.concat([pageRaw.subarray(0, afterHdr), payload]);
        }

        const toRead = Math.min(pageHdr.numValues, maxRows - outPos);
        const written = decodePage(data, afterHdr, actualEncoding, type, dictionary, toRead, out, outPos);

        outPos += written;
        pos += afterHdr + compSize;
      }

      // 3) Preparar Salida Final
      let finalOut: SupportedTypedArray | string[];
      if (Array.isArray(out)) {
        finalOut = outPos < out.length ? out.slice(0, outPos) : out;
      } else {
        finalOut = outPos < out.length ? out.subarray(0, outPos) : out;
      }

      const sampleArr = Array.isArray(finalOut) ? finalOut.slice(0, 5) : Array.from(finalOut.subarray(0, 5));
      const sample = sampleArr.map(fmt);

      const transferList: Transferable[] = [];

      // Verificamos que sea un TypedArray y NO un array de strings
      if (!Array.isArray(finalOut) && ArrayBuffer.isView(finalOut)) {
        // Verificamos si es un ArrayBuffer normal (transferible)
        // o SharedArrayBuffer (no transferible)
        if (finalOut.buffer instanceof ArrayBuffer) {
          transferList.push(finalOut.buffer);
        }
      }
      parentPort!.postMessage(
        {
          id,
          status: 'DONE',
          type,
          compression,
          encoding: actualEncoding,
          rowCount: outPos,
          data: finalOut,
          sample,
        },
        transferList,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      parentPort!.postMessage({
        id,
        status: 'ERROR',
        type,
        compression,
        encoding: 'UNKNOWN',
        rowCount: 0,
        data: [],
        sample: [`[Worker Error: ${msg}]`],
      });
    }
  });
}
