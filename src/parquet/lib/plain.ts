// ── Types ─────────────────────────────────────────────────────────────────────

type ParquetTypeStr = 'BOOLEAN' | 'INT32' | 'INT64' | 'INT96' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'FIXED_LEN_BYTE_ARRAY';

type SupportedTypedArray = Int32Array | Float64Array | Float32Array | Uint8Array;

// ── Output Helpers ───────────────────────────────────────────────────────────

/**
 * Crea el TypedArray correcto para un tipo de Parquet.
 */
export function makeOutput(type: ParquetTypeStr, size: number): SupportedTypedArray | string[] {
  switch (type) {
    case 'INT32':
      return new Int32Array(size);
    case 'INT64':
      return new Float64Array(size); // JS no tiene Int64Array nativo para Float
    case 'DOUBLE':
      return new Float64Array(size);
    case 'FLOAT':
      return new Float32Array(size);
    case 'BOOLEAN':
      return new Uint8Array(size);
    case 'BYTE_ARRAY':
      return new Array<string>(size);
    default:
      return new Array<string>(size);
  }
}

/**
 * Valor centinela para nulos por tipo.
 */
export function nullValue(type: ParquetTypeStr): number | null {
  if (type === 'DOUBLE' || type === 'FLOAT' || type === 'INT64') {
    return NaN;
  }
  if (type === 'INT32') {
    return -2147483648;
  } // INT32_MIN
  if (type === 'BOOLEAN') {
    return 255;
  }
  return null; // BYTE_ARRAY
}

// ── PLAIN Decoder ────────────────────────────────────────────────────────────

/**
 * Lee valores con codificación PLAIN y los escribe en un array de salida.
 */
export function readPlainInto(
  buf: Uint8Array,
  offset: number,
  type: ParquetTypeStr,
  count: number,
  out: SupportedTypedArray | string[],
  outPos: number,
): number {
  // Usamos DataView para lectura multi-plataforma (Little Endian)
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let currentOffset = offset;
  let written = 0;

  if (type === 'INT32' && out instanceof Int32Array) {
    for (let i = 0; i < count && currentOffset + 4 <= buf.length; i++) {
      out[outPos + written++] = view.getInt32(currentOffset, true);
      currentOffset += 4;
    }
  } else if (type === 'INT64' && out instanceof Float64Array) {
    for (let i = 0; i < count && currentOffset + 8 <= buf.length; i++) {
      // BigInt a Number para almacenamiento en Float64Array
      out[outPos + written++] = Number(view.getBigInt64(currentOffset, true));
      currentOffset += 8;
    }
  } else if (type === 'DOUBLE' && out instanceof Float64Array) {
    for (let i = 0; i < count && currentOffset + 8 <= buf.length; i++) {
      out[outPos + written++] = view.getFloat64(currentOffset, true);
      currentOffset += 8;
    }
  } else if (type === 'FLOAT' && out instanceof Float32Array) {
    for (let i = 0; i < count && currentOffset + 4 <= buf.length; i++) {
      out[outPos + written++] = view.getFloat32(currentOffset, true);
      currentOffset += 4;
    }
  } else if (type === 'BYTE_ARRAY' && Array.isArray(out)) {
    const decoder = new TextDecoder();
    for (let i = 0; i < count; i++) {
      if (currentOffset + 4 > buf.length) {
        break;
      }
      const len = view.getInt32(currentOffset, true);
      currentOffset += 4;
      if (len < 0 || currentOffset + len > buf.length) {
        break;
      }

      // Zero-copy subarray + TextDecoder
      out[outPos + written++] = decoder.decode(buf.subarray(currentOffset, currentOffset + len));
      currentOffset += len;
    }
  } else if (type === 'BOOLEAN' && out instanceof Uint8Array) {
    // Parquet BOOLEAN PLAIN usa bit-packing (1 bit por valor, LSB first)
    for (let i = 0; i < count; i++) {
      const byteIdx = Math.floor(i / 8);
      const bitIdx = i % 8;
      if (currentOffset + byteIdx >= buf.length) {
        break;
      }
      out[outPos + written++] = (buf[currentOffset + byteIdx] >> bitIdx) & 1;
    }
  }

  return written;
}

/**
 * Salta el bloque de niveles de definición.
 */
export function skipDefLevels(buf: Uint8Array, offset: number): number {
  if (offset + 4 > buf.length) {
    return offset;
  }

  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const levelLen = view.getInt32(offset, true);

  if (levelLen > 0 && levelLen < buf.length - offset - 4) {
    return offset + 4 + levelLen;
  }

  return offset;
}
