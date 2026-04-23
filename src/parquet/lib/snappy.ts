// ════════════════════════════════════════════════════════════════════════════
// @cervid/decomposer — snappy.ts
// Pure Node.js Snappy decompressor, zero dependencies
// ════════════════════════════════════════════════════════════════════════════

/**
 * Helper de copia Snappy: permite solapamiento (RLE).
 */
export function snappyCopy(dst: Uint8Array | Buffer, dstPos: number, offset: number, length: number): void {
  const srcPos = dstPos - offset;
  for (let i = 0; i < length; i++) {
    dst[dstPos + i] = dst[srcPos + i];
  }
}

/**
 * Descomprime un buffer con formato Snappy.
 */
export function snappyDecompress(src: Uint8Array | Buffer): Buffer {
  let pos = 0;

  // Leer longitud descomprimida (varint)
  let uncompressedLen = 0;
  let shift = 0;

  while (pos < src.length) {
    const b = src[pos++];
    uncompressedLen |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) {
      break;
    }
    shift += 7;
  }

  const dst = Buffer.allocUnsafe(uncompressedLen);
  let dstPos = 0;

  while (pos < src.length && dstPos < uncompressedLen) {
    const tag = src[pos++];
    const tagType = tag & 0x03;

    if (tagType === 0x00) {
      // ── LITERAL ───────────────────────────────────────────────────────────
      const lenCode = (tag >> 2) & 0x3f;
      let length: number;

      if (lenCode < 60) {
        length = lenCode + 1;
      } else {
        const extraBytes = lenCode - 59;
        length = 0;
        for (let i = 0; i < extraBytes; i++) {
          if (pos >= src.length) {
            throw new Error('Snappy literal length overflow');
          }
          // Usamos desplazamiento de bits en lugar de Math.pow para mayor velocidad
          length += src[pos++] << (i * 8);
        }
        length = (length + 1) >>> 0;
      }

      if (pos + length > src.length || dstPos + length > uncompressedLen) {
        throw new Error('Snappy literal exceeds buffer bounds');
      }

      if (Buffer.isBuffer(src) && Buffer.isBuffer(dst)) {
        src.copy(dst, dstPos, pos, pos + length);
      } else {
        dst.set(src.subarray(pos, pos + length), dstPos);
      }
      pos += length;
      dstPos += length;
    } else {
      // ── COPY OPS ──────────────────────────────────────────────────────────
      let copyLen: number;
      let copyOffset: number;

      if (tagType === 0x01) {
        // COPY_1 (2 bytes)
        if (pos >= src.length) {
          throw new Error('Snappy COPY_1 missing offset byte');
        }
        copyLen = ((tag >> 2) & 0x07) + 4;
        copyOffset = (((tag >> 5) & 0x07) << 8) | src[pos++];
      } else if (tagType === 0x02) {
        // COPY_2 (3 bytes)
        if (pos + 1 >= src.length) {
          throw new Error('Snappy COPY_2 missing offset bytes');
        }
        copyLen = (tag >> 2) + 1;
        copyOffset = src[pos] | (src[pos + 1] << 8);
        pos += 2;
      } else {
        // COPY_4 (5 bytes)
        if (pos + 3 >= src.length) {
          throw new Error('Snappy COPY_4 missing offset bytes');
        }
        copyLen = (tag >> 2) + 1;
        copyOffset = src[pos] | (src[pos + 1] << 8) | (src[pos + 2] << 16) | (src[pos + 3] << 24);
        pos += 4;
      }

      if (copyOffset <= 0 || copyOffset > dstPos) {
        throw new Error(`Snappy COPY invalid offset: ${copyOffset}`);
      }
      if (dstPos + copyLen > uncompressedLen) {
        throw new Error('Snappy COPY exceeds destination buffer');
      }

      snappyCopy(dst, dstPos, copyOffset, copyLen);
      dstPos += copyLen;
    }
  }

  if (dstPos !== uncompressedLen) {
    throw new Error(`Snappy decompression incomplete: ${dstPos}/${uncompressedLen}`);
  }

  return dst;
}

/**
 * Escaneo estocástico para encontrar el inicio real del stream de compresión.
 */
export function findAndDecompress(
  raw: Buffer,
  afterHdr: number,
  expectedSize: number,
  // El codec de compresión (e.g. 'GZIP' o 'SNAPPY')
  codec: string,
  zlib: { gunzipSync: (buf: Buffer | Uint8Array) => Buffer },
): Buffer {
  let best: Buffer | null = null;
  let bestDiff = Infinity;

  // Rango de búsqueda para corregir errores de alineación del Thrift Header
  const start = Math.max(0, afterHdr - 4);
  const end = Math.min(raw.length - 1, afterHdr + 24);

  for (let off = start; off <= end; off++) {
    try {
      const slice = raw.subarray(off);
      let candidate: Buffer;

      if (codec === 'GZIP') {
        candidate = zlib.gunzipSync(slice);
      } else {
        candidate = snappyDecompress(slice);
      }

      const diff = Math.abs(candidate.length - expectedSize);
      if (diff < bestDiff) {
        bestDiff = diff;
        best = candidate;
        if (diff === 0) {
          break;
        } // Perfect match
      }
    } catch {
      // Offset inválido, continuar escaneo
    }
  }

  return best ?? Buffer.alloc(0);
}
