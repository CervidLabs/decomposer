// ════════════════════════════════════════════════════════════════════════════
// @cervid/decomposer — encodings.ts
// ════════════════════════════════════════════════════════════════════════════

import * as plain from './plain.js';

// Re-utilizamos las utilidades tipadas del módulo plain
const skipDefLevels = plain.skipDefLevels;
const nullValue = plain.nullValue;

type ParquetTypeStr = 'BOOLEAN' | 'INT32' | 'INT64' | 'INT96' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'FIXED_LEN_BYTE_ARRAY';
type SupportedTypedArray = Int32Array | Float64Array | Float32Array | Uint8Array;

interface BitWidthInfo {
  lvlEnd: number;
  bitWidth: number;
  levelLen: number;
  effectiveStart: number;
}

// ── RLE / Bit-Packing hybrid ──────────────────────────────────────────────────

export function decodeRLE(buf: Uint8Array, offset: number, bitWidth: number, maxValues: number): number[] {
  const results: number[] = [];
  let pos = offset;
  const mask = bitWidth < 32 ? (1 << bitWidth) - 1 : 0xffffffff;

  while (pos < buf.length && results.length < maxValues) {
    let header = 0;
    let shift = 0;

    // Read Varint Header
    while (pos < buf.length) {
      const byte = buf[pos++];
      header |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        break;
      }
      shift += 7;
    }

    if ((header & 1) === 0) {
      // RLE run
      const runLen = header >> 1;
      const byteWidth = Math.max(1, Math.ceil(bitWidth / 8));
      let value = 0;
      for (let b = 0; b < byteWidth && pos < buf.length; b++) {
        value |= buf[pos++] << (b * 8);
      }
      value &= mask;
      const take = Math.min(runLen, maxValues - results.length);
      for (let i = 0; i < take; i++) {
        results.push(value);
      }
    } else {
      // Bit-packed
      const numGroups = header >> 1;
      const numVals = numGroups * 8;
      const bytesUsed = Math.ceil((numVals * bitWidth) / 8);
      const end = Math.min(pos + bytesUsed, buf.length);
      let bitBuf = 0;
      let bitsLeft = 0;
      let decodedCount = 0;

      while (decodedCount < numVals && results.length < maxValues) {
        while (bitsLeft < bitWidth && pos < end) {
          bitBuf |= buf[pos++] << bitsLeft;
          bitsLeft += 8;
        }
        results.push(bitBuf & mask);
        bitBuf >>>= bitWidth;
        bitsLeft -= bitWidth;
        decodedCount++;
      }
      pos = end;
    }
  }
  return results;
}

// ── BIT_PACKED (legacy) ───────────────────────────────────────────────────────

export function decodeBitPacked(buf: Uint8Array, offset: number, bitWidth: number, maxValues: number): number[] {
  const results: number[] = [];
  let pos = offset;
  const mask = (1 << bitWidth) - 1;
  let bitBuf = 0;
  let bitsLeft = 0;

  while (pos <= buf.length && results.length < maxValues) {
    while (bitsLeft < bitWidth) {
      if (pos >= buf.length) {
        break;
      }
      bitBuf |= buf[pos++] << bitsLeft;
      bitsLeft += 8;
    }
    if (bitsLeft < bitWidth) {
      break;
    }
    results.push(bitBuf & mask);
    bitBuf >>>= bitWidth;
    bitsLeft -= bitWidth;
  }
  return results;
}

// ── DELTA_BINARY_PACKED ───────────────────────────────────────────────────────

export function decodeDeltaBinaryPacked(buf: Uint8Array, offset: number, maxValues: number): number[] {
  const results: number[] = [];
  let pos = offset;

  const rv = (): number => {
    let r = 0,
      s = 0;
    while (pos < buf.length) {
      const b = buf[pos++];
      r |= (b & 0x7f) << s;
      if (!(b & 0x80)) {
        break;
      }
      s += 7;
    }
    return r >>> 0;
  };

  const rz = (): number => {
    const n = rv();
    return (n >>> 1) ^ -(n & 1);
  };

  const blockSize = rv();
  const miniblocksPerBlock = rv();
  const firstValue = rz();

  results.push(firstValue);
  if (results.length >= maxValues) {
    return results;
  }

  const valuesPerMiniblock = blockSize / miniblocksPerBlock;
  let lastValue = firstValue;

  while (pos < buf.length && results.length < maxValues) {
    const minDelta = rz();
    const bitWidths: number[] = [];
    for (let m = 0; m < miniblocksPerBlock; m++) {
      bitWidths.push(buf[pos++]);
    }

    for (let m = 0; m < miniblocksPerBlock && results.length < maxValues; m++) {
      const bw = bitWidths[m];
      if (bw === 0) {
        for (let v = 0; v < valuesPerMiniblock && results.length < maxValues; v++) {
          lastValue += minDelta;
          results.push(lastValue);
        }
      } else {
        const bytesNeeded = Math.ceil((valuesPerMiniblock * bw) / 8);
        const end = Math.min(pos + bytesNeeded, buf.length);
        const mask = (1 << bw) - 1;
        let bitBuf = 0;
        let bitsLeft = 0;
        let decoded = 0;
        while (decoded < valuesPerMiniblock && results.length < maxValues) {
          while (bitsLeft < bw && pos < end) {
            bitBuf |= buf[pos++] << bitsLeft;
            bitsLeft += 8;
          }
          lastValue += (bitBuf & mask) + minDelta;
          bitBuf >>>= bw;
          bitsLeft -= bw;
          results.push(lastValue);
          decoded++;
        }
        pos = end;
      }
    }
  }
  return results.slice(0, maxValues);
}

// ── DELTA_LENGTH_BYTE_ARRAY ───────────────────────────────────────────────────

export function decodeDeltaLengthByteArray(buf: Uint8Array, offset: number, count: number): string[] {
  const lengths = decodeDeltaBinaryPacked(buf, offset, count);
  let pos = offset;

  const rv = (): number => {
    let r = 0,
      s = 0;
    while (pos < buf.length) {
      const b = buf[pos++];
      r |= (b & 0x7f) << s;
      if (!(b & 0x80)) {
        break;
      }
      s += 7;
    }
    return r >>> 0;
  };

  const blockSize = rv();
  const miniblocksPerBlock = rv();
  const totalValues = rv();
  rv(); // first_value
  const vpm = blockSize / miniblocksPerBlock;
  let decoded = 1;

  while (decoded < totalValues && pos < buf.length) {
    rv(); // min_delta
    for (let m = 0; m < miniblocksPerBlock; m++) {
      const bw = buf[pos++];
      pos += bw === 0 ? 0 : Math.ceil((vpm * bw) / 8);
      decoded += vpm;
    }
  }

  const results: string[] = [];
  const decoder = new TextDecoder();
  for (let i = 0; i < Math.min(count, lengths.length); i++) {
    const len = lengths[i];
    if (len < 0 || pos + len > buf.length) {
      break;
    }
    results.push(decoder.decode(buf.subarray(pos, pos + len)));
    pos += len;
  }
  return results;
}

// ── DELTA_BYTE_ARRAY ──────────────────────────────────────────────────────────

export function decodeDeltaByteArray(buf: Uint8Array, offset: number, count: number): string[] {
  const prefixLengths = decodeDeltaBinaryPacked(buf, offset, count);

  let pos = offset;
  const rv = (): number => {
    let r = 0,
      s = 0;
    while (pos < buf.length) {
      const b = buf[pos++];
      r |= (b & 0x7f) << s;
      if (!(b & 0x80)) {
        break;
      }
      s += 7;
    }
    return r >>> 0;
  };

  const blockSize = rv();
  const miniblocksPerBlock = rv();
  const totalValues = rv();
  rv(); // first_value
  const vpm = blockSize / miniblocksPerBlock;
  let decoded = 1;

  while (decoded < totalValues && pos < buf.length) {
    rv();
    for (let m = 0; m < miniblocksPerBlock; m++) {
      const bw = buf[pos++];
      pos += bw === 0 ? 0 : Math.ceil((vpm * bw) / 8);
      decoded += vpm;
    }
  }

  const suffixes = decodeDeltaLengthByteArray(buf, pos, count);
  const results: string[] = [];
  let prev = '';
  for (let i = 0; i < Math.min(count, suffixes.length); i++) {
    const current = prev.slice(0, prefixLengths[i] || 0) + (suffixes[i] || '');
    results.push(current);
    prev = current;
  }
  return results;
}

// ── findBitWidthInfo ──────────────────────────────────────────────────────────

export function findBitWidthInfo(data: Uint8Array, afterHdr: number, dictLen: number): BitWidthInfo {
  const minBits = Math.max(1, Math.ceil(Math.log2(Math.max(dictLen, 2))));
  const maxBits = minBits + 2;
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const tryAt = (start: number): BitWidthInfo | null => {
    const t = skipDefLevels(data, start);
    const bw = data[t];
    if (bw >= minBits && bw <= maxBits) {
      const lvl = start + 4 <= data.length ? view.getInt32(start, true) : 0;
      return { lvlEnd: t, bitWidth: bw, levelLen: lvl, effectiveStart: start };
    }
    const bwd = data[start];
    if (bwd >= minBits && bwd <= maxBits) {
      return { lvlEnd: start, bitWidth: bwd, levelLen: 0, effectiveStart: start };
    }
    return null;
  };

  const res = tryAt(afterHdr);
  if (res) {
    return res;
  }

  for (let scan = afterHdr + 1; scan < Math.min(afterHdr + 25, data.length - 1); scan++) {
    const res2 = tryAt(scan);
    if (res2) {
      return res2;
    }
  }

  const t1 = skipDefLevels(data, afterHdr);
  return { lvlEnd: t1, bitWidth: data[t1], levelLen: 0, effectiveStart: afterHdr };
}

// ── decodePage ────────────────────────────────────────────────────────────────

export function decodePage(
  data: Uint8Array,
  afterHdr: number,
  actualEncoding: string,
  type: ParquetTypeStr,
  dictionary: (string | number)[] | null,
  numValues: number,
  out: SupportedTypedArray | string[],
  outPos: number,
): number {
  const readPlainInto = plain.readPlainInto;
  let written = 0;

  // Type Guard para asegurar acceso a TypedArrays numéricos
  const isTypedArray = (a: unknown): a is SupportedTypedArray => {
    return a instanceof Int32Array || a instanceof Float64Array || a instanceof Float32Array || a instanceof Uint8Array;
  };
  // ── BOOLEAN ────────────────────────────────────────────────────────────────
  if (type === 'BOOLEAN') {
    const start = skipDefLevels(data, afterHdr);
    if (actualEncoding === 'BIT_PACKED') {
      const bits = decodeRLE(data, start, 1, numValues); // RLE con width 1 es BitPacking
      if (isTypedArray(out)) {
        for (let i = 0; i < bits.length && written < numValues; i++) {
          out[outPos + written++] = bits[i];
        }
      }
    } else {
      written = readPlainInto(data, start, 'BOOLEAN', numValues, out, outPos);
    }
    return written;
  }

  const isDictEnc = actualEncoding === 'RLE_DICTIONARY' || actualEncoding === 'PLAIN_DICTIONARY';

  // ── PLAIN / DELTA ──────────────────────────────────────────────────────────
  if (!isDictEnc) {
    const dataStart = skipDefLevels(data, afterHdr);

    if (actualEncoding === 'PLAIN') {
      written = readPlainInto(data, dataStart, type, numValues, out, outPos);
    } else if (actualEncoding === 'DELTA_BINARY_PACKED') {
      const vals = decodeDeltaBinaryPacked(data, dataStart, numValues);
      if (isTypedArray(out)) {
        for (let i = 0; i < vals.length && written < numValues; i++) {
          out[outPos + written++] = vals[i];
        }
      }
    } else if (actualEncoding === 'DELTA_LENGTH_BYTE_ARRAY' && Array.isArray(out)) {
      const vals = decodeDeltaLengthByteArray(data, dataStart, numValues);
      for (let i = 0; i < vals.length && written < numValues; i++) {
        out[outPos + written++] = vals[i];
      }
    } else if (actualEncoding === 'DELTA_BYTE_ARRAY' && Array.isArray(out)) {
      const vals = decodeDeltaByteArray(data, dataStart, numValues);
      for (let i = 0; i < vals.length && written < numValues; i++) {
        out[outPos + written++] = vals[i];
      }
    } else {
      // Fallback a Plain si el encoding no se reconoce o no coincide con el output
      written = readPlainInto(data, dataStart, type, numValues, out, outPos);
    }
    return written;
  }

  // ── DICTIONARY ─────────────────────────────────────────────────────────────
  if (!dictionary || dictionary.length === 0) {
    return 0;
  }

  const info = findBitWidthInfo(data, afterHdr, dictionary.length);
  const { bitWidth, lvlEnd, levelLen, effectiveStart } = info;
  const nullVal = nullValue(type);

  // Helper interno para escribir valores de forma tipada
  // FIX: acepta undefined para robustez (antes undefined no coincidía
  // con ninguna rama y written no se incrementaba, produciendo 0 filas).
  const writeValue = (val: string | number | null | undefined) => {
    if (Array.isArray(out)) {
      if (val === null || val === undefined) {
        out[outPos + written++] = 'null';
      } else {
        out[outPos + written++] = String(val);
      }
    } else if (val === null || val === undefined) {
      // null o undefined → centinela (NaN para float, 0 para int)
      out[outPos + written++] = (nullVal ?? 0);
    } else {
      out[outPos + written++] = val as number;
    }
  };

  let hasNulls = false;
  if (levelLen > 4) {
    const defStart0 = effectiveStart + 4;
    const defBuf0 = data.slice(defStart0, lvlEnd);
    const defSample = decodeRLE(defBuf0, 0, 1, 8);
    for (const val of defSample) {
      if (val === 0) {
        hasNulls = true;
        break;
      }
    }
  }

  if (hasNulls) {
    const defStart = effectiveStart + 4;
    const defBuf = data.slice(defStart, lvlEnd);
    const defLevels = decodeRLE(defBuf, 0, 1, numValues + 8);
    let nonNullCount = 0;
    for (const lvl of defLevels) {
      if (lvl === 1) {
        nonNullCount++;
      }
    }

    const valIndices = decodeRLE(data, lvlEnd + 1, bitWidth, nonNullCount + 4);
    let vPos = 0;
    for (let ii = 0; ii < defLevels.length && written < numValues; ii++) {
      if (defLevels[ii] === 0) {
        writeValue(nullVal);
      } else {
        const idx = valIndices[vPos++];
        const val = idx !== undefined && idx < dictionary.length ? dictionary[idx] : nullVal;
        writeValue(val);
      }
    }
  } else {
    const indices = decodeRLE(data, lvlEnd + 1, bitWidth, numValues);
    for (let i = 0; i < indices.length && written < numValues; i++) {
      const idx = indices[i];
      const val = idx !== undefined && idx < dictionary.length ? dictionary[idx] : nullVal;
      writeValue(val);
    }
  }

  return written;
}
