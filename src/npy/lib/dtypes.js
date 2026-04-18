export const DTYPE_MAP = {
  f4: Float32Array,
  f8: Float64Array,
  i1: Int8Array,
  i2: Int16Array,
  i4: Int32Array,
  u1: Uint8Array,
  u2: Uint16Array,
  u4: Uint32Array,
  b1: Uint8Array
};

export function normalizeDtype(descr) {
  return descr.replace(/[<>=|]/g, '');
}

export function getTypedArray(descr) {
  return DTYPE_MAP[normalizeDtype(descr)] || Uint8Array;
}

export function getElementSize(descr) {
  return getTypedArray(descr).BYTES_PER_ELEMENT;
}