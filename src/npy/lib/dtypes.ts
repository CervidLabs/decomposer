/**
 * Definición de los dtypes soportados basados en la estructura de NumPy/Buffer
 */
export type Dtype = 'f4' | 'f8' | 'i1' | 'i2' | 'i4' | 'u1' | 'u2' | 'u4' | 'b1';

/**
 * Constructor de TypedArray para tipado estricto
 */
type TypedArrayConstructor =
  | Float32ArrayConstructor
  | Float64ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor;

export const DTYPE_MAP: Record<Dtype, TypedArrayConstructor> = {
  f4: Float32Array,
  f8: Float64Array,
  i1: Int8Array,
  i2: Int16Array,
  i4: Int32Array,
  u1: Uint8Array,
  u2: Uint16Array,
  u4: Uint32Array,
  b1: Uint8Array,
};

/**
 * Elimina caracteres de endianness o alineación (<, >, =, |)
 */
export function normalizeDtype(descr: string): string {
  return descr.replace(/[<>=|]/g, '');
}

/**
 * Retorna el constructor del TypedArray correspondiente.
 * Por defecto devuelve Uint8Array si no encuentra coincidencia.
 */
export function getTypedArray(descr: string): TypedArrayConstructor {
  const normalized = normalizeDtype(descr) as Dtype;
  return DTYPE_MAP[normalized] || Uint8Array;
}

/**
 * Obtiene el tamaño en bytes de un elemento según su descriptor
 */
export function getElementSize(descr: string): number {
  return getTypedArray(descr).BYTES_PER_ELEMENT;
}
