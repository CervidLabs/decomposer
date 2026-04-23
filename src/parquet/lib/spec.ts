// ── Types ─────────────────────────────────────────────────────────────────────

export type DecomposterDType = 'i4' | 'f4' | 'f8' | 'u1' | 'string' | 'unknown';

export interface ColumnSchema {
  name: string;
  dtype: DecomposterDType;
  [key: string]: unknown; // Para soportar 'extra'
}

export interface ArrayColumn {
  dtype: DecomposterDType;
  length: number;
  buffer: ArrayBufferLike;
  [key: string]: unknown;
}

export interface StringColumn {
  dtype: 'string';
  length: number;
  data: string[];
  [key: string]: unknown;
}

export interface TableResult {
  version: number;
  kind: 'table';
  source: { format: string; path: string };
  rowCount: number;
  columnCount: number;
  schema: ColumnSchema[];
  columns: Record<string, ArrayColumn | StringColumn>;
  metadata: Record<string, unknown>;
}

// ── Normalization & Mapping ──────────────────────────────────────────────────

/**
 * Normaliza el DType eliminando prefijos de endianness de NumPy/Parquet.
 */
export function normalizeDType(dtype: string | null | undefined): DecomposterDType {
  if (!dtype) {
    return 'unknown';
  }
  const clean = String(dtype).replace(/[<>=|]/g, '') as DecomposterDType;
  return clean;
}

/**
 * Mapea los tipos nativos de Parquet a la nomenclatura interna de Cervid.
 */
export function mapParquetType(type: string): DecomposterDType {
  let returnedType: DecomposterDType;
  switch (type) {
    case 'INT32':
      returnedType = 'i4';
      break;
    case 'INT64':
      returnedType = 'f8'; // Mapeado a f8 por precisión en JS
      break;
    case 'FLOAT':
      returnedType = 'f4';
      break;
    case 'DOUBLE':
      returnedType = 'f8';
      break;
    case 'BOOLEAN':
      returnedType = 'u1';
      break;
    case 'BYTE_ARRAY':
      returnedType = 'string';
      break;
    default:
      returnedType = 'unknown';
  }
  return returnedType;
}

// ── Factory Functions ────────────────────────────────────────────────────────

export function makeSource(format: string, filePath: string): { format: string; path: string } {
  return { format, path: filePath };
}

export function makeSchemaColumn(name: string, dtype: string, extra?: Record<string, unknown>): ColumnSchema {
  return {
    name,
    dtype: normalizeDType(dtype),
    ...extra,
  };
}

export function makeArrayColumn(dtype: string, buffer: ArrayBufferLike, length: number, extra?: Record<string, unknown>): ArrayColumn {
  return {
    dtype: normalizeDType(dtype),
    length,
    buffer,
    ...extra,
  };
}

export function makeStringColumn(data: string[], extra?: Record<string, unknown>): StringColumn {
  return {
    dtype: 'string',
    length: Array.isArray(data) ? data.length : 0,
    data,
    ...extra,
  };
}

export function createTableResult(
  filePath: string,
  rowCount: number,
  schema: ColumnSchema[],
  columns: Record<string, ArrayColumn | StringColumn>,
  metadata?: Record<string, unknown>,
): TableResult {
  return {
    version: 1,
    kind: 'table',
    source: makeSource('parquet', filePath),
    rowCount,
    columnCount: schema.length,
    schema,
    columns,
    metadata: metadata ?? {},
  };
}

// ── Shared Memory Utilities ──────────────────────────────────────────────────

/**
 * Convierte un TypedArray local en una columna basada en SharedArrayBuffer.
 * Esencial para el envío de datos entre Workers sin copia.
 */
export function toSharedTypedArrayColumn(dtype: string, typedArray: Int32Array | Float64Array | Float32Array | Uint8Array): ArrayColumn {
  const sab = new SharedArrayBuffer(typedArray.byteLength);

  // Usamos el constructor del TypedArray original para crear la vista sobre el SAB
  const ViewConstructor = typedArray.constructor as new (buf: SharedArrayBuffer) => typeof typedArray;
  const view = new ViewConstructor(sab);

  view.set(typedArray);

  return {
    dtype: normalizeDType(dtype),
    length: typedArray.length,
    buffer: sab,
  };
}
