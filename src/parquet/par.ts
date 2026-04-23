// ════════════════════════════════════════════════════════════════════════════
// @cervid/decomposer — parquet/ParquetReader.ts
// ════════════════════════════════════════════════════════════════════════════

import { Worker } from 'worker_threads';
import { fileURLToPath } from 'url';
import path from 'path';

import { CervidNavigator } from './core/CervidNavigator.js';
import * as spec from './lib/spec.js';
import type { DecomposterDType, ColumnSchema, ArrayColumn, StringColumn, TableResult } from './lib/spec.js';

// Configuración de rutas para ESModules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WORKER_PATH = path.join(__dirname, 'workers', 'extractor.worker.js');

// ── Interfaces públicas ─────────────────────────────────────────────────────

export interface ParquetReadOptions {
  sample?: boolean;
  columns?: string[];
  inspect?: boolean;
}

export interface ParquetInspectionColumnSummary {
  name: string;
  dtype: DecomposterDType;
  length: number;
  sample: string[];
  parquetType?: string;
  encoding?: string;
  compression?: string;
}

export interface ParquetInspectionResult {
  kind: 'parquet_inspection';
  filePath: string;
  metadata: {
    rowCount: number;
    rowGroups: number;
    schema: ColumnSchema[];
  };
  table: TableResult;
  summary: ParquetInspectionColumnSummary[];
}

// ── Interfaces internas ─────────────────────────────────────────────────────

interface WorkerMessage {
  id: string;
  status: 'DONE' | 'ERROR';
  data: Int32Array | Float64Array | Float32Array | Uint8Array | string[];
  type: string;
  rowCount: number;
}

interface RowGroup {
  numRows: number;
  columns: Array<{
    name: string;
    dataOffset: number;
    compressedSize: number;
    compression: string;
    encoding: string;
    type: string;
    dictOffset?: number;
  }>;
}

interface ParquetSchemaMeta {
  parquetType?: string;
  encoding?: string;
  compression?: string;
}

// ── Helpers de normalización ────────────────────────────────────────────────

function normalizeColumnToShared(
  col: Int32Array | Float64Array | Float32Array | Uint8Array | string[],
  dtype: DecomposterDType,
): ArrayColumn | StringColumn {
  if (!Array.isArray(col)) {
    return spec.toSharedTypedArrayColumn(dtype, col);
  }
  return spec.makeStringColumn(col);
}

function buildSharedColumns(
  columns: Record<string, Int32Array | Float64Array | Float32Array | Uint8Array | string[]>,
  fullSchema: ColumnSchema[],
): Record<string, ArrayColumn | StringColumn> {
  const out: Record<string, ArrayColumn | StringColumn> = {};

  for (const s of fullSchema) {
    const col = columns[s.name];
    if (col === null) {continue;}
    out[s.name] = normalizeColumnToShared(col, s.dtype);
  }

  return out;
}

function formatValue(v: unknown): string {
  // 1. Filtrar nulos y undefined
  if (v === null || v === undefined) {
    return 'null';
  }

  // 2. Manejar números (incluyendo NaN e Infinity)
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) {return 'NaN';}
    return v.toString();
  }

  // 3. Manejar BigInt (común en columnas INT64 de Parquet)
  if (typeof v === 'bigint') {
    return v.toString();
  }

  // 4. Manejar Booleanos
  if (typeof v === 'boolean') {
    return v ? 'true' : 'false';
  }

  // 5. Manejar Strings (evita procesar objetos si ya es string)
  if (typeof v === 'string') {
    return v;
  }

  /**
   * 6. Caso final para objetos/arrays:
   * Si llegamos aquí, v es un objeto. Usamos JSON.stringify para evitar 
   * el error de '@typescript-eslint/no-base-to-string'.
   */
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v);
    } catch {
      return '[Complex Object]';
    }
  }

  // Fallback seguro para cualquier otro tipo primitivo raro (como Symbols)
  return JSON.stringify(v);
}
// ── Orquestación de workers ────────────────────────────────────────────────

async function readRowGroup(rowGroup: RowGroup, fileBuffer: ArrayBufferLike, options: { sample?: boolean }): Promise<WorkerMessage[]> {
  const sampleOnly = !!options.sample;

  const promises = rowGroup.columns.map(
    async (col) =>
      new Promise<WorkerMessage>((resolve, reject) => {
        const worker = new Worker(WORKER_PATH);

        worker.postMessage({
          id: col.name,
          fileBuffer,
          offset: col.dataOffset,
          length: col.compressedSize,
          compression: col.compression,
          encoding: col.encoding,
          type: col.type,
          numRows: rowGroup.numRows,
          dictOffset: col.dictOffset,
          sampleOnly,
        });

        worker.on('message', (msg: WorkerMessage) => {
          if (msg.status === 'DONE') {
            void worker.terminate();
            resolve(msg);
          } else {
            void worker.terminate().catch(() => {
              // cleanup error intentionally ignored
            });
            reject(new Error(`Worker failed for column ${col.name}`));
          }
        });

        worker.on('error', (err) => {
          void worker.terminate().catch(() => {
            // cleanup error intentionally ignored
          });
          reject(new Error(`[${col.name}] ${err.message}`));
        });
      }),
  );

  return Promise.all(promises);
}

// ── Fusión de row groups ────────────────────────────────────────────────────

type DecomposerColumnData = Int32Array | Float64Array | Float32Array | Uint8Array | string[];

type SupportedTypedArray = Int32Array | Float64Array | Float32Array | Uint8Array;

interface TypedArrayConstructor {
  new (length: number): SupportedTypedArray;
}

function mergeRowGroups(allResults: WorkerMessage[][]): Record<string, DecomposerColumnData> {
  const columns: Record<string, DecomposerColumnData> = {};

  allResults.forEach((rgResults) => {
    rgResults.forEach((msg) => {
      const prev = columns[msg.id];
      const next = msg.data;

      if (!prev) {
        columns[msg.id] = next;
        return;
      }

      if (Array.isArray(prev) && Array.isArray(next)) {
        columns[msg.id] = prev.concat(next);
      } else if (!Array.isArray(prev) && !Array.isArray(next)) {
        const Constructor = prev.constructor as unknown as TypedArrayConstructor;
        const merged = new Constructor(prev.length + next.length);
        merged.set(prev, 0);
        merged.set(next, prev.length);
        columns[msg.id] = merged;
      }
    });
  });

  return columns;
}

// ── ParquetReader ───────────────────────────────────────────────────────────

export const ParquetReader = {
  /**
   * Convierte un archivo Parquet a un resultado columnar tipado.
   * Si inspect=true, además devuelve metadata y resumen tipo scanner.
   */
  async convert(filePath: string, options: ParquetReadOptions = {}): Promise<TableResult | ParquetInspectionResult> {
    // 1. Escaneo de footer
    const nav = new CervidNavigator(filePath);
    nav.scanFooter();

    const metadata = nav.metadata;
    if (!metadata) {
      throw new Error('Failed to read Parquet metadata');
    }

    let schemaElements = metadata.schema.slice(1); // saltar raíz
    let rowGroups = metadata.rowGroups as RowGroup[];
    const rowCount = metadata.rowCount;

    // 2. Proyección de columnas
    if (options.columns && options.columns.length > 0) {
      const selected = new Set(options.columns.map((c) => c.toLowerCase()));

      rowGroups = rowGroups.map((rg) => ({
        ...rg,
        columns: rg.columns.filter((col) => selected.has(col.name.toLowerCase())),
      }));

      schemaElements = schemaElements.filter((s) => selected.has(s.name.toLowerCase()));
    }

    // 3. Procesamiento por row group
    const allResults: WorkerMessage[][] = [];
    for (const rg of rowGroups) {
      const rgResults = await readRowGroup(rg, nav.fileBuffer!, options);
      allResults.push(rgResults);
    }

    // 4. Merge de datos
    const columns =
      allResults.length === 1
        ? allResults[0].reduce(
            (acc, msg) => {
              acc[msg.id] = msg.data;
              return acc;
            },
            {} as Record<string, Int32Array | Float64Array | Float32Array | Uint8Array | string[]>,
          )
        : mergeRowGroups(allResults);

    // 5. Construcción de esquema final
    const firstRGCols = metadata.rowGroups[0]?.columns ?? [];

    const outSchema = schemaElements.map((s) => {
      const rgInfo = firstRGCols.find((c) => c.name === s.name) ?? {
        encoding: 'PLAIN',
        compression: 'UNCOMPRESSED',
      };

      const dtype = spec.mapParquetType(s.type ?? 'UNKNOWN');

      return spec.makeSchemaColumn(s.name, dtype, {
        parquetType: s.type,
        encoding: rgInfo.encoding,
        compression: rgInfo.compression,
      });
    });

    const outColumns = buildSharedColumns(columns, outSchema);

    const table = spec.createTableResult(filePath, rowCount, outSchema, outColumns, { rowGroups: rowGroups.length });

    if (!options.inspect) {
      return table;
    }

    const summary: ParquetInspectionColumnSummary[] = outSchema.map((schemaCol) => {
      const meta = (schemaCol.meta ?? {}) as ParquetSchemaMeta;

      const rawData =
        columns[schemaCol.name] ??
        (outColumns[schemaCol.name]
          ? (outColumns[schemaCol.name].data as Int32Array | Float64Array | Float32Array | Uint8Array | string[] | undefined)
          : undefined);

      const result: ParquetInspectionColumnSummary = {
        name: schemaCol.name,
        dtype: schemaCol.dtype,
        length: 0,
        sample: [],
      };

      if (meta.parquetType !== undefined) {
        result.parquetType = meta.parquetType;
      }
      if (meta.encoding !== undefined) {
        result.encoding = meta.encoding;
      }
      if (meta.compression !== undefined) {
        result.compression = meta.compression;
      }

      if (rawData === null) {
        result.sample = ['[missing column data]'];
        return result;
      }

      result.length = rawData.length;

      result.sample = Array.isArray(rawData)
        ? rawData.slice(0, 5).map((v) => formatValue(v))
        : Array.from(rawData.slice(0, 5)).map((v) => formatValue(v));

      return result;
    });

    return {
      kind: 'parquet_inspection',
      filePath,
      metadata: {
        rowCount,
        rowGroups: rowGroups.length,
        schema: outSchema,
      },
      table,
      summary,
    };
  },
};

export default ParquetReader;
