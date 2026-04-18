// ─────────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────────

export interface DecomposerConfig {
  workers?: number;
  chunkSizeMB?: number;
  smallFileMB?: number;
}

// ─────────────────────────────────────────────
//  SOURCE INFO
// ─────────────────────────────────────────────

export interface DecomposerSource {
  format: string; // "npy", "parquet", etc.
  path: string;
}

// ─────────────────────────────────────────────
//  SCHEMA
// ─────────────────────────────────────────────

export interface DecomposerSchemaColumn {
  name: string;
  dtype: string;     // "<f4", "<i4", etc.
  offset: number;    // byte offset dentro del buffer
  length: number;    // número de elementos
}

// ─────────────────────────────────────────────
//  METADATA
// ─────────────────────────────────────────────

export interface DecomposerMetadata {
  fortranOrder?: boolean;
  originalShape?: number[];
  ndim?: number;
  elementSize?: number;
  [key: string]: unknown;
}

// ─────────────────────────────────────────────
//  RESULT
// ─────────────────────────────────────────────

export interface DecomposerResult {
  version: number;

  source: DecomposerSource;

  shape: number[];
  rowCount: number;
  columnCount: number;

  schema: DecomposerSchemaColumn[];

  buffer: SharedArrayBuffer;

  metadata: DecomposerMetadata;
}

// ─────────────────────────────────────────────
//  MAIN API
// ─────────────────────────────────────────────

export declare class Decomposer {
  /**
   * Punto de entrada principal.
   * Detecta el formato y retorna un payload compartido.
   */
  static read(
    filePath: string,
    config?: DecomposerConfig
  ): Promise<DecomposerResult>;
}

// ─────────────────────────────────────────────
//  ENGINE (USO AVANZADO)
// ─────────────────────────────────────────────

export declare class OctopusEngine {
  constructor(config?: DecomposerConfig);

  /**
   * Procesa directamente un archivo.
   */
  process(filePath: string): Promise<DecomposerResult>;

  /**
   * Libera workers.
   */
  destroy(): Promise<void>;
}