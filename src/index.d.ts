/**
 * @cervid/decomposer - Definiciones de tipos globales
 */

// --- DTYPES & HELPERS ---
export type Dtype = 'f4' | 'f8' | 'i1' | 'i2' | 'i4' | 'u1' | 'u2' | 'u4' | 'b1';

export type TypedArrayConstructor =
  | Float32ArrayConstructor
  | Float64ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor;

// --- NPY PARSER ---
export interface NPYMetadata {
  descr: string;
  shape: number[];
  fortran_order?: boolean;
}

export interface NPYHeader {
  metadata: NPYMetadata;
  dataOffset: number;
}

// --- ENGINE RESULTS ---
export interface ColumnSchema {
  name: string;
  dtype: string;
  offset: number;
  length: number;
}

export interface NPYProcessResult {
  version: number;
  source: {
    format: 'npy';
    path: string;
  };
  shape: number[];
  rowCount: number;
  columnCount: number;
  schema: ColumnSchema[];
  buffer: SharedArrayBuffer;
  metadata: {
    fortranOrder: boolean;
    originalShape: number[];
    ndim: number;
    elementSize: number;
  };
}

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

// --- CONFIGURATIONS ---
export interface DecomposerConfig {
  workers?: number;
  chunkSizeMB?: number;
  smallFileMB?: number;
  [key: string]: unknown;
}

// --- CLASSES ---
export { NPYHeaderParser } from './npy/core/NPYHeaderParser.js';
export { NPYEngine } from './npy/core/NPYEngine.js';
export { NPZEngine } from './npz/core/NPZEngine.js';
export { WorkerPool } from './npy/core/WorkerPool.js'; // Asumiendo esta ruta
export { IOScheduler } from './npy/core/IOScheduler.js';
export { ChunkReader } from './npy/core//ChunkReader.js';

export class Decomposer {
  /**
   * Lee y descompone archivos NPY/NPZ.
   */
  static read(filePath: string, config?: DecomposerConfig): Promise<NPYProcessResult | NPZProcessResult>;
}
