import { NPYEngine, type NPYProcessResult } from './npy/core/NPYEngine.js';
import { NPZEngine, type NPZProcessResult } from './npz/core/NPZEngine.js';
import { type TableResult } from './parquet/lib/spec.js';
import { ParquetReader, type ParquetInspectionResult, type ParquetReadOptions } from './parquet/ParquetReader.js';

/**
 * Opciones de configuración para los motores de procesamiento
 */
export interface DecomposerConfig extends ParquetReadOptions {
  workers?: number;
  chunkSizeMB?: number;
  smallFileMB?: number;
  format?: string;
  [key: string]: unknown;
}

export type DecomposerReadResult = NPYProcessResult | NPZProcessResult | TableResult | ParquetInspectionResult;

export const Decomposer = {
  /**
   * Lee y descompone archivos binarios (NPY/NPZ/Parquet).
   * @param filePath Ruta al archivo.
   * @param config Configuración opcional para el procesamiento.
   */
  async read(filePath: string, config: DecomposerConfig = {}): Promise<DecomposerReadResult> {
    const ext = filePath.split('.').pop()?.toLowerCase();

    switch (ext) {
      case 'npy': {
        const engine = new NPYEngine(config);
        try {
          return await engine.process(filePath);
        } finally {
          await engine.destroy();
        }
      }

      case 'npz': {
        const engine = new NPZEngine(config);
        return await engine.process(filePath);
      }

      case 'parquet': {
        const parquetOptions: ParquetReadOptions = {};

        if (config.sample !== undefined) {
          parquetOptions.sample = config.sample;
        }
        if (config.columns !== undefined) {
          parquetOptions.columns = config.columns;
        }
        if (config.inspect !== undefined) {
          parquetOptions.inspect = config.inspect;
        }

        return ParquetReader.convert(filePath, parquetOptions);
      }

      default:
        throw new Error(`Unsupported format: ${ext ?? 'unknown'}`);
    }
  },
};

export { NPYEngine, NPZEngine };
