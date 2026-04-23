/**
 * Estructura de los metadatos extraídos del header NPY
 */
export interface NPYMetadata {
  descr: string;
  shape: number[];
}

/**
 * Resultado del parseo del header
 */
export interface NPYHeader {
  metadata: NPYMetadata;
  dataOffset: number;
}

export class NPYHeaderParser {
  private readonly buffer: Buffer;

  constructor(buffer: Buffer) {
    this.buffer = buffer;
  }

  /**
   * Parse el header de un archivo .npy según la especificación de NumPy
   */
  public parse(): NPYHeader {
    // El número mágico es \x93NUMPY
    if (this.buffer[0] !== 0x93) {
      throw new Error('Invalid NPY: Magic number mismatch');
    }

    const major = this.buffer[6];
    let headerLen: number;
    let offset: number;

    // Versión 1.0 usa 2 bytes para la longitud, 2.0+ usa 4 bytes
    if (major === 1) {
      headerLen = this.buffer.readUInt16LE(8);
      offset = 10;
    } else {
      headerLen = this.buffer.readUInt32LE(8);
      offset = 12;
    }

    const headerStr = this.buffer.toString('utf8', offset, offset + headerLen);

    // Extraer descriptor de tipo de dato
    const descrMatch = headerStr.match(/'descr':\s*'([^']+)'/);
    if (!descrMatch) {
      throw new Error("Could not parse 'descr' from NPY header");
    }
    const descr = descrMatch[1];

    // Extraer shape (dimensiones)
    const shapeMatch = headerStr.match(/\(([^)]*)\)/);
    if (!shapeMatch) {
      throw new Error("Could not parse 'shape' from NPY header");
    }

    const shape = shapeMatch[1]
      .split(',')
      .map((x) => parseInt(x.trim()))
      .filter((x) => !isNaN(x));

    return {
      metadata: { descr, shape },
      dataOffset: offset + headerLen,
    };
  }
}
