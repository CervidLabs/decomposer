// ── Thrift Compact Protocol Types ─────────────────────────────────────────────

enum CompactType {
  STOP = 0x00,
  BOOLEAN_TRUE = 0x01,
  BOOLEAN_FALSE = 0x02,
  BYTE = 0x03,
  I16 = 0x04,
  I32 = 0x05,
  I64 = 0x06,
  DOUBLE = 0x07,
  BINARY = 0x08,
  LIST = 0x09,
  SET = 0x0a,
  MAP = 0x0b,
  STRUCT = 0x0c,
}

// ── Parquet Mappings ─────────────────────────────────────────────────────────

type ParquetTypeStr = 'BOOLEAN' | 'INT32' | 'INT64' | 'INT96' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'FIXED_LEN_BYTE_ARRAY' | 'UNKNOWN';

const ParquetTypeMap: Record<number, ParquetTypeStr> = {
  0: 'BOOLEAN',
  1: 'INT32',
  2: 'INT64',
  3: 'INT96',
  4: 'FLOAT',
  5: 'DOUBLE',
  6: 'BYTE_ARRAY',
  7: 'FIXED_LEN_BYTE_ARRAY',
};

type EncodingStr =
  | 'PLAIN'
  | 'GROUP_VAR_INT'
  | 'PLAIN_DICTIONARY'
  | 'RLE'
  | 'BIT_PACKED'
  | 'DELTA_BINARY_PACKED'
  | 'DELTA_LENGTH_BYTE_ARRAY'
  | 'DELTA_BYTE_ARRAY'
  | 'RLE_DICTIONARY'
  | 'UNKNOWN';

const EncodingMap: Record<number, EncodingStr> = {
  0: 'PLAIN',
  1: 'GROUP_VAR_INT',
  2: 'PLAIN_DICTIONARY',
  3: 'RLE',
  4: 'BIT_PACKED',
  5: 'DELTA_BINARY_PACKED',
  6: 'DELTA_LENGTH_BYTE_ARRAY',
  7: 'DELTA_BYTE_ARRAY',
  8: 'RLE_DICTIONARY',
};

type CompressionStr = 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD';

const CompressionMap: Record<number, CompressionStr> = {
  0: 'UNCOMPRESSED',
  1: 'SNAPPY',
  2: 'GZIP',
  3: 'LZO',
  4: 'BROTLI',
  5: 'LZ4',
  6: 'ZSTD',
};

// ── Interfaces de Metadatos ──────────────────────────────────────────────────

interface SchemaElement {
  name: string;
  type: ParquetTypeStr | null;
  numChildren: number;
}

interface ColumnMetaData {
  type: ParquetTypeStr;
  encodings: EncodingStr[];
  pathInSchema: string[];
  compression: CompressionStr;
  numValues: number;
  totalUncompressedSize: number;
  totalCompressedSize: number;
  dataPageOffset: number;
  dictionaryPageOffset: number | null;
}

interface ColumnChunkParsed {
  name: string;
  type: ParquetTypeStr;
  compression: CompressionStr;
  encoding: EncodingStr;
  dataOffset: number;
  compressedSize: number;
  uncompressedSize: number;
  numValues: number;
  dictOffset: number | null;
}

interface RowGroup {
  columns: ColumnChunkParsed[];
  totalBytes: number;
  numRows: number;
}

export interface FileMetaData {
  version: number;
  schema: SchemaElement[];
  rowCount: number;
  rowGroups: RowGroup[];
}

// ── ThriftReader Class ───────────────────────────────────────────────────────

export class ThriftReader {
  private pos = 0;
  private readonly view: Uint8Array;

  constructor(buffer: Uint8Array | Buffer) {
    this.view = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  }

  // ── Primitivos Compact Protocol ──────────────────────────

  private readByte(): number {
    return this.view[this.pos++];
  }

  private readVarint(): number {
    let result = 0;
    let shift = 0;
    while (true) {
      const byte = this.view[this.pos++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        break;
      }
      shift += 7;
    }
    return result >>> 0;
  }

  private readZigZag32(): number {
    const n = this.readVarint();
    return (n >>> 1) ^ -(n & 1);
  }

  private readZigZag64(): number {
    let result = BigInt(0);
    let shift = BigInt(0);
    while (true) {
      const byte = BigInt(this.view[this.pos++]);
      result |= (byte & BigInt(0x7f)) << shift;
      if ((byte & BigInt(0x80)) === BigInt(0)) {
        break;
      }
      shift += BigInt(7);
    }
    const decoded = (result >> BigInt(1)) ^ -(result & BigInt(1));
    return Number(decoded);
  }

  private readDouble(): number {
    // Usamos DataView para leer doubles de forma portable sin dependencias de Buffer.readDoubleLE
    const dv = new DataView(this.view.buffer, this.view.byteOffset + this.pos, 8);
    this.pos += 8;
    return dv.getFloat64(0, true);
  }

  private readBinary(): string {
    const len = this.readVarint();
    const start = this.pos;
    this.pos += len;
    return new TextDecoder().decode(this.view.subarray(start, start + len));
  }

  // ── Manejo de Campos ─────────────────────────────────────

  private readFieldHeader(lastFieldId: number): { type: CompactType; id: number } {
    const byte = this.readByte(); // Cast aquí
    // CompactType.STOP es 0, así que si byte es 0, es el fin de la estructura.
    if (byte === 0) {
      // Siendo honestos, en un parser de bytes, 0 es 0.
      return { type: CompactType.STOP, id: -1 };
    }

    const delta = (byte >> 4) & 0x0f;
    const type = byte & 0x0f;
    const fieldId = delta === 0 ? this.readZigZag32() : lastFieldId + delta;

    return { type, id: fieldId };
  }

  public skipValue(type: CompactType): void {
    switch (type) {
      case CompactType.BOOLEAN_TRUE:
      case CompactType.BOOLEAN_FALSE:
        break;
      case CompactType.BYTE:
        this.pos += 1;
        break;
      case CompactType.I16:
      case CompactType.I32:
        this.readZigZag32();
        break;
      case CompactType.I64:
        this.readZigZag64();
        break;
      case CompactType.DOUBLE:
        this.pos += 8;
        break;
      case CompactType.BINARY:
        this.pos += this.readVarint();
        break;
      case CompactType.STRUCT:
        this.skipStruct();
        break;
      case CompactType.LIST:
      case CompactType.SET: {
        const header = this.readByte();
        const elemType = header & 0x0f;
        const count = header >> 4 === 0x0f ? this.readVarint() : (header >> 4) & 0x0f;
        for (let i = 0; i < count; i++) {
          this.skipValue(elemType);
        }
        break;
      }
      case CompactType.MAP: {
        const count = this.readVarint();
        if (count > 0) {
          const types = this.readByte();
          const keyType = (types >> 4) & 0x0f;
          const valType = types & 0x0f;
          for (let i = 0; i < count; i++) {
            this.skipValue(keyType);
            this.skipValue(valType);
          }
        }
        break;
      }
    }
  }

  private skipStruct(): void {
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      this.skipValue(type);
    }
  }

  // ── Helpers de Colecciones ───────────────────────────────

  private readList<T>(readItem: (type: CompactType) => T): T[] {
    const header = this.readByte();
    const elemType = header & 0x0f;
    const count = header >> 4 === 0x0f ? this.readVarint() : (header >> 4) & 0x0f;
    const items: T[] = [];
    for (let i = 0; i < count; i++) {
      items.push(readItem(elemType));
    }
    return items;
  }

  // ── Métodos de Lectura de Parquet ────────────────────────

  public readFileMetaData(): FileMetaData {
    const meta: FileMetaData = { version: 0, schema: [], rowCount: 0, rowGroups: [] };
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      switch (id) {
        case 1:
          meta.version = this.readZigZag32();
          break;
        case 2:
          meta.schema = this.readList(() => this.readSchemaElement());
          break;
        case 3:
          meta.rowCount = this.readZigZag64();
          break;
        case 4:
          meta.rowGroups = this.readList(() => this.readRowGroup());
          break;
        default:
          this.skipValue(type);
      }
    }
    return meta;
  }

  private readSchemaElement(): SchemaElement {
    const el: SchemaElement = { name: '', type: null, numChildren: 0 };
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      switch (id) {
        case 1:
          el.type = ParquetTypeMap[this.readZigZag32()] || 'UNKNOWN';
          break;
        case 4:
          el.name = this.readBinary();
          break;
        case 5:
          el.numChildren = this.readZigZag32();
          break;
        default:
          this.skipValue(type);
      }
    }
    return el;
  }

  private readRowGroup(): RowGroup {
    const rg: RowGroup = { columns: [], totalBytes: 0, numRows: 0 };
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      switch (id) {
        case 1:
          rg.columns = this.readList(() => this.readColumnChunk());
          break;
        case 2:
          rg.totalBytes = this.readZigZag64();
          break;
        case 3:
          rg.numRows = this.readZigZag64();
          break;
        default:
          this.skipValue(type);
      }
    }
    return rg;
  }

  private readColumnChunk(): ColumnChunkParsed {
    let fileOffset = 0;
    let meta: ColumnMetaData | null = null;
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      switch (id) {
        case 2:
          fileOffset = this.readZigZag64();
          break;
        case 3:
          meta = this.readColumnMetaData();
          break;
        default:
          this.skipValue(type);
      }
    }

    return {
      name: meta?.pathInSchema.join('.') ?? 'unknown',
      type: meta?.type ?? 'UNKNOWN',
      compression: meta?.compression ?? 'UNCOMPRESSED',
      encoding: meta?.encodings[0] ?? 'PLAIN',
      dataOffset: meta?.dataPageOffset ?? fileOffset,
      compressedSize: meta?.totalCompressedSize ?? 0,
      uncompressedSize: meta?.totalUncompressedSize ?? 0,
      numValues: meta?.numValues ?? 0,
      dictOffset: meta?.dictionaryPageOffset ?? null,
    };
  }

  private readColumnMetaData(): ColumnMetaData {
    const meta: ColumnMetaData = {
      type: 'UNKNOWN',
      encodings: [],
      pathInSchema: [],
      compression: 'UNCOMPRESSED',
      numValues: 0,
      totalUncompressedSize: 0,
      totalCompressedSize: 0,
      dataPageOffset: 0,
      dictionaryPageOffset: null,
    };
    let lastId = 0;
    while (true) {
      const { type, id } = this.readFieldHeader(lastId);
      if (type === CompactType.STOP) {
        break;
      }
      lastId = id;
      switch (id) {
        case 1:
          meta.type = ParquetTypeMap[this.readZigZag32()] || 'UNKNOWN';
          break;
        case 2:
          meta.encodings = this.readList(() => EncodingMap[this.readZigZag32()] || 'UNKNOWN');
          break;
        case 3:
          meta.pathInSchema = this.readList(() => this.readBinary());
          break;
        case 4:
          meta.compression = CompressionMap[this.readZigZag32()] || 'UNCOMPRESSED';
          break;
        case 5:
          meta.numValues = this.readZigZag64();
          break;
        case 6:
          meta.totalUncompressedSize = this.readZigZag64();
          break;
        case 7:
          meta.totalCompressedSize = this.readZigZag64();
          break;
        case 9:
          meta.dataPageOffset = this.readZigZag64();
          break;
        case 11:
          meta.dictionaryPageOffset = this.readZigZag64();
          break;
        default:
          this.skipValue(type);
      }
    }
    return meta;
  }
}
