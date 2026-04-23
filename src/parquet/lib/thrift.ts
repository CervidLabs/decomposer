// ── Parquet Constants ─────────────────────────────────────────────────────────

export const ENC_NAME: Record<number, string> = {
  0: 'PLAIN',
  2: 'PLAIN_DICTIONARY',
  3: 'RLE',
  4: 'BIT_PACKED',
  5: 'DELTA_BINARY_PACKED',
  6: 'DELTA_LENGTH_BYTE_ARRAY',
  7: 'DELTA_BYTE_ARRAY',
  8: 'RLE_DICTIONARY',
};

export interface PageHeaderResult {
  pageType: number;
  uncompressedSize: number;
  compressedSize: number;
  numValues: number;
  headerSize: number;
  dataEncoding: string | null;
}

// ── ThriftPage Class ──────────────────────────────────────────────────────────

export class ThriftPage {
  private pos = 0;
  private _dataEncoding: string | null = null;
  private readonly buf: Uint8Array;

  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  public get currentPos(): number {
    return this.pos;
  }

  private readVarint(): number {
    let r = 0;
    let s = 0;
    while (true) {
      const b = this.buf[this.pos++];
      r |= (b & 0x7f) << s;
      if ((b & 0x80) === 0) {
        break;
      }
      s += 7;
    }
    return r >>> 0;
  }

  private readZZ32(): number {
    const n = this.readVarint();
    return (n >>> 1) ^ -(n & 1);
  }

  private readZZ64(): number {
    let r = BigInt(0);
    let s = BigInt(0);
    while (true) {
      const b = BigInt(this.buf[this.pos++]);
      r |= (b & BigInt(0x7f)) << s;
      if ((b & BigInt(0x80)) === BigInt(0)) {
        break;
      }
      s += BigInt(7);
    }
    const decoded = (r >> BigInt(1)) ^ -(r & BigInt(1));
    return Number(decoded);
  }

  private skipBin(): void {
    this.pos += this.readVarint();
  }

  private skipStruct(): void {
    let lid = 0;
    while (true) {
      if (this.pos >= this.buf.length) {
        break;
      }
      const b = this.buf[this.pos];
      if (b === 0x00) {
        this.pos++;
        break;
      }
      this.pos++;
      const delta = (b >> 4) & 0xf;
      const type = b & 0xf;
      lid = delta === 0 ? this.readZZ32() : lid + delta;
      this.skipVal(type);
    }
  }

  private skipVal(t: number): void {
    if (t === 0x01 || t === 0x02) {
      return;
    } // BOOLEAN (in nibble)
    if (t === 0x03) {
      this.pos++;
      return;
    } // BYTE
    if (t === 0x04 || t === 0x05) {
      this.readZZ32();
      return;
    }
    if (t === 0x06) {
      this.readZZ64();
      return;
    }
    if (t === 0x07) {
      this.pos += 8;
      return;
    }
    if (t === 0x08) {
      this.skipBin();
      return;
    }
    if (t === 0x0c) {
      this.skipStruct();
      return;
    }
    if (t === 0x09 || t === 0x0a) {
      const h = this.buf[this.pos++];
      const et = h & 0xf;
      const c = h >> 4 === 0xf ? this.readVarint() : h >> 4;
      for (let i = 0; i < c; i++) {
        this.skipVal(et);
      }
    }
  }

  private readDataPageHdr(): number {
    let numValues = 0;
    let encoding = 0;
    let lid = 0;
    while (true) {
      if (this.pos >= this.buf.length) {
        break;
      }
      const b = this.buf[this.pos];
      if (b === 0x00) {
        this.pos++;
        break;
      }
      this.pos++;
      const delta = (b >> 4) & 0xf;
      const type = b & 0xf;
      lid = delta === 0 ? this.readZZ32() : lid + delta;

      if (lid === 1) {
        numValues = this.readZZ32();
      } else if (lid === 2) {
        encoding = this.readZZ32();
      } else if (lid === 3 || lid === 4) {
        this.readZZ32();
      } else {
        this.skipVal(type);
      }
    }
    this._dataEncoding = ENC_NAME[encoding] || 'PLAIN';
    return numValues;
  }

  private readDictPageHdr(): number {
    let numValues = 0;
    let lid = 0;
    while (true) {
      if (this.pos >= this.buf.length) {
        break;
      }
      const b = this.buf[this.pos];
      if (b === 0x00) {
        this.pos++;
        break;
      }
      this.pos++;
      const delta = (b >> 4) & 0xf;
      const type = b & 0xf;
      lid = delta === 0 ? this.readZZ32() : lid + delta;

      if (lid === 1) {
        numValues = this.readZZ32();
      } else if (lid === 2) {
        this.readZZ32();
      } else {
        this.skipVal(type);
      }
    }
    return numValues;
  }

  /**
   * Procesa la cabecera de una página Parquet.
   */
  public readPageHeader(): PageHeaderResult {
    const start = this.pos;
    const res: PageHeaderResult = {
      pageType: -1,
      compressedSize: 0,
      uncompressedSize: 0,
      numValues: 0,
      headerSize: 0,
      dataEncoding: null,
    };
    let lid = 0;

    while (true) {
      if (this.pos >= this.buf.length) {
        break;
      }
      const b = this.buf[this.pos];
      if (b === 0x00) {
        this.pos++;
        break;
      }
      this.pos++;
      const delta = (b >> 4) & 0xf;
      const type = b & 0xf;
      lid = delta === 0 ? this.readZZ32() : lid + delta;

      if (lid === 1) {
        res.pageType = this.readZZ32();
      } else if (lid === 2) {
        res.uncompressedSize = this.readZZ32();
      } else if (lid === 3) {
        res.compressedSize = this.readZZ32();
      } else if (lid === 4) {
        this.readZZ32();
      } // CRC opcional
      else if (lid === 5) {
        res.numValues = this.readDataPageHdr();
      } else if (lid === 7) {
        res.numValues = this.readDictPageHdr();
      } else {
        this.skipVal(type);
      }
    }

    res.headerSize = this.pos - start;
    res.dataEncoding = this._dataEncoding;
    return res;
  }
}
