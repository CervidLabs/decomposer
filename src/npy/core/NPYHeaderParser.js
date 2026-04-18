export class NPYHeaderParser {
  constructor(buffer) {
    this.buffer = buffer;
  }

  parse() {
    if (this.buffer[0] !== 0x93) throw new Error("Invalid NPY");

    const major = this.buffer[6];

    let headerLen, offset;

    if (major === 1) {
      headerLen = this.buffer.readUInt16LE(8);
      offset = 10;
    } else {
      headerLen = this.buffer.readUInt32LE(8);
      offset = 12;
    }

    const headerStr = this.buffer.toString("utf8", offset, offset + headerLen);

    const descr = headerStr.match(/'descr':\s*'([^']+)'/)[1];

    const shape = headerStr
      .match(/\(([^)]*)\)/)[1]
      .split(",")
      .map(x => parseInt(x))
      .filter(Boolean);

    return {
      metadata: { descr, shape },
      dataOffset: offset + headerLen
    };
  }
}