import { parentPort } from "worker_threads";
import { getTypedArray } from "../lib/dtypes.js";

parentPort.on("message", ({ buffer, dtype }) => {
  const TA = getTypedArray(dtype);

  const arr = new TA(
    buffer.buffer,
    buffer.byteOffset,
    buffer.byteLength / TA.BYTES_PER_ELEMENT
  );

  let sum = 0;
  let i = 0;
  const len = arr.length;

  // 🔥 UNROLL x8 (clave para vectorización)
  for (; i < len - 8; i += 8) {
    sum += arr[i] +
           arr[i+1] +
           arr[i+2] +
           arr[i+3] +
           arr[i+4] +
           arr[i+5] +
           arr[i+6] +
           arr[i+7];
  }

  // resto
  for (; i < len; i++) {
    sum += arr[i];
  }

parentPort.postMessage({
  sum,
  count: arr.length
});
});