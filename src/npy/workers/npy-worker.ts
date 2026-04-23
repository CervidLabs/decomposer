import { parentPort } from 'worker_threads';
import { getTypedArray } from '../lib/dtypes.js';

/**
 * Interfaz que define la estructura del mensaje recibido.
 * Usamos Uint8Array para asegurar el acceso a las propiedades del buffer.
 */
interface NPYWorkerMessage {
  buffer: Uint8Array;
  dtype: string;
}

/**
 * Interfaz para el mensaje de respuesta.
 */
interface NPYWorkerResponse {
  sum: number;
  count: number;
}

if (!parentPort) {
  throw new Error('Este script debe ejecutarse dentro de un Worker Thread');
}

parentPort.on('message', (data: NPYWorkerMessage) => {
  const { buffer, dtype } = data;

  // Obtenemos el constructor del TypedArray correspondiente (Float64Array, Int32Array, etc.)
  const TA = getTypedArray(dtype);

  /**
   * Creamos la vista sobre el buffer compartido.
   * Al estar tipado como Uint8Array, buffer.buffer es el ArrayBuffer/SharedArrayBuffer subyacente.
   */
  const arr = new TA(
    // Forzamos el casting a ArrayBuffer para satisfacer al constructor
    buffer.buffer as ArrayBuffer,
    buffer.byteOffset,
    buffer.byteLength / TA.BYTES_PER_ELEMENT,
  );

  let sum = 0;
  let i = 0;
  const len = arr.length;

  /**
   * 🔥 LOOP UNROLLING (Factor 8)
   * Técnica para reducir los saltos de iteración y favorecer que el compilador JIT
   * de V8 realice optimizaciones de vectorización.
   */
  for (; i <= len - 8; i += 8) {
    sum += arr[i] + arr[i + 1] + arr[i + 2] + arr[i + 3] + arr[i + 4] + arr[i + 5] + arr[i + 6] + arr[i + 7];
  }

  // Procesamiento de los elementos restantes (limpieza del tail)
  for (; i < len; i++) {
    sum += arr[i];
  }

  // Envío de resultados al hilo principal
  const response: NPYWorkerResponse = {
    sum,
    count: len,
  };

  parentPort!.postMessage(response);
});
