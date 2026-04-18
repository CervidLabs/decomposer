import {CervidEngine } from "./npy/core/CervidEngine.js";

export class Decomposer {
  /**
   * Punto de entrada principal
   */
  static async read(filePath, config = {}) {
    const ext = filePath.split(".").pop().toLowerCase();

    switch (ext) {
      case "npy": {
        const engine = new CervidEngine(config);
        const result = await engine.process(filePath);
        await engine.destroy();
        return result;
      }

      default:
        throw new Error(`Unsupported format: ${ext}`);
    }
  }
}

// Opcional: power users
export { CervidEngine };