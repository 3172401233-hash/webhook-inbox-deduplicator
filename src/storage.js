import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * 存储接口：
 *   load() => object | null
 *   save(state) => void   （同步、原子）
 */
export class MemoryStorage {
  constructor() {
    this.state = null;
  }

  load() {
    return this.state ? structuredClone(this.state) : null;
  }

  save(state) {
    this.state = structuredClone(state);
  }
}

/**
 * 文件存储：写临时文件后 rename，保证 checkpoint 落盘是原子的，
 * 崩溃时不会留下半个 JSON。
 */
export class FileStorage {
  constructor(filePath) {
    this.filePath = filePath;
    mkdirSync(dirname(filePath), { recursive: true });
  }

  load() {
    try {
      return JSON.parse(readFileSync(this.filePath, 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  save(state) {
    const tmp = `${this.filePath}.tmp`;
    writeFileSync(tmp, JSON.stringify(state));
    renameSync(tmp, this.filePath);
  }
}
