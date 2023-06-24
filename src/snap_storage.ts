import { DB, joinPath } from "./deps.ts";
import {
  hash,
  hashLength,
  snapPath,
  type Content,
  type Snapshot,
} from "./snapshot.ts";

export class SnapStorage {
  #db: DB;

  constructor(readonly directory = ".") {
    this.#db = new DB(joinPath(directory, "snaps.db"));
    this.#db.execute(`CREATE TABLE IF NOT EXISTS snaps (
      uri TEXT NOT NULL,
      timestamp INT NOT NULL,
      content_hash CHAR(${hashLength}) NOT NULL
    )`);
  }

  async createSnap(uri: string, content: Content) {
    const timestamp = Date.now() / 1000;
    const contentHash = await hash(content);
    this.#db.query(
      `INSERT INTO snaps (uri, timestamp, content_hash) VALUES (?, ?, ?)`,
      [uri, timestamp, contentHash],
    );
  }

  getLatestSnap(uri: string): Snapshot {
    const [timestamp, contentHash] = this.#db.query<[number, string]>(
      `SELECT timestamp, content_hash FROM snaps
        WHERE uri = ?
        ORDER BY timestamp DESC
        LIMIT 1;`,
      [uri],
    )?.[0];

    return {
      timestamp,
      contentHash,
      path: snapPath(this.directory, contentHash),
    };
  }
}
