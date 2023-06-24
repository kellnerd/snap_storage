import {
  DB,
  joinPath,
  type PreparedQuery,
  type Row,
  type RowObject,
} from "./deps.ts";
import {
  type Content,
  hash,
  hashLength,
  snapPath,
  type Snapshot,
} from "./snapshot.ts";

export class SnapStorage {
  #db: DB;
  #createSnapQuery: PreparedQuery<Row, RowObject, [string, number, string]>;
  #latestSnapQuery: PreparedQuery<
    [number, string],
    { timestamp: number; content_hash: string },
    [string]
  >;

  constructor(readonly directory = ".") {
    this.#db = new DB(joinPath(directory, "snaps.db"));
    this.#db.execute(`CREATE TABLE IF NOT EXISTS snaps (
      uri TEXT NOT NULL,
      timestamp INT NOT NULL,
      content_hash CHAR(${hashLength}) NOT NULL
    )`);

    this.#createSnapQuery = this.#db.prepareQuery(
      `INSERT INTO snaps (uri, timestamp, content_hash) VALUES (?, ?, ?)`,
    );
    this.#latestSnapQuery = this.#db.prepareQuery(
      `SELECT timestamp, content_hash FROM snaps
        WHERE uri = ?
        ORDER BY timestamp DESC
        LIMIT 1;`,
    );
  }

  async createSnap(uri: string, content: Content) {
    const timestamp = Date.now() / 1000;
    const contentHash = await hash(content);
    this.#createSnapQuery.execute([uri, timestamp, contentHash]);
  }

  getLatestSnap(uri: string): Snapshot | undefined {
    const snap = this.#latestSnapQuery.first([uri]);
    if (!snap) return;

    const [timestamp, contentHash] = snap;

    return {
      timestamp,
      contentHash,
      path: snapPath(this.directory, contentHash),
    };
  }
}
