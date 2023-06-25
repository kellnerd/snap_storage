import {
  DB,
  joinPath,
  type PreparedQuery,
  type Row,
  type RowObject,
} from "./deps.ts";
import {
  type Content,
  followsPolicy,
  hashLength,
  type Policy,
  type SnapMeta,
  snapPath,
  type Snapshot,
  writeSnap,
} from "./snapshot.ts";

/**
 * Provides a persistent storage mechanism for multiple snapshots of the content
 * which is referenced by an URI (i.e. an URL or another unique ID).
 *
 * Each snapshot is identified by the URI and a timestamp.
 * Identical snapshots do not waste additional storage space because the content
 * will be deduplicated automatically using a hash function.
 */
export class SnapStorage {
  #db: DB;
  #createSnapQuery: PreparedQuery<Row, RowObject, [string, number, string]>;
  #latestSnapQuery: PreparedQuery<
    [number, string],
    { timestamp: number; content_hash: string },
    [string]
  >;

  /**
   * Opens or creates a snapshot storage using the given data directory.
   *
   * If no directory is specified, a `data` folder inside the current working
   * directory will be used.
   */
  constructor(readonly directory = "data") {
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

  /** Creates a new snapshot for the given URI and content. */
  async createSnap(uri: string, content: Content | string): Promise<SnapMeta> {
    const snap = await writeSnap(this.directory, content);
    this.#createSnapQuery.execute([uri, snap.timestamp, snap.contentHash]);

    return snap;
  }

  /** Returns the latest snapshot for the given URI (if one exists). */
  getLatestSnap(uri: string): SnapMeta | undefined {
    const snap = this.#latestSnapQuery.first([uri]);
    if (!snap) return;

    const [timestamp, contentHash] = snap;

    return {
      timestamp,
      contentHash,
      path: snapPath(this.directory, contentHash),
    };
  }

  /**
   * Returns the latest snapshot for the given URI.
   *
   * An optional policy can be specified to discard certain snapshots.
   */
  getSnap(uri: string, policy: Policy = {}): SnapMeta | undefined {
    const snap = this.getLatestSnap(uri);
    if (!snap || !followsPolicy(snap, policy)) return;

    return snap;
  }

  /**
   * Loads the latest snapshot for the given URI and its content as JSON.
   *
   * An optional policy can be specified to discard certain snapshots.
   *
   * Throws if there is no matching snapshot or if it contains no valid JSON.
   */
  async loadJSON<T>(uri: string, policy: Policy = {}): Promise<Snapshot<T>> {
    const snap = this.getSnap(uri, policy);
    if (!snap) throw new Error(`No matching snapshot found for '${uri}'`);

    const data = await Deno.readTextFile(snap.path);
    let content: T;
    try {
      content = JSON.parse(data);
    } catch (error) {
      throw new Error(`Snapshot does not contain valid JSON: ${error}`);
    }

    return { ...snap, content };
  }

  /**
   * Closes the storage.
   * This must be called after usage to avoid leaking open file descriptors.
   */
  close() {
    this.#db.close(true);
  }
}
