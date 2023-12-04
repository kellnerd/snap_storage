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
  type WriteOptions,
  writeSnap,
} from "./snapshot.ts";

/** Options for caching of network resources. */
export interface CacheOptions {
  fetch?: typeof fetch;
  requestInit?: RequestInit;
  policy?: Policy;
}

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
  #createUriQuery: PreparedQuery<[number], RowObject, [string]>;
  #createSnapQuery: PreparedQuery<Row, RowObject, [number, number, string]>;
  #latestSnapQuery: PreparedQuery<
    [number, string],
    { timestamp: number; content_hash: string },
    [string]
  >;

  /**
   * Opens or creates a snapshot storage using the given data directory.
   *
   * If no directory is specified, the current working directory will be used
   * to create a `snaps.db` database file and a `snaps/` data folder.
   */
  constructor(readonly directory = ".") {
    this.#db = new DB(joinPath(directory, "snaps.db"));
    this.#db.execute(`CREATE TABLE IF NOT EXISTS uri (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      value TEXT UNIQUE
    )`);
    this.#db.execute(`CREATE TABLE IF NOT EXISTS snap (
      uri_id INTEGER REFERENCES uri ON DELETE CASCADE,
      timestamp INTEGER NOT NULL,
      content_hash CHAR(${hashLength}) NOT NULL
    )`);

    // We have to perform an "update" on conflicts, otherwise no ID is returned.
    this.#createUriQuery = this.#db.prepareQuery(
      `INSERT INTO uri (value) VALUES (?)
        ON CONFLICT DO UPDATE SET value = uri.value
        RETURNING id`,
    );
    this.#createSnapQuery = this.#db.prepareQuery(
      `INSERT INTO snap (uri_id, timestamp, content_hash) VALUES (?, ?, ?)`,
    );
    this.#latestSnapQuery = this.#db.prepareQuery(
      `SELECT timestamp, content_hash FROM snap
        JOIN uri ON snap.uri_id = uri.id
        WHERE uri.value = ?
        ORDER BY timestamp DESC
        LIMIT 1`,
    );
  }

  /**
   * Returns the latest snapshot of the given URL or fetches the resource from
   * the network and creates a new snapshot from the response body.
   */
  async cache(
    url: string | URL,
    { fetch = self.fetch, requestInit, policy = {} }: CacheOptions = {},
  ): Promise<Snapshot<Response>> {
    url = url.toString();
    let response: Response;
    let isFresh = false;
    let snap = this.getLatestSnap(url);

    if (snap && followsPolicy(snap, policy)) {
      const data = await Deno.readFile(snap.path);
      response = new Response(data);
    } else {
      response = await fetch(url, requestInit);
      if (response.ok && response.body) {
        // Response body can only be consumed once, so we need to create a copy.
        snap = await this.createSnap(url, response.clone().body!, {
          previousHash: snap?.contentHash,
        });
        isFresh = true;
      } else {
        throw new Error(`Failed to fetch resource at ${url}`);
      }
    }

    return { ...snap, content: response, isFresh };
  }

  /** Creates a new snapshot for the given URI and content. */
  async createSnap(
    uri: string,
    content: Content | string,
    options: WriteOptions = {},
  ): Promise<SnapMeta> {
    const snap = await writeSnap(this.directory, content, options);
    const [uriId] = this.#createUriQuery.first([uri])!;
    this.#createSnapQuery.execute([uriId, snap.timestamp, snap.contentHash]);

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
    try {
      return { ...snap, content: JSON.parse(data) };
    } catch (error) {
      throw new Error(`Snapshot does not contain valid JSON: ${error}`);
    }
  }

  /**
   * Closes the storage.
   * This must be called after usage to avoid leaking open file descriptors.
   */
  close() {
    this.#db.close(true);
  }
}
