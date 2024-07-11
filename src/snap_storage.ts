import {
  DB,
  joinPath,
  type PreparedQuery,
  type Row,
  type RowObject,
} from "./deps.ts";
import { ResponseError } from "./error.ts";
import {
  type Content,
  followsPolicy,
  hashLength,
  now,
  type Policy,
  type SnapMeta,
  snapPath,
  type Snapshot,
  type WriteOptions,
  writeSnap,
} from "./snapshot.ts";

/**
 * Function for modifying the Response object returned by `fetch`
 * before it is cached and returned to the caller.
 *
 * Care must be taken to ensure that the returned Promise resolves to a
 * Response with an unused body property.
 */
export type ResponseMutator = (response: Response) => Promise<Response>;

/** Options for caching of network resources. */
export interface CacheOptions {
  fetch?: typeof fetch;
  requestInit?: RequestInit;
  responseMutator?: ResponseMutator;
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
    [string, number]
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
    // Optimize lookups of latest snapshot by `uri_id`.
    this.#db.execute(`CREATE INDEX IF NOT EXISTS idx_snap_uri_id_timestamp
      ON snap (uri_id, timestamp DESC)`);

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
        WHERE uri.value = ? AND timestamp <= ?
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
    {
      fetch = self.fetch,
      requestInit,
      responseMutator,
      policy = {},
    }: CacheOptions = {},
  ): Promise<Snapshot<Response>> {
    url = url.toString();
    let response: Response;
    let isFresh = false;
    let snap = this.getLatestSnap(url, policy.maxTimestamp);

    if (snap && followsPolicy(snap, policy)) {
      const data = await Deno.readFile(snap.path);
      response = new Response(data);
    } else {
      response = await fetch(url, requestInit);
      if (responseMutator) {
        response = await responseMutator(response);
      }
      if (response.ok && response.body) {
        // Response body can only be consumed once, so we need to create a copy.
        snap = await this.createSnap(url, response.clone().body!, {
          previousHash: snap?.contentHash,
        });
        isFresh = true;
      } else {
        throw new ResponseError(`Failed to fetch resource at ${url}`, response);
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

  /**
   * Looks up the latest snapshot for the given URI (if one exists).
   *
   * If a maximum timestamp is given, the historically latest snapshot for that
   * time will be queried, otherwise the current time will be used as a limit.
   */
  getLatestSnap(uri: string, maxTimestamp?: number): SnapMeta | undefined {
    const snap = this.#latestSnapQuery.first([uri, maxTimestamp ?? now()]);
    if (!snap) return;

    const [timestamp, contentHash] = snap;

    return {
      timestamp,
      contentHash,
      path: snapPath(this.directory, contentHash),
    };
  }

  /** Looks up a snapshot for the given URI which follows the given policy. */
  getSnap(uri: string, policy: Policy = {}): SnapMeta | undefined {
    const snap = this.getLatestSnap(uri, policy.maxTimestamp);
    if (!snap || !followsPolicy(snap, policy)) return;

    return snap;
  }

  /**
   * Loads a snapshot for the given URI and its content as JSON.
   *
   * An optional policy can be specified to filter snapshots.
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
