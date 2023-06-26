import { assert, crypto, dirname, joinPath, toHashString } from "./deps.ts";

export type Content = Uint8Array | ReadableStream<Uint8Array>;

/** Policy which is used to filter snapshots. */
export interface Policy {
  /** Maximum age of the snapshot in seconds. */
  maxAge?: number;
}

/** Metadata of a snapshot for an URI. */
export interface SnapMeta {
  /** Creation date and time in seconds since the UNIX epoch. */
  timestamp: number;
  /** Hash of the content. Used to determine the path. */
  contentHash: string;
  /** Path to the content. */
  path: string;
}

/** Metadata and content of a snapshot for an URI. */
export interface Snapshot<T> extends SnapMeta {
  content: T;
}

/** Options for creating snapshots. */
export interface WriteOptions {
  /**
   * Hash of a previous snapshot's content. Used to prevent unnecessary
   * overwriting of a snapshot file with the same content.
   */
  previousHash?: string;
}

/** Validates whether the given snapshot follows the given policy. */
export function followsPolicy(snap: SnapMeta, policy: Policy) {
  if (policy.maxAge) {
    return now() < snap.timestamp + policy.maxAge;
  }
  return true;
}

/** Calculates a hash over the given content. */
export async function hash(content: Content): Promise<string> {
  const contentHash = await crypto.subtle.digest("SHA-256", content);
  return toHashString(contentHash);
}

export const hashLength = 64;

/** Determines the path to a snapshot based on the hash of its content. */
export function snapPath(basePath: string, contentHash: string): string {
  assert(contentHash.length === hashLength);
  const dirNameLength = 2;

  return joinPath(
    basePath,
    "snaps",
    contentHash.slice(0, dirNameLength),
    contentHash.slice(dirNameLength),
  );
}

/** Writes a new snapshot with the given content and returns its metadata. */
export async function writeSnap(
  basePath: string,
  content: Content | string,
  options: WriteOptions = {},
): Promise<SnapMeta> {
  if (typeof content === "string") {
    textEncoder ??= new TextEncoder();
    content = textEncoder.encode(content);
  }

  // Readable streams can only be consumed once, so we need a copy for hashing.
  let contentForHashing = content;
  if (content instanceof ReadableStream) {
    [content, contentForHashing] = content.tee();
  }

  const timestamp = now();
  const contentHash = await hash(contentForHashing);
  const path = snapPath(basePath, contentHash);

  // Check if there is already a snapshot file with the same content.
  if (contentHash === options.previousHash) {
    if (content instanceof ReadableStream) {
      await content.cancel();
    }
  } else {
    await Deno.mkdir(dirname(path), { recursive: true });

    const snapFile = await Deno.create(path);
    if (content instanceof ReadableStream) {
      await content.pipeTo(snapFile.writable);
      // snapFile closes automatically when the end of the stream has been reached
    } else {
      await snapFile.write(content);
      snapFile.close();
    }
  }

  return {
    timestamp,
    contentHash,
    path,
  };
}

let textEncoder: TextEncoder;

/**
 * Returns the current timestamp in seconds since the UNIX epoch.
 */
function now() {
  return Math.floor(Date.now() / 1000);
}
