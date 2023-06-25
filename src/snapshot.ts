import { assert, crypto, joinPath, toHashString } from "./deps.ts";

export type Content = ReadableStream<Uint8Array>;

export type Policy = {
  /** Maximum age of the snapshot in seconds. */
  maxAge?: number;
};

export type Snapshot = {
  /** Creation date and time in seconds since the UNIX epoch. */
  timestamp: number;
  contentHash: string;
  path: string;
};

export function followsPolicy(snap: Snapshot, policy: Policy) {
  if (policy.maxAge) {
    return now() < snap.timestamp + policy.maxAge;
  }
  return true;
}

export async function hash(content: Content): Promise<string> {
  const contentHash = await crypto.subtle.digest("SHA-256", content);
  return toHashString(contentHash);
}

export const hashLength = 64;

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

export async function writeSnap(
  basePath: string,
  content: Content,
): Promise<Snapshot> {
  const timestamp = now();
  const contentHash = await hash(content);
  const path = snapPath(basePath, contentHash);

  const snapFile = await Deno.create(path);
  content.pipeTo(snapFile.writable);
  snapFile.close();

  return {
    timestamp,
    contentHash,
    path,
  };
}

/**
 * Returns the current timestamp in seconds since the UNIX epoch.
 */
function now() {
  return Math.floor(Date.now() / 1000);
}
