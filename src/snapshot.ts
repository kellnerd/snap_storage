import { assert, crypto, joinPath, toHashString } from "./deps.ts";

export type Content =
  | BufferSource
  | AsyncIterable<BufferSource>
  | Iterable<BufferSource>;

export type Snapshot = {
  timestamp: number;
  contentHash: string;
  path: string;
};

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
