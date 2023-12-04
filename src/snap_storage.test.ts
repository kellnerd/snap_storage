import {
  afterAll,
  assert,
  assertEquals,
  beforeAll,
  delay,
  describe,
  it,
  joinPath,
  resolve,
  toFileUrl,
} from "./deps.ts";
import { SnapStorage } from "./snap_storage.ts";

describe("Snapshot storage", () => {
  const testDirectory = "test_data";
  let snaps: SnapStorage;

  beforeAll(async () => {
    await Deno.mkdir(testDirectory);
    snaps = new SnapStorage(testDirectory);
  });

  afterAll(async () => {
    snaps.close();
    await Deno.remove(testDirectory, { recursive: true });
  });

  it("opens a new database in the correct directory", async () => {
    const dbPath = joinPath(testDirectory, "snaps.db");
    await Deno.lstat(dbPath);
  });

  it("does not retrieve a snapshot of an unknown URI", () => {
    assert(snaps.getLatestSnap("test:unknown") === undefined);
  });

  it("only retrieves an existing snapshot when it is not too old", async () => {
    const uri = "test:plaintext";
    await snaps.createSnap(uri, "test content");

    assert(snaps.getSnap(uri), "Snapshot could not be retrieved again at all");

    assert(
      snaps.getSnap(uri, { maxAge: 9999 }),
      "Snapshot could not be retrieved again despite large maxAge",
    );

    assert(
      snaps.getSnap(uri, { maxAge: -9999 }) === undefined,
      "Snapshot could be retrieved again despite negative maxAge",
    );
  });

  it("creates a JSON snapshot and retrieves it again", async () => {
    const uri = "test:json/simple";
    const data = { name: "John Doe", value: 42 };
    const createdSnap = await snaps.createSnap(uri, JSON.stringify(data));

    assertEquals(
      snaps.getLatestSnap(uri),
      createdSnap,
      "Retrieved snapshot metadata does not match the created snapshot",
    );

    const loadedSnap = await snaps.loadJSON(uri);
    assertEquals(
      loadedSnap.content,
      data,
      "JSON data could not be retrieved again",
    );
  });

  it("retrieves the latest snapshot if there are multiple", async () => {
    const uri = "test:updated";

    await snaps.createSnap(uri, JSON.stringify({ value: "old" }));
    await delay(1000); // timestamp resolution is seconds
    await snaps.createSnap(uri, JSON.stringify({ value: "new" }));

    const snap = await snaps.loadJSON<{ value: string }>(uri);
    assertEquals(
      snap.content,
      { value: "new" },
      "Snapshot should contain the latest content",
    );
  });

  it("caches a fetched JSON response", async () => {
    const url = toFileUrl(resolve("deno.json"));
    const fetchedSnap = await snaps.cache(url);
    const storedSnap = await snaps.loadJSON(url.href);

    assertEquals(
      await fetchedSnap.content.json(),
      storedSnap.content,
      "Fetched JSON response has not been persisted correctly",
    );

    assert(fetchedSnap.isFresh, "Fetched snapshots have to be marked as fresh");

    assert(!storedSnap.isFresh, "Existing snapshots should not be fresh");
  });
});
