import {
  afterAll,
  assertEquals,
  beforeAll,
  describe,
  it,
  joinPath,
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

  it("caches a JSON API response", async () => {
    const url = "https://api.github.com/users/denoland";
    const fetchedSnap = await snaps.cache(url);
    const storedSnap = await snaps.loadJSON(url);

    assertEquals(
      await fetchedSnap.content.json(),
      storedSnap.content,
      "Fetched JSON response has not been persisted correctly",
    );
  });
});
