import { afterAll, assertEquals, beforeAll, describe, it } from "./deps.ts";
import { SnapStorage } from "./snap_storage.ts";

describe("Snapshot storage", () => {
  let snaps: SnapStorage;

  beforeAll(() => {
    snaps = new SnapStorage("test_data");
  });

  afterAll(() => {
    snaps.close();
  });

  it("creates a JSON snapshot and retrieves it again", async () => {
    const uri = "test:json/simple";
    const data = { name: "John Doe", value: 42 };
    const createdSnap = await snaps.createSnap(uri, JSON.stringify(data));

    assertEquals(
      snaps.getLatestSnap(uri),
      createdSnap,
      "Retrieved snapshot does not match created snapshot",
    );

    const loadedSnap = await snaps.loadJSON(uri);
    assertEquals(
      loadedSnap.content,
      data,
      "JSON data could not be retrieved again",
    );
  });
});
