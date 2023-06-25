import { assertEquals } from "./deps.ts";
import { SnapStorage } from "./snap_storage.ts";

Deno.test("Create a JSON snapshot and retrieve it again", async () => {
  const snaps = new SnapStorage("test_data");

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

  snaps.close();
});
