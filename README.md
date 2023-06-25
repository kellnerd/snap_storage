# Snapshot Storage

This [Deno](https://deno.land) module provides a persistent storage mechanism
for multiple snapshots of the content which is referenced by an URI (i.e. an URL
or another unique ID).

Each snapshot is identified by the URI and a timestamp. Identical snapshots do
not waste additional storage space because the content will be deduplicated
automatically using a hash function.
