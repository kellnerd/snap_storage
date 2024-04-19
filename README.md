# Snapshot Storage

This [Deno](https://deno.land) module provides a persistent storage mechanism
for multiple snapshots of the content which is referenced by an URI (i.e. an URL
or another unique ID).

Each snapshot is identified by the URI and a timestamp. Identical snapshots do
not waste additional storage space because the content will be deduplicated
automatically using a hash function.

## Contributing

Your contributions are welcome, be it code, documentation or feedback.

If you want to contribute a bigger feature, please open a discussion first to be
sure that your idea will be accepted.

Before submitting your changes, please make sure that they pass the tests, are
properly formatted and follow the linting rules:

```sh
deno task test
deno fmt --check
deno lint
```
