# golden fixtures

Java regen for committed oracle bytes:

- `make recordinglog-fixtures-regen` writes `cluster/recordinglog/testdata/`
- `make node-state-fixtures-regen` writes `cluster/election/testdata/node-state/`
- `make recovery-state-key-fixtures-regen` writes `cluster/codecs/testdata/recovery-state-key/`

Verify: `make test-golden-fixtures-unit` (also via `make check` when mvn is installed).
