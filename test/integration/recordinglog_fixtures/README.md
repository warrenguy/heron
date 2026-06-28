# recordinglog golden fixtures

Regenerates committed files under `cluster/recordinglog/testdata/`.

```bash
make recordinglog-fixtures-regen
```

`mvn -q test` checks committed bytes match a fresh run without overwriting.
`cluster/recordinglog/fixtures_test.go` is the CI oracle; Java is not invoked in `make check`.