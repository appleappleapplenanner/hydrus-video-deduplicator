# testdb

testdb is a submodule that contains the Hydrus DB and videos for testing purposes.

It is a submodule to avoid bloating this repo with large media files used for testing.

## Instructions

To checkout the submodule:

```sh
git submodule update --init --recursive
```

To run tests:

```sh
hatch run test:all
```

To run only specific tests, e.g., vcr:

```sh
hatch run test:vcr
```

See [pyproject.toml](../pyproject.toml) test.scripts for full list of test groups.

---

HVD DEV ONLY:

To update the submodule to main:

```sh
git submodule foreach git pull origin main
```
