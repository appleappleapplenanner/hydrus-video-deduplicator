# Development

## Setup

[Hatch](https://hatch.pypa.io/latest/) is the Python project manager for this project.

It is used for packaging and environment management for development.

There are hatch commands for this project are defined in the [pyproject.toml]. They are just aliases for other commands that run in specific environments.

For example, to run the command to generate the documentation and serve it locally:

```sh
hatch run docs:serve
```

`docs` is the environment and `build` is the command to run.

For more information, see the Hatch [environment documentation](https://hatch.pypa.io/latest/environment/)

### Useful commands

Run all tests:

```sh
hatch run test:all
```

Serve documentation locally:

```sh
hatch run docs:serve
```

Build documentation:

```sh
hatch run docs:build
```

Check code formatting (doesn't actually run formatting):

```sh
hatch run lint:format
```

Lint code:

```sh
hatch run lint:lint
```

Format code:

```sh
hatch run format:format
```

Benchmark vpdq:

```sh
hatch run benchmark:vpdq
```

### Developing without Hatch (not recommended)

Alternatively, if you don't want to use hatch and you know what you're doing, you can install and do development the general way.

1. Clone or download the repository:

    ```sh
    git clone https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator.git
    ```

1. Install local editable package with pip and venv:

    ```sh
    cd hydrus-video-deduplicator
    python3 -m venv venv
    source venv/bin/activate
    pip install -e .
    ```

1. Now if you run `python3 -m hydrusvideodeduplicator` there should be no errors.

---

### Testing

#### testdb

testdb is a submodule that contains the Hydrus DB and videos for integration testing purposes.

It is a submodule to avoid bloating this repo with large media files used for testing.

To checkout the testdb submodule:

```sh
git submodule update --init --recursive
```

Run tests with `hatch run test:all`

TODO: Explain how to run Hydrus using this DB.

## git workflow

`main` is for releases. It must be committed to through a PR from `develop` by the project maintainer.

`develop` should have the same history as main unless it has newer commits that have not been merged. When a PR is approved to develop, changes should be squash-merged.

Create PRs for `develop` if you want to submit changes.
