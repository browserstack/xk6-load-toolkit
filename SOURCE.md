# Corresponding Source for the k6 binary

The k6 binary built from this repository is not made from this repository's code
alone. It links k6 itself, this module, and fifteen third-party extensions, and
it is compiled by xk6 with a specific Go toolchain. This file lists every one of
those inputs, at the exact version, and the recipe that turns them into the
binary, so that anyone who receives the binary can rebuild it.

## Which version of this file applies to a binary

Every version pin below lives in this repository (`extensions.json`, this file,
`.github/workflows/ci.yml`), so the version of this file **at the commit a binary
was built from** is the source listing for that binary. Read that commit from
the binary's container image:

```sh
cat /opt/xk6-load-toolkit/build-info.json   # "commit", "arch", "sha256" of the binary
```

then read this file at that commit:
`https://github.com/browserstack/xk6-load-toolkit/blob/<commit>/SOURCE.md`.

## Linked components

### k6 and this module

| Component | Go module | Version | Commit | Licence | Source |
|---|---|---|---|---|---|
| k6 | `go.k6.io/k6/v2` | v2.2.0 | `00a9a1b7f552d6bb4337278b10ae25aac0f4e666` | AGPL-3.0 | <https://github.com/grafana/k6/tree/v2.2.0> |
| xk6-load-toolkit | `github.com/browserstack/xk6-load-toolkit` | the commit in `build-info.json` | (same) | AGPL-3.0 | <https://github.com/browserstack/xk6-load-toolkit/tree/<commit>> |

### Third-party extensions (15, pinned in `extensions.json`)

| Extension | Go module | Version | Commit | Licence | Source |
|---|---|---|---|---|---|
| dns | `github.com/grafana/xk6-dns` | v1.2.0 | `3c1e5d1f1c6f98e51ad6a4a8da32d52437286d2f` | AGPL-3.0 | <https://github.com/grafana/xk6-dns/tree/v1.2.0> |
| faker | `github.com/grafana/xk6-faker` | v0.5.3 | `4bee0e8e1a580bb1ffa5d4c04956bc92aaa023bd` | AGPL-3.0 | <https://github.com/grafana/xk6-faker/tree/v0.5.3> |
| icmp | `github.com/grafana/xk6-icmp` | v0.3.3 | `e3261d10b5607084855728c3a2edb35992b257b6` | AGPL-3.0 | <https://github.com/grafana/xk6-icmp/tree/v0.3.3> |
| mqtt | `github.com/grafana/xk6-mqtt` | v0.3.2 | `00e66fb3dba37fce52d8d0e9ade66d4fb68c9ee3` | AGPL-3.0 | <https://github.com/grafana/xk6-mqtt/tree/v0.3.2> |
| redis | `github.com/grafana/xk6-redis` | v0.4.3 | `273f36deae0534bb7e65aea5c323ecc1ad5f6716` | Apache-2.0 | <https://github.com/grafana/xk6-redis/tree/v0.4.3> |
| client-prometheus-remote | `github.com/grafana/xk6-client-prometheus-remote` | v0.5.3 | `b2a97ac06a4a3c134e7f5b8ab64085df5d8c8049` | Apache-2.0 | <https://github.com/grafana/xk6-client-prometheus-remote/tree/v0.5.3> |
| sql | `github.com/grafana/xk6-sql` | v1.2.2 | `0b0d08de5972ce71bd1984b5f36facfccd8a2254` | Apache-2.0 | <https://github.com/grafana/xk6-sql/tree/v1.2.2> |
| sql-driver-azuresql | `github.com/grafana/xk6-sql-driver-azuresql` | v0.3.1 | `b800867496c4f9f982e9cf86ee820104b65642b3` | AGPL-3.0 | <https://github.com/grafana/xk6-sql-driver-azuresql/tree/v0.3.1> |
| sql-driver-clickhouse | `github.com/grafana/xk6-sql-driver-clickhouse` | v0.3.1 | `8b6f8872cd66356d0496a4d1777304fcad957984` | AGPL-3.0 | <https://github.com/grafana/xk6-sql-driver-clickhouse/tree/v0.3.1> |
| sql-driver-mysql | `github.com/grafana/xk6-sql-driver-mysql` | v0.4.1 | `d7d49e091c552e76f7db812e2cd8487ccd6cb0ed` | AGPL-3.0 | <https://github.com/grafana/xk6-sql-driver-mysql/tree/v0.4.1> |
| sql-driver-postgres | `github.com/grafana/xk6-sql-driver-postgres` | v0.3.1 | `321ca22613bc81cdaa26bab22d63c0094425768f` | AGPL-3.0 | <https://github.com/grafana/xk6-sql-driver-postgres/tree/v0.3.1> |
| sql-driver-sqlserver | `github.com/grafana/xk6-sql-driver-sqlserver` | v0.3.1 | `6a489dc7acf2d6b27e8cb6e7c1f1b700084f82ef` | AGPL-3.0 | <https://github.com/grafana/xk6-sql-driver-sqlserver/tree/v0.3.1> |
| tcp | `github.com/grafana/xk6-tcp` | v0.3.1 | `5cc4495c5150c9e332cf0d219b8cf4219e41d364` | AGPL-3.0 | <https://github.com/grafana/xk6-tcp/tree/v0.3.1> |
| tls | `github.com/grafana/xk6-tls` | v0.3.1 | `3560b8d8e0750aa06b5c25aebc647c25b1c8be10` | AGPL-3.0 | <https://github.com/grafana/xk6-tls/tree/v0.3.1> |
| client-tracing | `github.com/grafana/xk6-client-tracing` | v0.0.10 | `e7c578899134dd231e0f61360a3d8d6a0a5575e4` | AGPL-3.0 | <https://github.com/grafana/xk6-client-tracing/tree/v0.0.10> |

Twelve of the fifteen extensions are AGPL-3.0; redis, client-prometheus-remote
and sql are Apache-2.0. Licences were read from each repository at the pinned
version.

### Transitive Go modules

Every other module linked into the binary is resolved by Go's minimal version
selection from the `go.mod` files of the components above, so it is fixed by
them. The binary records the full list with versions and checksums:

```sh
go version -m k6        # every linked module, its version and its h1: checksum
```

Each listed module's source is available from the Go module proxy at that
version (`https://proxy.golang.org/<module>/@v/<version>.zip`) and is under its
own licence.

## Build tools

| Tool | Version | Licence | Source |
|---|---|---|---|
| Go toolchain | go1.26.11, image `golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0` | BSD-3-Clause | <https://go.dev/dl/> |
| xk6 | v1.4.12 (`go.k6.io/xk6`, commit `3b5a796179b8f5e714fe25d9806e6ab44ea75e50`) | Apache-2.0 | <https://github.com/grafana/xk6/tree/v1.4.12> |
| git, jq, bash | from the Go image's Debian bookworm packages | various | <https://packages.debian.org/bookworm/> |

Build settings, which `go version -m` also records: `GOOS=linux`,
`GOARCH=amd64` (with `GOAMD64=v1`) or `GOARCH=arm64`, `CGO_ENABLED=0`,
`-trimpath`.

## Build scripts and manifest

All in this repository at the same commit:

| File | Role |
|---|---|
| [`extensions.json`](extensions.json) | The manifest: the k6 version and every extension with its pinned version. |
| [`build.sh`](build.sh) | Turns the manifest into `xk6 build --k6-version … --with <module>@<version> …` and runs it with CGO off. |
| [`scripts/release-gate.sh`](scripts/release-gate.sh) | Reads the binary's `go version -m` output and fails unless every pin above is linked at exactly its version. |
| This file | The builder stage below, the toolchain pins and the component list. |

### Builder stage

The container stage the binary is built in. The `golang` image digest and the
xk6 version are the pins listed under [Build tools](#build-tools).

```dockerfile
FROM --platform=linux/amd64 golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS k6-builder

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git jq && \
    rm -rf /var/lib/apt/lists/*

RUN go install go.k6.io/xk6/cmd/xk6@v1.4.12 && ln -sf /go/bin/xk6 /usr/local/bin/xk6

# The commit from build-info.json, and the directory the original build checked
# this repository out to (see "Rebuilding the exact binary" below).
ARG TOOLKIT_COMMIT
ARG TOOLKIT_DIR=/src/xk6-load-toolkit
ARG TARGETARCH=amd64

RUN git clone https://github.com/browserstack/xk6-load-toolkit.git "$TOOLKIT_DIR" && \
    git -C "$TOOLKIT_DIR" checkout --detach "$TOOLKIT_COMMIT"

WORKDIR $TOOLKIT_DIR
RUN GOOS=linux GOARCH="$TARGETARCH" CGO_ENABLED=0 ./build.sh --output /out/k6 && \
    scripts/release-gate.sh --binary /out/k6 --arch "$TARGETARCH"
```

## Rebuilding the exact binary

1. Read `commit`, `arch` and `sha256` from `/opt/xk6-load-toolkit/build-info.json`
   in the image, and copy the binary out of it.
2. Read the directory the original build checked this repository out to. Go
   records it in the binary, and it is part of the binary's contents:

   ```sh
   go version -m k6 | grep -A1 'xk6-load-toolkit'   # the "=>" line is the directory
   ```

3. Build with the stage above, passing those values:

   ```sh
   docker build --target k6-builder \
     --build-arg TOOLKIT_COMMIT=<commit> \
     --build-arg TOOLKIT_DIR=<directory from step 2> \
     --build-arg TARGETARCH=<arch> \
     -t k6-rebuild -f <file holding the stage> .
   docker run --rm k6-rebuild sha256sum /out/k6
   ```

4. Compare. The `sha256` should equal the one in `build-info.json`, and
   `go version -m` of the two binaries should match line for line. If the hashes
   differ, the `go version -m` diff names the input that differs.

## Keeping this file current

A change to `extensions.json`, to the Go image or `go-version`, or to the xk6
version must update this file in the same commit, so that this file at any
commit describes the binaries built from that commit.
