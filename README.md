# xk6-load-toolkit

A [k6](https://github.com/grafana/k6) extension with three independent,
configuration-driven capabilities:

| Capability | Registered as | Config section |
|---|---|---|
| Row sink | output `load-toolkit-sink` | `sink` |
| Request-rate limiter | JS module `k6/x/load-toolkit/throughput` | `throughput` |
| Tag filter | JS module `k6/x/load-toolkit/tagfilter` | `tagFilter` |

Build it into k6 with [xk6](https://github.com/grafana/xk6):

```sh
xk6 build --with github.com/browserstack/xk6-load-toolkit=.
```

## Configuration

Everything is configured by one JSON document in the environment variable
`XK6_LOAD_TOOLKIT_CONFIG`. The extension reads it at process start, validates
it, and removes it (and any variable listed in `env.unset`) from the environment
before k6 builds `__ENV`, so scripts cannot read it through `__ENV`. Removing a
variable does not scrub the process's initial environment block (on Linux,
`/proc/self/environ`), so never put credentials in the configuration.

- A missing section switches that capability off: the limiter grants
  everything, the filter runs everything, and the sink accepts samples and
  writes nothing.
- An invalid document stops k6 at startup with a fixed message that contains no
  configuration detail. If `XK6_LOAD_TOOLKIT_DIAGNOSTICS` names a file, the
  reason (with the failing path, e.g. `sink.columns[3].from[0]`) is written there.
- Unknown fields are rejected, so a typo fails fast.

```json
{
  "version": 1,
  "env": { "unset": ["SOME_SECRET"] },
  "throughput": { "podRps": 50 },
  "tagFilter": { "include": "{\"group\":[\"Checkout\"]}" },
  "sink": { "...": "see below" }
}
```

### `throughput`

| Key | Meaning |
|---|---|
| `podRps` | grants per rolling second for this process, `>= 1`; fractions truncate |
| `traceFile` | optional: periodically write every grant's timestamp (ns) here |
| `traceCap` | optional: maximum grants traced (default 500000) |

The JS module exports `_enabled()`, `_rate()`, `_acquire()`, `_acquireN(n)` and
`_dumpTrace()`. One sliding window is shared by every VU in the process.

### `tagFilter`

`include` or `exclude` is a JSON string `{"test":[...],"group":[...],"scenario":[...]}`.
Include wins when both are set; a rule that does not parse leaves the filter off.
Levels are OR'd. `test` and `scenario` match exactly; a `group` entry matches its
group and every nested group (`A` matches `::A::B`). The JS module exports
`_mode()`, `_shouldRunRequest(tags)`, `_shouldRunGroup(name)` and
`_shouldRunScenario(name)`.

### `sink`

Rows are posted as `JSONEachRow` to a ClickHouse-HTTP-compatible endpoint
(`POST <endpoint>/?query=INSERT INTO <db>.<table> FORMAT JSONEachRow`).

| Key | Meaning |
|---|---|
| `transport.endpoint` | required, `http(s)://host:port` |
| `transport.database`, `table`, `assertionTable` | target tables; check rows need `assertionTable` and `assertionColumns` together |
| `transport.asyncInsert` | add `async_insert=1&wait_for_async_insert=1` |
| `transport.timeout`, `flushInterval`, `completeAfter`, `staleAfter` | durations (`"5s"`); defaults 30s, 5s, 2s, 1m |
| `transport.retry` | `{max, base, maxDelay}`, exponential; defaults 3, 500ms, 5s |
| `transport.maxInsertRows` | rows per POST (default 10000); the first failed chunk stops the flush and the rest are requeued |
| `buffers.metricMax`, `assertionMax` | in-memory caps (defaults 500000, 100000); overflow is shed and logged |
| `constants` | named strings for `const:` sources |
| `columns`, `assertionColumns` | ordered `[{name, from}]`; see below |
| `correlation` | metric, tag and metadata names the row engine joins on; see below |
| `sendTimeRestamp` | stamp request rows at the send instant instead of the response |
| `checkFilter` | drop check samples recorded against requests `tagFilter` skipped |
| `markers` | progress files; see below |
| `cardinality` | `{maxTimeSeries, exitCode, message}`; see below |

#### Columns

Each column's `from` is one source or a fallback list (first non-empty string
wins):

| Source | Value |
|---|---|
| `tag:<key>` | the sample's tag |
| `meta:<key>` | the sample's non-indexed metadata |
| `const:<key>` | `sink.constants[key]` |
| `computed:<name>` | a built-in value |

Request and WebSocket rows (`columns`): `timestamp`, `scenario` (default
`default`), `status` (default `0`), `statusMessage` (`OK` for 2xx/3xx, or 101 on
WebSocket), `errorCount`, `url` (method-prefixed real URL), `requestName`
(method-prefixed name tag, or the metric name on WebSocket rows), `group`, `vus`,
`duration`, `latency`, `connect`, `bytesSent`, `bytesReceived`.

Check rows (`assertionColumns`): `timestamp`, `parentLabel` and `parentStatus`
(the name/url and status of the same VU's previous request), `failed`.

Numeric and boolean built-ins cannot be part of a fallback list.

#### Correlation

| Key | Meaning |
|---|---|
| `requestMetrics` | which of `http_reqs`, `http_req_duration`, `http_req_waiting`, `http_req_blocked`, `http_req_connecting`, `http_req_tls_handshaking` to collect; a row is written once `http_reqs` arrives |
| `urlMetaKey`, `urlLegacyTag` | where the real URL is, when the script's `name` tag replaced k6's `url` tag |
| `bytesIdMetaKey`, `bytesSentMetric`, `bytesReceivedMetric` | per-request byte counters, joined to their request by a metadata id |
| `wsMetrics` | WebSocket metrics written one row per sample |
| `wsSummedMetrics` | WebSocket counters summed per flush and per url/status/scenario |
| `wsErrorMetric`, `wsErrorMetaKey` | a WebSocket error metric whose metadata carries the cause for `statusMessage` |
| `checkMetric` | the check metric (`checks`) |

#### Markers

`{dir, totalRows, scriptData, pushAttempted, iteration, iterationThreshold, dataMetricPrefixes}`.
`totalRows` holds the number of rows delivered. The others are written once with
`1`: `scriptData` on the first sample whose metric is the check metric or starts
with a `dataMetricPrefixes` entry, `pushAttempted` before the first POST, and
`iteration` after `iterationThreshold` (default 3) iterations. Stale markers are
removed at start. An empty name disables that marker.

#### Cardinality guard

When the number of distinct time series (metric plus indexed tags) exceeds
`maxTimeSeries`, the run is stopped through k6's test-run-stop hook with
`exitCode` (default 105) and `message`, in which `{count}` and `{limit}` are
replaced. Buffered rows are still flushed. `maxTimeSeries <= 0` disables it.

## Tests

```sh
go test ./...
```

## License

AGPL-3.0, the same license as k6. See [LICENSE](LICENSE).
