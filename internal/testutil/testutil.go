// Package testutil holds test fixtures shared by more than one package. It
// imports no capability package, so a capability's own in-package tests can
// use it without an import cycle.
package testutil

import "go.k6.io/k6/v2/metrics"

// SinkConfigJSON is a neutral, valid sink section that exposes every computed
// value. The sink tests decode it into their Config; the config tests embed it
// in a full document.
const SinkConfigJSON = `{
	"transport": {"endpoint": "http://127.0.0.1:1", "table": "requests", "assertionTable": "checks"},
	"constants": {"job": "job-const", "instance": "main"},
	"columns": [
		{"name": "timestamp", "from": ["computed:timestamp"]},
		{"name": "job", "from": ["tag:job", "const:job"]},
		{"name": "scenario", "from": ["computed:scenario"]},
		{"name": "status", "from": ["computed:status"]},
		{"name": "statusMessage", "from": ["computed:statusMessage"]},
		{"name": "url", "from": ["computed:url"]},
		{"name": "requestName", "from": ["computed:requestName"]},
		{"name": "group", "from": ["computed:group"]},
		{"name": "vus", "from": ["computed:vus"]},
		{"name": "bytesReceived", "from": ["computed:bytesReceived"]},
		{"name": "bytesSent", "from": ["computed:bytesSent"]},
		{"name": "latency", "from": ["computed:latency"]},
		{"name": "connect", "from": ["computed:connect"]},
		{"name": "duration", "from": ["computed:duration"]},
		{"name": "errorCount", "from": ["computed:errorCount"]},
		{"name": "instance", "from": ["tag:instance", "const:instance"]}
	],
	"assertionColumns": [
		{"name": "name", "from": ["tag:check"]},
		{"name": "label", "from": ["tag:name", "tag:url", "computed:parentLabel"]},
		{"name": "scenario", "from": ["tag:group", "tag:scenario"]},
		{"name": "status", "from": ["tag:status", "computed:parentStatus"]},
		{"name": "failed", "from": ["computed:failed"]},
		{"name": "job", "from": ["tag:job", "const:job"]}
	],
	"correlation": {
		"requestMetrics": ["http_reqs", "http_req_duration", "http_req_waiting", "http_req_blocked", "http_req_connecting", "http_req_tls_handshaking"],
		"urlMetaKey": "fullUrl",
		"urlLegacyTag": "legacyUrl",
		"bytesIdMetaKey": "reqId",
		"bytesSentMetric": "req_bytes_sent",
		"bytesReceivedMetric": "req_bytes_received",
		"wsMetrics": ["ws_connecting", "ws_sessions", "ws_session_duration", "ws_ping", "ws_connect_error"],
		"wsSummedMetrics": ["ws_msgs_sent", "ws_msgs_received"],
		"wsErrorMetric": "ws_connect_error",
		"wsErrorMetaKey": "wsError",
		"checkMetric": "checks"
	},
	"sendTimeRestamp": true
}`

// GroupEnabled is a system tag set with only the group tag enabled.
func GroupEnabled() *metrics.SystemTagSet {
	return metrics.NewSystemTagSet(metrics.TagGroup)
}
