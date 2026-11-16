package loadtoolkit

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type valueKind int

const (
	kindString valueKind = iota
	kindInt
	kindBool
)

// rowView is everything a column source can read for one output row.
type rowView struct {
	tags map[string]string
	meta map[string]string
	corr *CorrelationConfig

	// Request and WebSocket rows.
	ws            bool
	wsMetric      string
	wsMessage     string // raw WebSocket error cause, when one was recorded
	timestamp     int64
	duration      int64
	latency       int64
	connect       int64
	bytesSent     int64
	bytesReceived int64

	// Assertion rows.
	parentLabel  string
	parentStatus string
	failed       bool
}

type builtin struct {
	kind    valueKind
	str     func(*rowView) string
	num     func(*rowView) int64
	boolean func(*rowView) bool
}

// requestBuiltins are the computed values available to sink.columns. Each one
// covers both HTTP request rows and WebSocket rows.
var requestBuiltins = map[string]builtin{
	"timestamp":     {kind: kindInt, num: func(v *rowView) int64 { return v.timestamp }},
	"scenario":      {kind: kindString, str: func(v *rowView) string { return nonEmpty(v.tags["scenario"], "default") }},
	"status":        {kind: kindString, str: func(v *rowView) string { return nonEmpty(v.tags["status"], "0") }},
	"statusMessage": {kind: kindString, str: statusMessage},
	"errorCount":    {kind: kindInt, num: errorCount},
	"url":           {kind: kindString, str: rowURL},
	"requestName":   {kind: kindString, str: requestName},
	"group":         {kind: kindString, str: func(v *rowView) string { return v.tags["group"] }},
	"vus": {kind: kindInt, num: func(v *rowView) int64 {
		if v.ws {
			return 0
		}
		return parseInt64(v.tags["vu"], 0)
	}},
	"duration":      {kind: kindInt, num: func(v *rowView) int64 { return v.duration }},
	"latency":       {kind: kindInt, num: func(v *rowView) int64 { return v.latency }},
	"connect":       {kind: kindInt, num: func(v *rowView) int64 { return v.connect }},
	"bytesSent":     {kind: kindInt, num: func(v *rowView) int64 { return v.bytesSent }},
	"bytesReceived": {kind: kindInt, num: func(v *rowView) int64 { return v.bytesReceived }},
}

// assertionBuiltins are the computed values available to sink.assertionColumns.
var assertionBuiltins = map[string]builtin{
	"timestamp":    {kind: kindInt, num: func(v *rowView) int64 { return v.timestamp }},
	"parentLabel":  {kind: kindString, str: func(v *rowView) string { return v.parentLabel }},
	"parentStatus": {kind: kindString, str: func(v *rowView) string { return v.parentStatus }},
	"failed":       {kind: kindBool, boolean: func(v *rowView) bool { return v.failed }},
}

type source struct {
	tag, meta string
	isTag     bool
	isMeta    bool
	constant  *string
	builtin   *builtin
}

func (s source) read(v *rowView) string {
	switch {
	case s.isTag:
		return v.tags[s.tag]
	case s.isMeta:
		return v.meta[s.meta]
	case s.constant != nil:
		return *s.constant
	default:
		return s.builtin.str(v)
	}
}

type column struct {
	key     []byte // `"name":`
	kind    valueKind
	sources []source
}

type columns []column

// compileColumns resolves every column source once, so rendering a row is a
// slice walk with no map lookups on the configuration.
func compileColumns(specs []ColumnSpec, builtins map[string]builtin, constants map[string]string, path string) (columns, error) {
	out := make(columns, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for i, spec := range specs {
		where := fmt.Sprintf("%s[%d]", path, i)
		if spec.Name == "" {
			return nil, fmt.Errorf("%s.name: must not be empty", where)
		}
		if _, dup := seen[spec.Name]; dup {
			return nil, fmt.Errorf("%s.name: duplicate column %q", where, spec.Name)
		}
		seen[spec.Name] = struct{}{}
		if len(spec.From) == 0 {
			return nil, fmt.Errorf("%s.from: at least one source is required", where)
		}
		key, _ := json.Marshal(spec.Name)
		col := column{key: append(key, ':'), kind: kindString}
		for j, raw := range spec.From {
			src, kind, err := parseSource(raw, builtins, constants)
			if err != nil {
				return nil, fmt.Errorf("%s.from[%d]: %w", where, j, err)
			}
			if kind != kindString && len(spec.From) > 1 {
				return nil, fmt.Errorf("%s.from: a numeric or boolean source cannot be part of a fallback list", where)
			}
			col.kind = kind
			col.sources = append(col.sources, src)
		}
		out = append(out, col)
	}
	return out, nil
}

func parseSource(raw string, builtins map[string]builtin, constants map[string]string) (source, valueKind, error) {
	prefix, name, ok := strings.Cut(raw, ":")
	if !ok || name == "" {
		return source{}, 0, fmt.Errorf("%q is not <kind>:<name>", raw)
	}
	switch prefix {
	case "tag":
		return source{isTag: true, tag: name}, kindString, nil
	case "meta":
		return source{isMeta: true, meta: name}, kindString, nil
	case "const":
		value, ok := constants[name]
		if !ok {
			return source{}, 0, fmt.Errorf("constant %q is not defined in sink.constants", name)
		}
		return source{constant: &value}, kindString, nil
	case "computed":
		b, ok := builtins[name]
		if !ok {
			return source{}, 0, fmt.Errorf("unknown computed value %q", name)
		}
		return source{builtin: &b}, b.kind, nil
	default:
		return source{}, 0, fmt.Errorf("unknown source kind %q", prefix)
	}
}

// appendRow renders one JSON object. Strings are encoded with encoding/json so
// escaping matches a json.Marshal of an equivalent struct byte for byte.
func (cols columns) appendRow(buf []byte, v *rowView) []byte {
	buf = append(buf, '{')
	for i := range cols {
		c := &cols[i]
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, c.key...)
		switch c.kind {
		case kindInt:
			buf = strconv.AppendInt(buf, c.sources[0].builtin.num(v), 10)
		case kindBool:
			buf = strconv.AppendBool(buf, c.sources[0].builtin.boolean(v))
		default:
			value := ""
			for _, s := range c.sources {
				if value = s.read(v); value != "" {
					break
				}
			}
			encoded, _ := json.Marshal(value)
			buf = append(buf, encoded...)
		}
	}
	return append(buf, '}')
}

func statusMessage(v *rowView) string {
	status := nonEmpty(v.tags["status"], "0")
	if v.ws {
		if v.wsMessage != "" {
			return v.wsMessage
		}
		if status == "101" {
			return "OK"
		}
		return "Error"
	}
	if status[0] == '2' || status[0] == '3' {
		return "OK"
	}
	return "Error"
}

func errorCount(v *rowView) int64 {
	status := nonEmpty(v.tags["status"], "0")
	if v.ws {
		if status == "101" {
			return 0
		}
		return 1
	}
	if status[0] == '2' || status[0] == '3' {
		return 0
	}
	return 1
}

// requestName is the request's name tag (or its url tag when unnamed),
// prefixed with the method. WebSocket rows carry their metric name instead.
func requestName(v *rowView) string {
	if v.ws {
		return v.wsMetric
	}
	name := nonEmpty(v.tags["name"], v.tags["url"])
	if name == "" {
		return ""
	}
	return withMethod(methodOf(v.tags), name)
}

// rowURL is the real request URL, prefixed with the method. k6 replaces the url
// tag with the name tag on named requests, so the real URL comes from metadata
// or a legacy tag when either is present.
func rowURL(v *rowView) string {
	if v.ws {
		return nonEmpty(v.tags["url"], "unknown")
	}
	method := methodOf(v.tags)
	nameTag, urlTag := v.tags["name"], v.tags["url"]
	realURL := ""
	if v.corr.URLMetaKey != "" {
		realURL = v.meta[v.corr.URLMetaKey]
	}
	if realURL == "" && v.corr.URLLegacyTag != "" {
		realURL = v.tags[v.corr.URLLegacyTag]
	}
	if realURL == "" && nameTag == "" {
		realURL = urlTag
	}
	switch {
	case realURL != "":
		return withMethod(method, realURL)
	case urlTag != "":
		return withMethod(method, urlTag)
	default:
		return "unknown"
	}
}

func methodOf(tags map[string]string) string { return nonEmpty(tags["method"], "GET") }

func withMethod(method, value string) string {
	if strings.HasPrefix(value, method+" ") {
		return value
	}
	return method + " " + value
}

func parseInt64(v string, fallback int64) int64 {
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func nonEmpty(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}
