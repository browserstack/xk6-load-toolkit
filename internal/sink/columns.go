package sink

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type valueKind int

const (
	kindString valueKind = iota
	kindInt
	kindBool
	kindUint // a string source parsed and bounded by the column's min/max
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

	// Request rows only: whether a verdict-metric sample reached the row and
	// what it said, whether a join sample reached it, and the values of its
	// distinct-bounded columns by column index (first value wins).
	verdictSet    bool
	verdictFailed bool
	joined        bool
	resolved      map[int]string
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
	// failedVerdict is 1 when the verdict metric reported a failure, 0 when it
	// reported success and -1 when no verdict arrived (and on WebSocket rows).
	"failedVerdict": {kind: kindInt, num: func(v *rowView) int64 {
		switch {
		case !v.verdictSet:
			return -1
		case v.verdictFailed:
			return 1
		default:
			return 0
		}
	}},
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

	// Optional bounds; all zero for a plain column.
	min, max       uint64 // kindUint range
	maxRunes       int    // > 0 truncates a string value
	when           []condition
	budget         *distinctBudget
	joinDefault    *string
	joinDefaultNum uint64 // joinDefault parsed, for kindUint
}

// condition holds when its source reads equals, or any non-empty value when
// equals is nil.
type condition struct {
	src    source
	equals *string
}

// distinctBudget caps the distinct values one column writes per process.
// Values beyond the limit are written as overflow. It is shared by every row
// the column renders, so it carries its own lock.
type distinctBudget struct {
	limit     int
	overflow  string
	partition *source        // nil: one budget
	buckets   map[string]int // partition value -> budget index; unlisted values use 0

	mu   sync.Mutex
	seen []map[string]struct{}
}

// admit returns value while its budget holds it or has room, else the overflow value.
func (d *distinctBudget) admit(v *rowView, value string) string {
	bucket := 0
	if d.partition != nil {
		bucket = d.buckets[d.partition.read(v)]
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	set := d.seen[bucket]
	if _, ok := set[value]; ok {
		return value
	}
	if len(set) >= d.limit {
		return d.overflow
	}
	set[value] = struct{}{}
	return value
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
		if err := compileBounds(&col, spec, builtins, constants, where); err != nil {
			return nil, err
		}
		out = append(out, col)
	}
	return out, nil
}

// compileBounds applies a column's optional type, bounds, conditions and default.
func compileBounds(col *column, spec ColumnSpec, builtins map[string]builtin, constants map[string]string, where string) error {
	switch spec.Type {
	case "", "string":
		if spec.Min != nil || spec.Max != nil {
			return fmt.Errorf("%s: min and max apply only to type \"uint\"", where)
		}
	case "uint":
		if col.kind != kindString {
			return fmt.Errorf("%s.type: \"uint\" reads string sources (tag:, meta:, const:), not a numeric or boolean computed value", where)
		}
		if spec.Max == nil {
			return fmt.Errorf("%s.max: required for type \"uint\"", where)
		}
		col.max = *spec.Max
		if spec.Min != nil {
			col.min = *spec.Min
		}
		if col.min > col.max {
			return fmt.Errorf("%s.min: must not exceed max", where)
		}
		if spec.MaxRunes != 0 || spec.Distinct != nil {
			return fmt.Errorf("%s: maxRunes and distinct apply only to string columns", where)
		}
		col.kind = kindUint
	default:
		return fmt.Errorf("%s.type: unknown type %q (want \"string\" or \"uint\")", where, spec.Type)
	}

	if spec.MaxRunes < 0 {
		return fmt.Errorf("%s.maxRunes: must not be negative", where)
	}
	if (spec.MaxRunes > 0 || spec.Distinct != nil || spec.JoinDefault != nil || len(spec.When) > 0) &&
		col.kind != kindString && col.kind != kindUint {
		return fmt.Errorf("%s: bounds, conditions and joinDefault do not apply to a numeric or boolean computed value", where)
	}
	col.maxRunes = spec.MaxRunes

	for j, w := range spec.When {
		src, kind, err := parseSource(w.Source, builtins, constants)
		if err != nil {
			return fmt.Errorf("%s.when[%d].source: %w", where, j, err)
		}
		if kind != kindString {
			return fmt.Errorf("%s.when[%d].source: must be a string source", where, j)
		}
		col.when = append(col.when, condition{src: src, equals: w.Equals})
	}

	if d := spec.Distinct; d != nil {
		if d.Limit <= 0 {
			return fmt.Errorf("%s.distinct.limit: must be positive", where)
		}
		if d.Overflow == "" {
			return fmt.Errorf("%s.distinct.overflow: must not be empty", where)
		}
		if (d.PartitionBy == "") != (len(d.Partitions) == 0) {
			return fmt.Errorf("%s.distinct: partitionBy and partitions are set together", where)
		}
		budget := &distinctBudget{limit: d.Limit, overflow: d.Overflow, seen: []map[string]struct{}{{}}}
		if d.PartitionBy != "" {
			src, kind, err := parseSource(d.PartitionBy, builtins, constants)
			if err != nil {
				return fmt.Errorf("%s.distinct.partitionBy: %w", where, err)
			}
			if kind != kindString {
				return fmt.Errorf("%s.distinct.partitionBy: must be a string source", where)
			}
			budget.partition = &src
			budget.buckets = make(map[string]int, len(d.Partitions))
			for _, p := range d.Partitions {
				if _, dup := budget.buckets[p]; dup {
					continue
				}
				budget.buckets[p] = len(budget.seen)
				budget.seen = append(budget.seen, map[string]struct{}{})
			}
		}
		col.budget = budget
	}

	if spec.JoinDefault != nil {
		value := *spec.JoinDefault
		if col.kind == kindUint {
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil || n < col.min || n > col.max {
				return fmt.Errorf("%s.joinDefault: %q is not a number in [min, max]", where, value)
			}
			col.joinDefaultNum = n
		}
		col.joinDefault = &value
	}
	return nil
}

// allSources lists every source the column reads, conditions and partition included.
func (c *column) allSources() []source {
	out := append([]source(nil), c.sources...)
	for _, w := range c.when {
		out = append(out, w.src)
	}
	if c.budget != nil && c.budget.partition != nil {
		out = append(out, *c.budget.partition)
	}
	return out
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
		case kindUint:
			buf = strconv.AppendUint(buf, c.uintValue(v), 10)
		default:
			encoded, _ := json.Marshal(c.stringValue(i, v))
			buf = append(buf, encoded...)
		}
	}
	return append(buf, '}')
}

// raw is the first non-empty string source.
func (c *column) raw(v *rowView) string {
	for _, s := range c.sources {
		if value := s.read(v); value != "" {
			return value
		}
	}
	return ""
}

// conditionsHold reports whether every when condition holds.
func (c *column) conditionsHold(v *rowView) bool {
	for _, w := range c.when {
		got := w.src.read(v)
		if w.equals == nil {
			if got == "" {
				return false
			}
		} else if got != *w.equals {
			return false
		}
	}
	return true
}

// stringValue renders a string column. Without bounds it is the first
// non-empty source, exactly as before bounds existed.
func (c *column) stringValue(idx int, v *rowView) string {
	if len(c.when) > 0 && !c.conditionsHold(v) {
		return ""
	}
	if c.budget != nil {
		if value, ok := v.resolved[idx]; ok {
			return value
		}
	}
	value := c.raw(v)
	if value == "" {
		if v.joined && c.joinDefault != nil {
			return *c.joinDefault
		}
		return ""
	}
	value = truncateRunes(value, c.maxRunes)
	if c.budget != nil {
		value = c.budget.admit(v, value)
	}
	return value
}

// resolveDistinct fixes a distinct-bounded column's value the first time it is
// non-empty, so the budget is spent in arrival order. ok is false while the
// column has no value yet.
func (c *column) resolveDistinct(v *rowView) (string, bool) {
	if !c.conditionsHold(v) {
		return "", false
	}
	value := truncateRunes(c.raw(v), c.maxRunes)
	if value == "" {
		return "", false
	}
	return c.budget.admit(v, value), true
}

// uintValue parses a uint column; unparseable or out-of-range values give 0,
// or joinDefault on a joined row.
func (c *column) uintValue(v *rowView) uint64 {
	if !c.conditionsHold(v) {
		return 0
	}
	if n, err := strconv.ParseUint(c.raw(v), 10, 64); err == nil && n >= c.min && n <= c.max {
		return n
	}
	if v.joined && c.joinDefault != nil {
		return c.joinDefaultNum
	}
	return 0
}

// truncateRunes keeps at most n runes of s; n <= 0 keeps all of it.
func truncateRunes(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
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
