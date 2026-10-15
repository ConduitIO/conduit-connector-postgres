// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package debezium

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// decode turns pgoutput's text form of a value into the record value the
// encoder's FromGo accepts for the column's schema. The replication session
// pins DateStyle=ISO, IntervalStyle=postgres, TimeZone=UTC,
// extra_float_digits=3 and bytea_output=hex, so the text forms below are the
// only ones that arrive.
//
// Invariant 6: nothing is rounded, truncated or coerced. A value the
// Debezium type cannot hold fails with CodeValueOutOfRange, as Debezium
// throws.
//
//nolint:gocyclo // one branch per kind
func (c *column) decode(text string) (any, error) {
	switch c.kind {
	case kindInt16:
		return parseInt(c, text, 16)
	case kindInt32:
		return parseInt(c, text, 32)
	case kindInt64:
		return parseInt(c, text, 64)
	case kindOID:
		u, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return nil, badText(c, text, err)
		}
		return int64(u), nil
	case kindFloat32:
		f, err := strconv.ParseFloat(text, 32)
		if err != nil {
			return nil, badText(c, text, err)
		}
		return float32(f), nil
	case kindFloat64:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, badText(c, text, err)
		}
		return f, nil
	case kindBool:
		switch text {
		case "t":
			return true, nil
		case "f":
			return false, nil
		}
		return nil, badText(c, text, nil)
	case kindString, kindXML, kindUUID, kindJSON, kindEnum:
		return text, nil
	case kindDecimal, kindVariableDecimal:
		if text == textNaN || strings.HasSuffix(text, "Infinity") {
			return nil, outOfRange(c, text, "decimal.handling.mode=precise cannot carry NaN or Infinity")
		}
		if _, ok := new(big.Rat).SetString(text); !ok {
			return nil, badText(c, text, nil)
		}
		return text, nil
	case kindMoney:
		return decodeMoney(c, text)
	case kindDate:
		return decodeDate(c, text)
	case kindTime, kindMicroTime:
		us, err := parseTimeOfDay(text)
		if err != nil {
			return nil, badText(c, text, err)
		}
		if c.kind == kindTime {
			return int32(us / 1000), nil //nolint:gosec // at most 86_400_000
		}
		return us, nil
	case kindTimeTZ:
		return decodeTimeTZ(c, text)
	case kindTimestamp, kindMicroTimestamp:
		t, err := parseTimestamp(text, false)
		if err != nil {
			return nil, timestampErr(c, text, err)
		}
		us, ok := unixMicros(t)
		if !ok {
			return nil, outOfRange(c, text, "outside the range of epoch microseconds")
		}
		if c.kind == kindTimestamp {
			return floorDiv(us, 1000), nil
		}
		return us, nil
	case kindTimestampTZ:
		t, err := parseTimestamp(text, true)
		if err != nil {
			return nil, timestampErr(c, text, err)
		}
		return formatZonedTimestamp(t), nil
	case kindInterval:
		return decodeInterval(c, text)
	case kindBytes:
		return decodeBytea(c, text)
	case kindHstore:
		return decodeHstore(c, text)
	case kindArray:
		return decodeArray(c, text)
	}
	return nil, unsupportedType(c.name, c.typeName)
}

// Postgres' text for special values.
const (
	textNaN         = "NaN"
	textInfinity    = "infinity"
	textNegInfinity = "-infinity"
)

func parseInt(c *column, text string, bits int) (any, error) {
	n, err := strconv.ParseInt(text, 10, bits)
	if err != nil {
		return nil, badText(c, text, err)
	}
	switch bits {
	case 16:
		return int16(n), nil //nolint:gosec // ParseInt checked the 16-bit range
	case 32:
		return int32(n), nil //nolint:gosec // ParseInt checked the 32-bit range
	}
	return n, nil
}

// decodeMoney parses money's text form ("$1,234.56", "-$5.00"; lc_monetary
// is pinned to C) into a decimal string at the column scale.
func decodeMoney(c *column, text string) (any, error) {
	var b strings.Builder
	for _, r := range text {
		switch {
		case r >= '0' && r <= '9', r == '.', r == '-':
			b.WriteRune(r)
		case r == '$', r == ',', r == ' ':
		case r == '(' || r == ')':
			b.WriteByte('-')
		default:
			return nil, badText(c, text, nil)
		}
	}
	s := b.String()
	if strings.Count(s, "-") > 1 {
		s = "-" + strings.ReplaceAll(s, "-", "")
	}
	if _, ok := new(big.Rat).SetString(s); !ok {
		return nil, badText(c, text, nil)
	}
	return rescale(c, text, s)
}

// rescale writes a plain decimal literal at the column's scale, as
// Debezium's setScale does for money (money.fraction.digits may differ from
// the two digits lc_monetary=C writes). Dropping a non-zero digit would
// change the value, so it fails instead (invariant 6).
func rescale(c *column, text, s string) (any, error) {
	ip, fp, _ := strings.Cut(s, ".")
	switch {
	case len(fp) < c.scale:
		fp += strings.Repeat("0", c.scale-len(fp))
	case len(fp) > c.scale:
		if strings.Trim(fp[c.scale:], "0") != "" {
			return nil, outOfRange(c, text, fmt.Sprintf("has more than debezium.money.fraction.digits=%d fractional digits", c.scale))
		}
		fp = fp[:c.scale]
	}
	if fp == "" {
		return ip, nil
	}
	return ip + "." + fp, nil
}

// decodeDate is io.debezium.time.Date: days since the epoch, proleptic
// Gregorian (LocalDate.toEpochDay).
func decodeDate(c *column, text string) (any, error) {
	if text == textInfinity || text == textNegInfinity {
		return nil, outOfRange(c, text, "Debezium cannot represent an infinite date")
	}
	t, err := parseDatePart(text)
	if err != nil {
		return nil, badText(c, text, err)
	}
	// t.Unix, not t.Sub: a Duration saturates at about 292 years, which
	// turned 0001-01-01 into -106752 (caught by the golden test).
	days := floorDiv(t.Unix(), 86_400)
	if days < math.MinInt32 || days > math.MaxInt32 {
		return nil, outOfRange(c, text, "outside int32 days")
	}
	return int32(days), nil
}

// parseDatePart parses "YYYY-MM-DD" with an optional " BC" suffix. Year 1 BC
// is ISO year 0, as java.time counts.
func parseDatePart(text string) (time.Time, error) {
	bc := strings.HasSuffix(text, " BC")
	text = strings.TrimSuffix(text, " BC")
	parts := strings.SplitN(text, "-", 3)
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("not a date")
	}
	y, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	d, err3 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return time.Time{}, fmt.Errorf("not a date")
	}
	if bc {
		y = 1 - y
	}
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), nil
}

// parseTimeOfDay parses "HH:MM:SS[.ffffff]" into microseconds since
// midnight. "24:00:00" is 86_400_000_000.
func parseTimeOfDay(text string) (int64, error) {
	hms, frac, _ := strings.Cut(text, ".")
	parts := strings.Split(hms, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("not a time")
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	s, err3 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || err3 != nil || h > 24 || m > 59 || s > 60 {
		return 0, fmt.Errorf("not a time")
	}
	us := (int64(h)*3600+int64(m)*60+int64(s))*1_000_000 + fracMicros(frac)
	return us, nil
}

func fracMicros(frac string) int64 {
	if frac == "" {
		return 0
	}
	for len(frac) < 6 {
		frac += "0"
	}
	n, _ := strconv.ParseInt(frac[:6], 10, 64)
	return n
}

// splitOffset splits a trailing "+HH[:MM[:SS]]" or "-HH..." UTC offset.
func splitOffset(text string) (string, int, error) {
	i := strings.LastIndexAny(text, "+-")
	if i <= 0 {
		return "", 0, fmt.Errorf("no offset")
	}
	sign := 1
	if text[i] == '-' {
		sign = -1
	}
	parts := strings.Split(text[i+1:], ":")
	secs := 0
	for j, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || j > 2 {
			return "", 0, fmt.Errorf("bad offset")
		}
		secs += n * []int{3600, 60, 1}[j]
	}
	return text[:i], sign * secs, nil
}

// decodeTimeTZ is io.debezium.time.ZonedTime: the time converted to UTC,
// "HH:MM:SS[.f...]Z" with the fraction's trailing zeros removed (golden:
// 13:14:15.5+05:30 is "07:44:15.5Z").
func decodeTimeTZ(c *column, text string) (any, error) {
	local, off, err := splitOffset(text)
	if err != nil {
		return nil, badText(c, text, err)
	}
	us, err := parseTimeOfDay(local)
	if err != nil {
		return nil, badText(c, text, err)
	}
	const day = 86_400_000_000
	us -= int64(off) * 1_000_000
	us = ((us % day) + day) % day
	sec := us / 1_000_000
	out := fmt.Sprintf("%02d:%02d:%02d", sec/3600, sec/60%60, sec%60)
	if f := us % 1_000_000; f != 0 {
		out += strings.TrimRight(fmt.Sprintf(".%06d", f), "0")
	}
	return out + "Z", nil
}

// parseTimestamp parses "YYYY-MM-DD HH:MM:SS[.ffffff][+HH[:MM]][ BC]". With
// zoned, an offset is required and the result is in UTC; without, the
// wall-clock value is read as UTC (Debezium's no-shift rule).
func parseTimestamp(text string, zoned bool) (time.Time, error) {
	if text == textInfinity || text == textNegInfinity {
		return time.Time{}, errInfinite
	}
	bc := strings.HasSuffix(text, " BC")
	text = strings.TrimSuffix(text, " BC")
	datePart, rest, ok := strings.Cut(text, " ")
	if !ok {
		return time.Time{}, fmt.Errorf("not a timestamp")
	}
	off := 0
	if zoned {
		var err error
		if rest, off, err = splitOffset(rest); err != nil {
			return time.Time{}, err
		}
	}
	if bc {
		datePart += " BC"
	}
	d, err := parseDatePart(datePart)
	if err != nil {
		return time.Time{}, err
	}
	us, err := parseTimeOfDay(rest)
	if err != nil {
		return time.Time{}, err
	}
	return d.Add(time.Duration(us) * time.Microsecond).Add(-time.Duration(off) * time.Second), nil
}

var errInfinite = fmt.Errorf("infinite timestamp")

func timestampErr(c *column, text string, err error) error {
	if err == errInfinite { //nolint:errorlint // sentinel compared by identity
		return outOfRange(c, text, "Debezium cannot represent an infinite timestamp in this mode")
	}
	return badText(c, text, err)
}

// unixMicros returns t as microseconds since the epoch, false if it does not
// fit an int64.
func unixMicros(t time.Time) (int64, bool) {
	secs := t.Unix()
	if secs > math.MaxInt64/1_000_000-1 || secs < math.MinInt64/1_000_000+1 {
		return 0, false
	}
	return secs*1_000_000 + int64(t.Nanosecond()/1000), true
}

// formatZonedTimestamp is io.debezium.time.ZonedTimestamp as Debezium 2.7
// and 3.7 write it: UTC, always six fractional digits (golden: decision of
// 2026-10-15, "2024-03-15T08:11:12.500000Z").
func formatZonedTimestamp(t time.Time) string {
	t = t.UTC()
	y := t.Year()
	var year string
	switch {
	case y < 0:
		year = fmt.Sprintf("-%04d", -y)
	case y > 9999:
		year = fmt.Sprintf("+%d", y)
	default:
		year = fmt.Sprintf("%04d", y)
	}
	return fmt.Sprintf("%s-%02d-%02dT%02d:%02d:%02d.%06dZ", year, t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000)
}

// decodeInterval is io.debezium.time.MicroDuration under
// interval.handling.mode=numeric: a port of MicroDuration.durationMicros
// with pgjdbc's PGInterval fields, in double arithmetic, cast to long as Java
// does (saturating). Text is IntervalStyle=postgres:
// "1 year 2 mons 3 days 04:05:06.789", "-1 days -00:00:01".
func decodeInterval(c *column, text string) (any, error) {
	var years, months, days, hours, minutes int64
	var seconds float64
	fields := strings.Fields(text)
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if strings.Contains(f, ":") {
			neg := strings.HasPrefix(f, "-")
			f = strings.TrimLeft(f, "+-")
			parts := strings.Split(f, ":")
			if len(parts) != 3 {
				return nil, badText(c, text, nil)
			}
			h, err1 := strconv.ParseInt(parts[0], 10, 64)
			m, err2 := strconv.ParseInt(parts[1], 10, 64)
			whole, frac, _ := strings.Cut(parts[2], ".")
			s, err3 := strconv.ParseInt(whole, 10, 64)
			if err1 != nil || err2 != nil || err3 != nil {
				return nil, badText(c, text, nil)
			}
			// pgjdbc: getSeconds() = wholeSeconds + microSeconds / 1e6.
			sec := float64(s) + float64(fracMicros(frac))/1e6
			if neg {
				h, m, sec = -h, -m, -sec
			}
			hours, minutes, seconds = h, m, sec
			continue
		}
		if i+1 >= len(fields) {
			return nil, badText(c, text, nil)
		}
		n, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return nil, badText(c, text, err)
		}
		switch strings.TrimSuffix(fields[i+1], "s") {
		case "year":
			years = n
		case "mon":
			months = n
		case "day":
			days = n
		default:
			return nil, badText(c, text, nil)
		}
		i++
	}
	// MicroDuration.durationMicros: ((years*12)+months) is int arithmetic in
	// Java (int32), the rest double.
	ym := int32(years)*12 + int32(months) // int arithmetic, as Java does
	numberOfDays := float64(ym)*daysPerMonthAvg + float64(days)
	numberOfSeconds := (((numberOfDays*24+float64(hours))*60)+float64(minutes))*60 + seconds
	return javaLong(numberOfSeconds * 1e6), nil
}

const daysPerMonthAvg = 365.25 / 12

// javaLong is Java's (long) cast of a double: NaN is 0, out-of-range values
// saturate.
func javaLong(f float64) int64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= math.MaxInt64:
		return math.MaxInt64
	case f <= math.MinInt64:
		return math.MinInt64
	}
	return int64(f)
}

func decodeBytea(c *column, text string) (any, error) {
	if !strings.HasPrefix(text, `\x`) {
		return nil, badText(c, text, fmt.Errorf("bytea_output must be hex"))
	}
	b, err := hex.DecodeString(text[2:])
	if err != nil {
		return nil, badText(c, text, err)
	}
	return b, nil
}

// decodeHstore is hstore.handling.mode=json: the map as a JSON object.
func decodeHstore(c *column, text string) (any, error) {
	m, keys, err := parseHstore(text)
	if err != nil {
		return nil, badText(c, text, err)
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		if v := m[k]; v == nil {
			b.WriteString("null")
		} else {
			vb, _ := json.Marshal(*v)
			b.Write(vb)
		}
	}
	b.WriteByte('}')
	return b.String(), nil
}

// parseHstore parses hstore's text form: "k"=>"v", "k2"=>NULL.
func parseHstore(text string) (map[string]*string, []string, error) {
	m := map[string]*string{}
	var keys []string
	i := 0
	skipSpace := func() {
		for i < len(text) && (text[i] == ' ' || text[i] == ',') {
			i++
		}
	}
	quoted := func() (string, error) {
		if i >= len(text) || text[i] != '"' {
			return "", fmt.Errorf("expected a quote at %d", i)
		}
		i++
		var b strings.Builder
		for i < len(text) {
			ch := text[i]
			if ch == '\\' && i+1 < len(text) {
				b.WriteByte(text[i+1])
				i += 2
				continue
			}
			if ch == '"' {
				i++
				return b.String(), nil
			}
			b.WriteByte(ch)
			i++
		}
		return "", fmt.Errorf("unterminated quote")
	}
	for {
		skipSpace()
		if i >= len(text) {
			return m, keys, nil
		}
		k, err := quoted()
		if err != nil {
			return nil, nil, err
		}
		if !strings.HasPrefix(text[i:], "=>") {
			return nil, nil, fmt.Errorf("expected => at %d", i)
		}
		i += 2
		if strings.HasPrefix(text[i:], "NULL") {
			i += 4
			m[k] = nil
		} else {
			v, err := quoted()
			if err != nil {
				return nil, nil, err
			}
			m[k] = &v
		}
		keys = append(keys, k)
	}
}

// decodeArray parses a one-dimensional array literal ({1,2,NULL},
// {"a","b c"}) and decodes each element by the element column. A
// multi-dimensional array fails: Debezium's mapping is one-dimensional.
func decodeArray(c *column, text string) (any, error) {
	if !strings.HasPrefix(text, "{") || !strings.HasSuffix(text, "}") {
		return nil, badText(c, text, fmt.Errorf("not an array literal (or one with explicit bounds)"))
	}
	body := text[1 : len(text)-1]
	out := []any{}
	if body == "" {
		return out, nil
	}
	i := 0
	for i <= len(body) {
		if i < len(body) && body[i] == '{' {
			return nil, &Error{Code: CodeUnsupportedType,
				Message: fmt.Sprintf("column %q holds a multi-dimensional array, which Debezium's array mapping does not represent", c.name),
				Fix:     "Store one-dimensional arrays, or run the table in the default output mode."}
		}
		var elem string
		var isNull bool
		if i < len(body) && body[i] == '"' {
			i++
			var b strings.Builder
			for i < len(body) && body[i] != '"' {
				if body[i] == '\\' && i+1 < len(body) {
					i++
				}
				b.WriteByte(body[i])
				i++
			}
			i++ // closing quote
			elem = b.String()
		} else {
			j := strings.IndexByte(body[i:], ',')
			if j < 0 {
				j = len(body) - i
			}
			elem = body[i : i+j]
			i += j
			isNull = elem == "NULL"
		}
		if isNull {
			out = append(out, nil)
		} else {
			v, err := c.elem.decode(elem)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if i >= len(body) {
			break
		}
		if body[i] != ',' {
			return nil, badText(c, text, fmt.Errorf("unexpected %q", body[i]))
		}
		i++
	}
	return out, nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

func badText(c *column, text string, err error) error {
	msg := fmt.Sprintf("column %q (%s): cannot decode %q", c.name, c.typeName, truncate(text))
	if err != nil {
		msg += ": " + err.Error()
	}
	return &Error{Code: CodeValueOutOfRange, Message: msg,
		Fix: "This is a bug or an unsupported server setting; report it with the column type and the value's text form."}
}

func outOfRange(c *column, text, why string) error {
	return &Error{Code: CodeValueOutOfRange,
		Message: fmt.Sprintf("column %q (%s): value %q cannot be represented: %s", c.name, c.typeName, truncate(text), why),
		Fix:     "Debezium fails on this value too. Change the value, or run the table in the default output mode."}
}

func truncate(s string) string {
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}
