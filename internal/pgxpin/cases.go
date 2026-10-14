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

// Package pgxpin lists the Postgres types and values the pgx pin tests replay
// through the source and the destination, so that a pgx upgrade cannot change
// what the connector emits or writes unnoticed. It holds test data only.
package pgxpin

// Case is one column type with the values to insert. Every value becomes a
// row (id 1..n); one more row with NULL follows.
type Case struct {
	Name  string
	Typ   string
	Vals  []string
	Setup []string // DDL that must run before the table is created
	// Avro is true when source/schema can build an Avro schema for the type, so
	// that the Avro variants of the test cover it.
	Avro bool
	// TZ is true for types whose value depends on the host time zone, so that
	// the test repeats them under several time zones.
	TZ bool
	// Isolate runs the CDC case alone, because the stream fails on its values
	// (the error is what is pinned).
	Isolate bool
}

// Cases is the list of types the pin covers.
var Cases = []Case{
	// booleans, numbers
	{Name: "bool", Typ: "boolean", Vals: []string{"true", "false"}, Avro: true},
	{Name: "int2", Typ: "int2", Vals: []string{"-32768", "0", "32767"}, Avro: true},
	{Name: "int4", Typ: "int4", Vals: []string{"-2147483648", "0", "2147483647"}, Avro: true},
	{Name: "int8", Typ: "int8", Vals: []string{"-9223372036854775808", "0", "9223372036854775807"}, Avro: true},
	{Name: "oid", Typ: "oid", Vals: []string{"0", "4294967295"}},
	{Name: "xid", Typ: "xid", Vals: []string{"0", "4294967295"}},
	{Name: "xid8", Typ: "xid8", Vals: []string{"0", "18446744073709551615"}},
	{Name: "cid", Typ: "cid", Vals: []string{"0", "12"}},
	{Name: "float4", Typ: "float4", Vals: []string{"0", "-1.5", "3.4028235e38", "1e-45", "NaN", "Infinity", "-Infinity"}, Avro: true},
	{Name: "float8", Typ: "float8", Vals: []string{"0", "-1.5", "1.7976931348623157e308", "5e-324", "NaN", "Infinity", "-Infinity", "-0"}, Avro: true},
	{Name: "numeric", Typ: "numeric", Vals: []string{"0", "1", "-1.5", "123456789012345678901234567890.123456789", "0.000000000000000000001", "1e100", "1e1000", "-1e-1000", "12345678901234567890.1234567890123456789012345", "10000.0000", "100000", "0.5"}, Avro: true},
	{Name: "numeric_10_2", Typ: "numeric(10,2)", Vals: []string{"0", "12345678.90", "-0.01"}, Avro: true},
	{Name: "numeric_special", Typ: "numeric", Vals: []string{"NaN"}},
	{Name: "numeric_inf", Typ: "numeric", Vals: []string{"Infinity"}},
	{Name: "money", Typ: "money", Vals: []string{"0", "$1,234.56", "-$0.01"}},
	// text
	{Name: "text", Typ: "text", Vals: []string{"", "foo", "multi\nline\ttab", "héllo wörld ✓", "quote's \"dq\" back\\slash"}, Avro: true},
	{Name: "varchar", Typ: "varchar(20)", Vals: []string{"", "abc", "héllo"}, Avro: true},
	{Name: "bpchar", Typ: "char(5)", Vals: []string{"a", "abcde"}},
	{Name: "name", Typ: "name", Vals: []string{"relname"}},
	{Name: "qchar", Typ: `"char"`, Vals: []string{"a", "z"}},
	{Name: "bytea", Typ: "bytea", Vals: []string{`\x`, `\x00ff10`, `\xdeadbeef`}, Avro: true},
	{Name: "uuid", Typ: "uuid", Vals: []string{"00000000-0000-0000-0000-000000000000", "bd94ee0b-564f-4088-bf4e-8d5e626caf66"}, Avro: true},
	{Name: "json", Typ: "json", Vals: []string{`{"foo": "bar"}`, `[1, 2.5, "x", null, true]`, `"str"`, `12345678901234567890`, `{"a": {"b": [1, {"c": null}]}}`}, Avro: true},
	{Name: "jsonb", Typ: "jsonb", Vals: []string{`{"foo": "bar"}`, `[1, 2.5, "x", null, true]`, `"str"`, `12345678901234567890.5`, `{"a": {"b": [1, {"c": null}]}, "é": "✓"}`, `{}`, `null`}, Avro: true},
	{Name: "xml", Typ: "xml", Vals: []string{"<foo>bar</foo>", "<a x=\"1\"><b/></a>"}},
	{Name: "bit", Typ: "bit(8)", Vals: []string{"00000001", "11111111"}},
	{Name: "varbit", Typ: "varbit(16)", Vals: []string{"", "1", "1010101010101010"}},
	// dates and times
	{Name: "date", Typ: "date", Vals: []string{"2022-03-14", "1970-01-01", "0001-01-01", "0001-01-01 BC", "9999-12-31", "infinity", "-infinity"}, Avro: true, TZ: true},
	{Name: "date_far", Typ: "date", Vals: []string{"5874897-12-31"}, Avro: true, TZ: true},
	{Name: "time", Typ: "time", Vals: []string{"00:00:00", "04:05:06.789", "23:59:59.999999", "24:00:00"}, TZ: true},
	{Name: "timetz", Typ: "timetz", Vals: []string{"04:05:06.789-08", "04:05:06+05:30", "00:00:00+00"}, TZ: true},
	{Name: "timestamp", Typ: "timestamp", Vals: []string{"2022-03-14 15:16:17", "1970-01-01 00:00:00", "2022-03-14 15:16:17.123456", "2022-03-13 02:30:00", "0001-01-01 00:00:00", "0044-03-15 12:00:00 BC", "9999-12-31 23:59:59.999999", "infinity", "-infinity"}, Avro: true, TZ: true},
	{Name: "timestamp_far", Typ: "timestamp", Vals: []string{"294276-12-31 23:59:59"}, Avro: true, TZ: true, Isolate: true},
	{Name: "timestamptz", Typ: "timestamptz", Vals: []string{"2022-03-14 15:16:17-08", "2022-03-14 15:16:17.123456+05:30", "1970-01-01 00:00:00+00", "0001-01-01 00:00:00+00", "0044-03-15 12:00:00+00 BC", "9999-12-31 23:59:59.999999+00", "infinity", "-infinity"}, Avro: true, TZ: true},
	{Name: "interval", Typ: "interval", Vals: []string{"0", "18 seconds", "1 year 2 mons 3 days 04:05:06.789", "-1 mons -2 days -00:00:01.5", "178000000 years", "1 day 25:00:00", "00:00:00.000001"}},
	// network
	{Name: "inet", Typ: "inet", Vals: []string{"192.168.0.17", "192.168.0.17/24", "::1", "2001:db8::/32"}},
	{Name: "cidr", Typ: "cidr", Vals: []string{"192.168.100.128/25", "2001:db8::/32"}},
	{Name: "macaddr", Typ: "macaddr", Vals: []string{"08:00:2b:01:02:26"}},
	{Name: "macaddr8", Typ: "macaddr8", Vals: []string{"08:00:2b:01:02:03:04:27"}},
	// geometry
	{Name: "point", Typ: "point", Vals: []string{"(38,39)", "(-1.5,2.25)"}},
	{Name: "line", Typ: "line", Vals: []string{"{19,20,21}"}},
	{Name: "lseg", Typ: "lseg", Vals: []string{"((22,23),(24,25))"}},
	{Name: "box", Typ: "box", Vals: []string{"(3,4),(5,6)"}},
	{Name: "path", Typ: "path", Vals: []string{"[(30,31),(32,33),(34,35)]", "((1,2),(3,4))"}},
	{Name: "polygon", Typ: "polygon", Vals: []string{"((40,41),(42,43),(44,45))"}},
	{Name: "circle", Typ: "circle", Vals: []string{"<(11,12),13>"}},
	// text search and misc
	{Name: "tsvector", Typ: "tsvector", Vals: []string{
		"a fat cat sat on a mat and ate a fat rat",
		"'a':1A,2B,3C,4D 'cat':3 'fat':2,11",
		"'it''s' 'back\\\\slash' 'x y'",
		"",
		"'só':1 'üñí':2 '日本語':3",
		"'a':65535 'b':1,1",
	}},
	{Name: "tsquery", Typ: "tsquery", Vals: []string{"fat & (rat | cat)", "'a' <-> 'b' & !c:*"}},
	{Name: "pg_lsn", Typ: "pg_lsn", Vals: []string{"0/0", "36/37", "FFFFFFFF/FFFFFFFF"}},
	{Name: "pg_snapshot", Typ: "pg_snapshot", Vals: []string{"10:20:10,14,15", "1:1:"}},
	{Name: "tid", Typ: "tid", Vals: []string{"(0,1)", "(4294967295,65535)"}},
	{Name: "regclass", Typ: "regclass", Vals: []string{"pg_class"}},
	// ranges
	{Name: "int4range", Typ: "int4range", Vals: []string{"[1,10)", "(,5]", "empty", "(,)"}},
	{Name: "int8range", Typ: "int8range", Vals: []string{"[1,9223372036854775806]"}},
	{Name: "numrange", Typ: "numrange", Vals: []string{"[1.5,2.5)", "(,)"}},
	{Name: "daterange", Typ: "daterange", Vals: []string{"[2022-01-01,2022-12-31)", "[2022-01-01,infinity)", "(-infinity,2022-01-01)", "empty"}, TZ: true},
	{Name: "tsrange", Typ: "tsrange", Vals: []string{"[2022-03-14 15:16:17,2022-03-15 00:00:00)", "(,2022-03-15 00:00:00]", "empty"}, TZ: true},
	{Name: "tstzrange", Typ: "tstzrange", Vals: []string{"[2022-03-14 15:16:17-08,2022-03-15 00:00:00+00)", "(,2022-03-15 00:00:00+05:30]", "empty"}, TZ: true},
	{Name: "int4multirange", Typ: "int4multirange", Vals: []string{"{[1,3),[5,10)}", "{}"}},
	{Name: "nummultirange", Typ: "nummultirange", Vals: []string{"{[1.5,2.5),[3,4]}"}},
	{Name: "datemultirange", Typ: "datemultirange", Vals: []string{"{[2022-01-01,2022-02-01),[2022-03-01,2022-04-01)}"}, TZ: true},
	{Name: "tsmultirange", Typ: "tsmultirange", Vals: []string{"{[2022-03-14 15:16:17,2022-03-15 00:00:00)}"}, TZ: true},
	{Name: "tstzmultirange", Typ: "tstzmultirange", Vals: []string{"{[2022-03-14 15:16:17-08,2022-03-15 00:00:00+00)}"}, TZ: true},
	// arrays
	{Name: "arr_bool", Typ: "boolean[]", Vals: []string{"{t,f,NULL}"}},
	{Name: "arr_int2", Typ: "int2[]", Vals: []string{"{1,-2,32767}"}},
	{Name: "arr_int4", Typ: "int4[]", Vals: []string{"{1,2,3}", "{}", "{NULL,1}", "{{1,2},{3,4}}", "[0:2]={1,2,3}"}},
	{Name: "arr_int8", Typ: "int8[]", Vals: []string{"{9223372036854775807,-1}"}},
	{Name: "arr_float4", Typ: "float4[]", Vals: []string{"{1.5,NaN,Infinity}"}},
	{Name: "arr_float8", Typ: "float8[]", Vals: []string{"{1.5,NaN,-Infinity}"}},
	{Name: "arr_numeric", Typ: "numeric[]", Vals: []string{"{1.5,2,NULL}"}},
	{Name: "arr_text", Typ: "text[]", Vals: []string{`{a,"b c","d\"e",NULL,""}`, "{}"}},
	{Name: "arr_varchar", Typ: "varchar(10)[]", Vals: []string{"{a,b}"}},
	{Name: "arr_bpchar", Typ: "char(3)[]", Vals: []string{"{a,bc}"}},
	{Name: "arr_bytea", Typ: "bytea[]", Vals: []string{`{"\\x00ff","\\xdead"}`}},
	{Name: "arr_uuid", Typ: "uuid[]", Vals: []string{"{bd94ee0b-564f-4088-bf4e-8d5e626caf66,00000000-0000-0000-0000-000000000000}"}},
	{Name: "arr_json", Typ: "json[]", Vals: []string{`{"{\"a\": 1}","[1,2]"}`}},
	{Name: "arr_jsonb", Typ: "jsonb[]", Vals: []string{`{"{\"a\": 1}","[1,2]"}`}},
	{Name: "arr_date", Typ: "date[]", Vals: []string{"{2022-03-14,infinity,NULL}"}, TZ: true},
	{Name: "arr_time", Typ: "time[]", Vals: []string{"{04:05:06.789,23:59:59}"}, TZ: true},
	{Name: "arr_timestamp", Typ: "timestamp[]", Vals: []string{"{\"2022-03-14 15:16:17\",infinity,NULL}"}, TZ: true},
	{Name: "arr_timestamptz", Typ: "timestamptz[]", Vals: []string{"{\"2022-03-14 15:16:17-08\",\"2022-03-14 15:16:17.5+05:30\",-infinity}"}, TZ: true},
	{Name: "arr_interval", Typ: "interval[]", Vals: []string{"{\"1 day\",\"2 mons 3 seconds\"}"}},
	{Name: "arr_inet", Typ: "inet[]", Vals: []string{"{192.168.0.17,::1}"}},
	{Name: "arr_cidr", Typ: "cidr[]", Vals: []string{"{192.168.100.128/25}"}},
	{Name: "arr_macaddr", Typ: "macaddr[]", Vals: []string{"{08:00:2b:01:02:26}"}},
	{Name: "arr_point", Typ: "point[]", Vals: []string{`{"(1,2)","(3,4)"}`}},
	{Name: "arr_tsvector", Typ: "tsvector[]", Vals: []string{`{"'a':1 'b':2","'c'"}`, `{NULL,"'x'"}`, `{{'a','b'},{'c','d'}}`, `[0:1]={'a','b'}`, `{}`}},
	{Name: "arr_tsquery", Typ: "tsquery[]", Vals: []string{`{"a & b"}`}},
	{Name: "arr_int4range", Typ: "int4range[]", Vals: []string{`{"[1,3)",empty}`}},
	{Name: "arr_tstzrange", Typ: "tstzrange[]", Vals: []string{`{"[2022-03-14 15:16:17-08,2022-03-15 00:00:00+00)"}`}, TZ: true},
	{Name: "arr_oid", Typ: "oid[]", Vals: []string{"{1,4294967295}"}},
	{Name: "arr_bit", Typ: "bit(3)[]", Vals: []string{"{101,010}"}},
	{Name: "arr_xml", Typ: "xml[]", Vals: []string{`{"<a/>"}`}},
	{Name: "arr_money", Typ: "money[]", Vals: []string{`{"$1.00"}`}},
	// extension types (unknown to pgx)
	{Name: "hstore", Typ: "hstore", Setup: []string{"CREATE EXTENSION IF NOT EXISTS hstore"}, Vals: []string{`"a"=>"1", "b"=>NULL`, ``}},
	{Name: "citext", Typ: "citext", Setup: []string{"CREATE EXTENSION IF NOT EXISTS citext"}, Vals: []string{"Hello", ""}},
	{Name: "ltree", Typ: "ltree", Setup: []string{"CREATE EXTENSION IF NOT EXISTS ltree"}, Vals: []string{"Top.Science.Astronomy"}},
	// user defined types
	{
		Name: "enum", Typ: "pgxpin_enum",
		Setup: []string{
			"DROP TYPE IF EXISTS pgxpin_enum CASCADE",
			"CREATE TYPE pgxpin_enum AS ENUM ('sad', 'ok', 'happy')",
		},
		Vals: []string{"sad", "happy"},
	},
	{
		Name: "arr_enum", Typ: "pgxpin_enum2[]",
		Setup: []string{
			"DROP TYPE IF EXISTS pgxpin_enum2 CASCADE",
			"CREATE TYPE pgxpin_enum2 AS ENUM ('sad', 'ok', 'happy')",
		},
		Vals: []string{"{sad,happy}"},
	},
	{
		Name: "domain_int", Typ: "pgxpin_posint",
		Setup: []string{
			"DROP DOMAIN IF EXISTS pgxpin_posint CASCADE",
			"CREATE DOMAIN pgxpin_posint AS int4 CHECK (VALUE > 0)",
		},
		Vals: []string{"1", "2147483647"},
	},
	{
		Name: "domain_text", Typ: "pgxpin_label",
		Setup: []string{
			"DROP DOMAIN IF EXISTS pgxpin_label CASCADE",
			"CREATE DOMAIN pgxpin_label AS text",
		},
		Vals: []string{"x"},
	},
	{
		Name: "composite", Typ: "pgxpin_comp",
		Setup: []string{
			"DROP TYPE IF EXISTS pgxpin_comp CASCADE",
			"CREATE TYPE pgxpin_comp AS (a int4, b text, c timestamptz)",
		},
		Vals: []string{`(1,"x y",)`, `(2,z,"2022-03-14 15:16:17-08")`},
		TZ:   true,
	},
	{
		Name: "custom_range", Typ: "pgxpin_floatrange",
		Setup: []string{
			"DROP TYPE IF EXISTS pgxpin_floatrange CASCADE",
			"CREATE TYPE pgxpin_floatrange AS RANGE (subtype = float8)",
		},
		Vals: []string{"[1.5,2.5)"},
	},
}

// Zones are the host time zones the tz cases are replayed under. The Local
// zone is what pgx and the Avro library use to materialise time.Time values.
var Zones = []string{"UTC", "America/Los_Angeles", "Asia/Kolkata"}
