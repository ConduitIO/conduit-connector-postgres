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

package types

import "github.com/jackc/pgx/v5/pgtype"

// RegisterTextOnly makes m decode the types that pgx decodes differently
// depending on its version as the text Postgres prints for them.
//
// pgx 5.9 registers tsvector and tsvector[] and decodes them into
// pgtype.TSVector, a struct. Before 5.9 pgx did not know the types, so the
// connector emitted the text form ('a':1 'cat':3), and an array as the array
// literal ({"'a':1 'b':2",'c'}). This connector line (v0.14.x) keeps emitting
// the text form on every pgx, so that a pgx upgrade does not change records.
//
// The text comes from Postgres itself, not from a formatter in this repo, so it
// is exact for every value, including arrays with several dimensions or a
// non-default lower bound, which pgx flattens when it decodes them.
//
// Call it on every pgtype.Map the connector decodes values with: the maps of
// the pooled connections (cpool) and the one of the logical replication
// stream (logrepl/internal.RelationSet).
func RegisterTextOnly(m *pgtype.Map) {
	m.RegisterType(&pgtype.Type{Name: "tsvector", OID: pgtype.TSVectorOID, Codec: pgtype.TextCodec{}})
	m.RegisterType(&pgtype.Type{Name: "_tsvector", OID: pgtype.TSVectorArrayOID, Codec: pgtype.TextCodec{}})
}
