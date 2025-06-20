// Copyright © 2022 Meroxa, Inc.
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

package internal

import (
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-postgres/internal"
	"github.com/conduitio/conduit-connector-postgres/source/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// RelationSet can be used to build a cache of relations returned by logical
// replication.
type RelationSet struct {
	relations map[uint32]*pglogrepl.RelationMessage
	connInfo  *pgtype.Map
}

// NewRelationSet creates a new relation set.
func NewRelationSet() *RelationSet {
	return &RelationSet{
		relations: map[uint32]*pglogrepl.RelationMessage{},
		connInfo:  pgtype.NewMap(),
	}
}

func (rs *RelationSet) Add(r *pglogrepl.RelationMessage) {
	rs.relations[r.RelationID] = r
}

func (rs *RelationSet) Get(id uint32) (*pglogrepl.RelationMessage, error) {
	msg, ok := rs.relations[id]
	if !ok {
		return nil, fmt.Errorf("no relation for %d", id)
	}
	return msg, nil
}

func (rs *RelationSet) Values(id uint32, row *pglogrepl.TupleData, tableInfo *internal.TableInfo) (map[string]any, error) {
	if row == nil {
		return nil, errors.New("no tuple data")
	}

	rel, err := rs.Get(id)
	if err != nil {
		return nil, fmt.Errorf("no relation for %d", id)
	}

	values := map[string]any{}

	// assert same number of row and rel columns
	for i, tuple := range row.Columns {
		col := rel.Columns[i]
		v, decodeErr := rs.decodeValue(col, tableInfo.Columns[col.Name], tuple.Data)
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode value for column %q: %w", col.Name, err)
		}

		values[col.Name] = v
	}

	return values, nil
}

func (rs *RelationSet) oidToCodec(id uint32) pgtype.Codec {
	dt, ok := rs.connInfo.TypeForOID(id)
	if !ok {
		return rs.oidToCodec(pgtype.UnknownOID)
	}
	return dt.Codec
}

func (rs *RelationSet) decodeValue(col *pglogrepl.RelationMessageColumn, colInfo *internal.ColumnInfo, data []byte) (any, error) {
	decoder := rs.oidToCodec(col.DataType)
	// This workaround is due to an issue in pgx v5.7.1.
	// Namely, that version introduces an XML codec
	// (see: https://github.com/jackc/pgx/pull/2083/files#diff-8288d41e69f73d01a874b40de086684e5894da83a627e845e484b06d5e053a44).
	// The XML codec, however, always return nil when deserializing input bytes
	// (see: https://github.com/jackc/pgx/pull/2083#discussion_r1755768269).
	var val any
	var err error

	switch col.DataType {
	case pgtype.XMLOID, pgtype.XMLArrayOID, pgtype.JSONBOID, pgtype.JSONOID:
		val, err = decoder.DecodeDatabaseSQLValue(rs.connInfo, col.DataType, pgtype.TextFormatCode, data)
	default:
		val, err = decoder.DecodeValue(rs.connInfo, col.DataType, pgtype.TextFormatCode, data)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to decode value of pgtype %v: %w", col.DataType, err)
	}

	v, err := types.Format(col.DataType, val, colInfo.IsNotNull)
	if err != nil {
		return nil, fmt.Errorf("failed to format column %q type %T: %w", col.Name, val, err)
	}

	return v, nil
}
