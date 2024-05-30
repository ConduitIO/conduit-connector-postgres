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

func (rs *RelationSet) Values(id uint32, row *pglogrepl.TupleData) (map[string]any, error) {
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
		decoder := rs.oidToCodec(col.DataType)
		val, err := decoder.DecodeValue(rs.connInfo, col.DataType, pgtype.TextFormatCode, tuple.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tuple %d: %w", i, err)
		}

		switch t := val.(type) {
		case pgtype.Numeric:
			v, err := types.Numeric.Format(t)
			if err != nil {
				return nil, fmt.Errorf("failed to format numeric value: %w", err)
			}
			values[col.Name] = v
		default:
			values[col.Name] = val
		}
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
