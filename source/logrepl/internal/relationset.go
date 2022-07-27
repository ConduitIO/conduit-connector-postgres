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

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

// RelationSet can be used to build a cache of relations returned by logical
// replication.
type RelationSet struct {
	relations map[pgtype.OID]*pglogrepl.RelationMessage
	connInfo  *pgtype.ConnInfo
}

// NewRelationSet creates a new relation set.
func NewRelationSet(ci *pgtype.ConnInfo) *RelationSet {
	return &RelationSet{
		relations: map[pgtype.OID]*pglogrepl.RelationMessage{},
		connInfo:  ci,
	}
}

func (rs *RelationSet) Add(r *pglogrepl.RelationMessage) {
	rs.relations[pgtype.OID(r.RelationID)] = r
}

func (rs *RelationSet) Get(id pgtype.OID) (*pglogrepl.RelationMessage, error) {
	msg, ok := rs.relations[id]
	if !ok {
		return nil, fmt.Errorf("no relation for %d", id)
	}
	return msg, nil
}

func (rs *RelationSet) Values(id pgtype.OID, row *pglogrepl.TupleData) (map[string]pgtype.Value, error) {
	if row == nil {
		return nil, errors.New("no tuple data")
	}

	rel, err := rs.Get(id)
	if err != nil {
		return nil, fmt.Errorf("no relation for %d", id)
	}

	values := map[string]pgtype.Value{}

	// assert same number of row and rel columns
	for i, tuple := range row.Columns {
		col := rel.Columns[i]
		decoder := rs.oidToDecoderValue(col.DataType)

		if err := decoder.DecodeText(rs.connInfo, tuple.Data); err != nil {
			return nil, fmt.Errorf("failed to decode tuple %d: %w", i, err)
		}

		values[col.Name] = decoder
	}

	return values, nil
}

type decoderValue interface {
	pgtype.Value
	pgtype.TextDecoder
}

func (rs *RelationSet) oidToDecoderValue(id uint32) decoderValue {
	dt, ok := rs.connInfo.DataTypeForOID(id)
	if !ok {
		return rs.oidToDecoderValue(pgtype.UnknownOID)
	}
	value := pgtype.NewValue(dt.Value)

	decoder, ok := value.(decoderValue)
	if !ok {
		return rs.oidToDecoderValue(pgtype.UnknownOID)
	}
	return decoder
}
