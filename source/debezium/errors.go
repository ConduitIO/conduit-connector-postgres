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
	"errors"
	"fmt"
)

// Code is a stable error code. The codes are shared with the debezium.encode
// processor (ConduitIO/conduit, pkg/plugin/processor/builtin/impl/debezium),
// so one failure has one code end to end.
type Code string

const (
	// CodeConfigInvalid: a debezium.* setting has a value this version does
	// not support, or the mode is enabled where it cannot work (a standalone
	// build, decision 18).
	CodeConfigInvalid Code = "debezium.config_invalid"
	// CodeUnsupportedType: a column type Debezium does not map, or one this
	// version does not map yet.
	CodeUnsupportedType Code = "debezium.unsupported_type"
	// CodeValueOutOfRange: a value the Debezium type cannot hold (NaN in
	// precise decimals, infinite dates, out-of-range timestamps).
	CodeValueOutOfRange Code = "debezium.value_out_of_range"
	// CodeCatalogSkew: the catalog's columns disagree with the Relation
	// message's for the same column (a DDL raced the catalog read).
	CodeCatalogSkew Code = "debezium.catalog_skew"
	// CodeDeleteKeyUnavailable: a delete or primary-key change arrived
	// without the old key (the replica identity does not carry it).
	CodeDeleteKeyUnavailable Code = "debezium.delete_key_unavailable"
	// CodeReplicaIdentityIndexNotKey: the table's REPLICA IDENTITY USING
	// INDEX does not contain the record key, so old rows cannot carry it
	// (design D19).
	CodeReplicaIdentityIndexNotKey Code = "debezium.replica_identity_index_not_key"
)

// Error is a coded failure. Its message starts with the code, as every
// coded error in this connector does.
type Error struct {
	Code    Code
	Message string
	Fix     string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("%s: %s", e.Code, e.Message)
	if e.Fix != "" {
		s += ". Fix: " + e.Fix
	}
	return s
}

// CodeOf returns the code of the first *Error in err's chain, or "".
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
