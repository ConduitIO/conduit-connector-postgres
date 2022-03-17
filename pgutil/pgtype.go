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

package pgutil

import "github.com/jackc/pgtype"

// OIDToPgType returns the corresponding postgres type for the provided OID.
// Results from Postgres queries can be scanned into the returned value.
// Beware that most, but not all returned types implement the sql.Scanner and
// pgtype.Value interfaces, you need to do a type assertion to be sure,
// otherwise use pgtype.Unknown.
// If the oid is unknown pgtype.Unknown is returned.
func OIDToPgType(oid pgtype.OID) interface{} {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.ByteaOID:
		return &pgtype.Bytea{}
	case pgtype.QCharOID:
		return &pgtype.QChar{}
	case pgtype.NameOID:
		return &pgtype.Name{}
	case pgtype.Int8OID:
		return &pgtype.Int8{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
	case pgtype.Int4OID:
		return &pgtype.Int4{}
	case pgtype.TextOID:
		return &pgtype.Text{}
	case pgtype.OIDOID:
		var o pgtype.OID
		return &o
	case pgtype.TIDOID:
		return &pgtype.TID{}
	case pgtype.XIDOID:
		return &pgtype.XID{}
	case pgtype.CIDOID:
		return &pgtype.CID{}
	case pgtype.JSONOID:
		return &pgtype.JSON{}
	case pgtype.PointOID:
		return &pgtype.Point{}
	case pgtype.LsegOID:
		return &pgtype.Lseg{}
	case pgtype.PathOID:
		return &pgtype.Path{}
	case pgtype.BoxOID:
		return &pgtype.Box{}
	case pgtype.PolygonOID:
		return &pgtype.Polygon{}
	case pgtype.LineOID:
		return &pgtype.Line{}
	case pgtype.CIDROID:
		return &pgtype.CIDR{}
	case pgtype.CIDRArrayOID:
		return &pgtype.CIDRArray{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	case pgtype.CircleOID:
		return &pgtype.Circle{}
	case pgtype.UnknownOID:
		return &pgtype.Unknown{}
	case pgtype.MacaddrOID:
		return &pgtype.Macaddr{}
	case pgtype.InetOID:
		return &pgtype.Inet{}
	case pgtype.BoolArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.Int2ArrayOID:
		return &pgtype.Int2Array{}
	case pgtype.Int4ArrayOID:
		return &pgtype.Int4Array{}
	case pgtype.TextArrayOID:
		return &pgtype.TextArray{}
	case pgtype.ByteaArrayOID:
		return &pgtype.ByteaArray{}
	case pgtype.BPCharArrayOID:
		return &pgtype.BPCharArray{}
	case pgtype.VarcharArrayOID:
		return &pgtype.VarcharArray{}
	case pgtype.Int8ArrayOID:
		return &pgtype.Int8Array{}
	case pgtype.Float4ArrayOID:
		return &pgtype.Float4Array{}
	case pgtype.Float8ArrayOID:
		return &pgtype.Float8Array{}
	case pgtype.ACLItemOID:
		return &pgtype.ACLItem{}
	case pgtype.ACLItemArrayOID:
		return &pgtype.ACLItemArray{}
	case pgtype.InetArrayOID:
		return &pgtype.InetArray{}
	case pgtype.BPCharOID:
		return &pgtype.BPChar{}
	case pgtype.VarcharOID:
		return &pgtype.Varchar{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	case pgtype.TimeOID:
		return &pgtype.Time{}
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}
	case pgtype.TimestampArrayOID:
		return &pgtype.TimestampArray{}
	case pgtype.DateArrayOID:
		return &pgtype.DateArray{}
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}
	case pgtype.TimestamptzArrayOID:
		return &pgtype.TimestamptzArray{}
	case pgtype.IntervalOID:
		return &pgtype.Interval{}
	case pgtype.NumericArrayOID:
		return &pgtype.NumericArray{}
	case pgtype.BitOID:
		return &pgtype.Bit{}
	case pgtype.VarbitOID:
		return &pgtype.Varbit{}
	case pgtype.NumericOID:
		return &pgtype.Numeric{}
	case pgtype.RecordOID:
		return &pgtype.Record{}
	case pgtype.UUIDOID:
		return &pgtype.UUID{}
	case pgtype.UUIDArrayOID:
		return &pgtype.UUIDArray{}
	case pgtype.JSONBOID:
		return &pgtype.JSONB{}
	case pgtype.JSONBArrayOID:
		return &pgtype.JSONBArray{}
	case pgtype.DaterangeOID:
		return &pgtype.Daterange{}
	case pgtype.Int4rangeOID:
		return &pgtype.Int4range{}
	case pgtype.NumrangeOID:
		return &pgtype.Numrange{}
	case pgtype.TsrangeOID:
		return &pgtype.Tsrange{}
	case pgtype.TsrangeArrayOID:
		return &pgtype.TsrangeArray{}
	case pgtype.TstzrangeOID:
		return &pgtype.Tstzrange{}
	case pgtype.TstzrangeArrayOID:
		return &pgtype.TstzrangeArray{}
	case pgtype.Int8rangeOID:
		return &pgtype.Int8range{}
	default:
		return &pgtype.Unknown{}
	}
}
