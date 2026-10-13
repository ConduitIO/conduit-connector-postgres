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

package types_test

// goldenJSON is the Record payload JSON of goldenRow and goldenArrays. Every
// timestamp is the wall clock that was inserted, labeled Z; tstz_offset is
// 03:04:05.5+05:30 as the UTC instant.
const goldenJSON = `{"date":"2024-01-02T00:00:00Z","date_arr":["2024-01-02T00:00:00Z","2024-12-31T00:00:00Z"],"ts":"2024-01-02T03:04:05.123456Z","ts_arr":["2024-01-02T03:04:05Z",null,"2024-06-07T08:09:10.5Z"],"ts_gap_la":"2024-03-10T02:30:00Z","ts_gap_nz":"2024-09-29T02:30:00Z","tstz":"2024-01-02T03:04:05.123456Z","tstz_arr":["2024-01-02T03:04:05Z"],"tstz_offset":"2024-01-01T21:34:05.5Z","tsvector":"'a':1 'cat':3A,4B 'it''s' 'x\\\\y'","tsvector_arr":["'a':1 'b'"]}`

// goldenAvroHex is the Avro encoding of goldenRow (without the arrays, which
// the Avro schema cannot express). The ts* fields are local-timestamp-micros of
// the inserted wall clock: 03:04:05.123456 is 1704164645123456, in every zone.
const goldenAvroHex = "98b40280d6fd84b8fb860680e8f8b9a6d1890680e8c1d69bce910680d6fd84b8fb8606c0b9cbc2a4fa8606402761273a312027636174273a33412c344220276974272773272027785c5c7927"

const goldenTimestampJSON = `{"date":"2024-01-02T00:00:00Z","ts":"2024-01-02T03:04:05.123456Z","ts_gap_la":"2024-03-10T02:30:00Z","ts_gap_nz":"2024-09-29T02:30:00Z","tstz":"2024-01-02T03:04:05.123456Z","tstz_offset":"2024-01-01T21:34:05.5Z"}`

// Avro of ts, ts_gap_la, ts_gap_nz, tstz, tstz_offset and date. Same bytes in every zone.
const goldenTimestampAvroHex = "98b40280d6fd84b8fb860680e8f8b9a6d1890680e8c1d69bce910680d6fd84b8fb8606c0b9cbc2a4fa8606"
