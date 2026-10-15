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

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-protocol/pconnector/v2/fromproto"
	"github.com/conduitio/conduit-connector-protocol/pconnector/v2/toproto"
	connectorv2 "github.com/conduitio/conduit-connector-protocol/proto/connector/v2"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"google.golang.org/protobuf/proto"
)

// TestDriftMarker_SurvivesTransportAndIsRecognized is the source/destination
// contract test for the marker shape. It takes a marker from the real source
// path (a logrepl source that detects a real ALTER TABLE), carries it through
// everything between the source connector and Destination.Write
// (opencdc ToProto -> proto.Marshal -> proto.Unmarshal -> FromProto, then the
// connector-protocol destination Run request conversions), and asserts
// isDriftMarker still recognizes it and Write still treats it as a no-op.
//
// If the source ever changes the marker's shape (adds a key or payload,
// changes the operation or the metadata key), or the transport normalizes it
// into something isDriftMarker rejects, this test fails. Without it,
// isDriftMarker and emitDriftMarker could drift apart unnoticed and the
// marker would be written as an INSERT ... DEFAULT VALUES row.
func TestDriftMarker_SurvivesTransportAndIsRecognized(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	table := test.RandomIdentifier(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v text)`, table))
	is.NoErr(err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE %s`, table)) })

	slot, pub := "drift_contract_"+table, "drift_contract_"+table
	if len(slot) > 60 {
		slot, pub = slot[:60], pub[:60]
	}

	s := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
		"url":                     test.RepmgrConnString,
		"tables":                  table,
		"snapshotMode":            "never",
		"cdcMode":                 "logrepl",
		"logrepl.slotName":        slot,
		"logrepl.publicationName": pub,
	}, s.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(s.Open(ctx, nil))
	t.Cleanup(func() {
		_ = s.Teardown(context.Background())
		_ = logrepl.Cleanup(context.Background(), logrepl.CleanupConfig{
			URL: test.RepmgrConnString, SlotName: slot, PublicationName: pub,
		})
	})

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// A baseline change fixes the initial shape (pgoutput sends the relation
	// lazily); the first change after the ALTER then yields the marker.
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (1, 'baseline')`, table))
	is.NoErr(err)
	recs, err := s.ReadN(readCtx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	is.True(!isDriftMarker(recs[0])) // a normal change is not a marker

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra int`, table))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, v, extra) VALUES (2, 'after ddl', 1)`, table))
	is.NoErr(err)
	recs, err = s.ReadN(readCtx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	marker := recs[0]
	is.Equal(marker.Metadata[logrepl.MetadataSchemaDrift], "true") // got the real marker
	is.True(isDriftMarker(marker))                                 // as emitted
	is.True(hasMarkerPosition(marker))                             // with the source-only signal

	// opencdc record <-> proto, through the wire encoding.
	pb := &opencdcv1.Record{}
	is.NoErr(marker.ToProto(pb))
	wire, err := proto.Marshal(pb)
	is.NoErr(err)
	pb2 := &opencdcv1.Record{}
	is.NoErr(proto.Unmarshal(wire, pb2))
	var viaProto opencdc.Record
	is.NoErr(viaProto.FromProto(pb2))
	is.True(isDriftMarker(viaProto)) // after the opencdc proto round trip
	is.True(hasMarkerPosition(viaProto))

	// connector-protocol destination Run request, as Conduit sends it to a
	// destination plugin and the plugin's server decodes it.
	req, err := toproto.DestinationRunRequest(pconnector.DestinationRunRequest{
		Records: []opencdc.Record{marker},
	})
	is.NoErr(err)
	wire, err = proto.Marshal(req)
	is.NoErr(err)
	req2 := &connectorv2.Destination_Run_Request{}
	is.NoErr(proto.Unmarshal(wire, req2))
	got, err := fromproto.DestinationRunRequest(req2)
	is.NoErr(err)
	is.Equal(len(got.Records), 1)
	is.True(isDriftMarker(got.Records[0])) // after the destination transport
	is.True(hasMarkerPosition(got.Records[0]))

	// And the destination treats the decoded marker as an acked no-op.
	ctxD, d, tableName, dconn := openDriftTestDestination(t)
	got.Records[0].Metadata[opencdc.MetadataCollection] = tableName
	n, err := d.Write(ctxD, got.Records)
	is.NoErr(err)
	is.Equal(n, 1)
	var count int
	is.NoErr(dconn.QueryRow(ctxD, fmt.Sprintf(`SELECT count(*) FROM %q`, tableName)).Scan(&count))
	is.Equal(count, 4) // the seeded rows only
}
