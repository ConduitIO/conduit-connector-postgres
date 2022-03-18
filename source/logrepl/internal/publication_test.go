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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/jackc/pgconn"
	"github.com/matryer/is"
)

func TestCreatePublication(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	conn := test.ConnectSimple(ctx, t, test.RepmgrConnString)

	pubNames := []string{"testpub", "123", "test-hyphen", "test=equal"}
	allTables := []bool{true, false}
	pubParams := [][]string{
		nil,
		{"publish = 'insert'"},
		{"publish = 'insert,update,delete'"},
	}

	for _, givenPubName := range pubNames {
		for _, givenAllTables := range allTables {
			for i, givenPubParams := range pubParams {
				testName := fmt.Sprintf("%s_%v_%d", givenPubName, givenAllTables, i)
				t.Run(testName, func(t *testing.T) {
					err := CreatePublication(
						ctx,
						conn.PgConn(),
						givenPubName,
						CreatePublicationOptions{
							AllTables:         givenAllTables,
							PublicationParams: givenPubParams,
						},
					)
					is.NoErr(err)
					// cleanup
					is.NoErr(DropPublication(ctx, conn.PgConn(), givenPubName, DropPublicationOptions{}))
				})
			}
		}
	}
}

func TestCreatePublicationForTables(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	pub := test.RandomIdentifier(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	tables := [][]string{
		nil,
		{},
		// there is a 0.3% chance this test will fail because test tables will
		// have clashing names, a risk we're willing to take
		{test.SetupTestTable(ctx, t, conn)},
		{test.SetupTestTable(ctx, t, conn), test.SetupTestTable(ctx, t, conn)},
	}

	for _, givenTables := range tables {
		testName := strings.Join(givenTables, ",")
		t.Run(testName, func(t *testing.T) {
			err := CreatePublication(
				ctx,
				conn.PgConn(),
				pub,
				CreatePublicationOptions{
					Tables: givenTables,
				},
			)
			is.NoErr(err)
			// cleanup
			is.NoErr(DropPublication(ctx, conn.PgConn(), pub, DropPublicationOptions{}))
		})
	}
}

func TestCreatePublicationForTablesAndAllTables(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	pub := test.RandomIdentifier(t)
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)

	err := CreatePublication(
		ctx,
		conn.PgConn(),
		pub,
		CreatePublicationOptions{
			Tables:    []string{"something"},
			AllTables: true,
		},
	)

	is.True(err != nil)
	_, ok := err.(*pgconn.PgError)
	is.True(!ok) // err shouldn't be a PgError, no statement should be even sent to Postgres
}

func TestCreatePublicationAllTables(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	pub := test.RandomIdentifier(t)

	// first connect with regular user
	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	err := CreatePublication(
		ctx,
		conn.PgConn(),
		pub,
		CreatePublicationOptions{
			AllTables: true,
		},
	)
	test.IsPgError(is, err, "42501")

	// next connect with repmgr
	conn = test.ConnectSimple(ctx, t, test.RepmgrConnString)
	err = CreatePublication(
		ctx,
		conn.PgConn(),
		pub,
		CreatePublicationOptions{
			AllTables: true,
		},
	)
	is.NoErr(err)
	// cleanup
	is.NoErr(DropPublication(ctx, conn.PgConn(), pub, DropPublicationOptions{}))
}

func TestDropPublication(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	pub := test.RandomIdentifier(t)

	conn := test.ConnectSimple(ctx, t, test.RegularConnString)
	err := DropPublication(
		ctx,
		conn.PgConn(),
		pub,
		DropPublicationOptions{
			IfExists: false, // fail if pub doesn't exist
		},
	)
	test.IsPgError(is, err, "42704")

	// next connect with repmgr
	err = DropPublication(
		ctx,
		conn.PgConn(),
		pub,
		DropPublicationOptions{
			IfExists: true, // fail if pub doesn't exist
		},
	)
	is.NoErr(err)
}
