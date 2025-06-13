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
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-postgres/test"
	"github.com/matryer/is"
)

func TestCreatePublication(t *testing.T) {
	ctx := test.Context(t)
	pool := test.ConnectPool(ctx, t, test.RegularConnString)

	pubNames := []string{"testpub", "123", "test-hyphen", "test:semicolon", "test.dot", "test=equal"}
	pubParams := [][]string{
		nil,
		{"publish = 'insert'"},
		{"publish = 'insert,update,delete'"},
	}

	tables := []string{
		test.SetupTable(ctx, t, pool),
		test.SetupTable(ctx, t, pool),
	}

	for _, givenPubName := range pubNames {
		for i, givenPubParams := range pubParams {
			testName := fmt.Sprintf("%s_%d", givenPubName, i)
			t.Run(testName, func(t *testing.T) {
				is := is.New(t)
				err := CreatePublication(
					ctx,
					pool,
					givenPubName,
					CreatePublicationOptions{
						Tables:            tables,
						PublicationParams: givenPubParams,
					},
				)
				is.NoErr(err)
				// cleanup
				is.NoErr(DropPublication(ctx, pool, givenPubName, DropPublicationOptions{}))
			})
		}
	}

	// Without tables
	t.Run("fails without tables", func(t *testing.T) {
		is := is.New(t)

		err := CreatePublication(ctx, nil, "testpub", CreatePublicationOptions{})
		is.Equal(err.Error(), `publication "testpub" requires at least one table`)
	})
}

func TestCreatePublicationForTables(t *testing.T) {
	ctx := test.Context(t)
	pub := test.RandomIdentifier(t)
	pool := test.ConnectPool(ctx, t, test.RegularConnString)

	tables := [][]string{
		{test.SetupTable(ctx, t, pool)},
		{test.SetupTable(ctx, t, pool), test.SetupTable(ctx, t, pool)},
	}

	for _, givenTables := range tables {
		testName := strings.Join(givenTables, ",")
		t.Run(testName, func(t *testing.T) {
			is := is.New(t)
			err := CreatePublication(
				ctx,
				pool,
				pub,
				CreatePublicationOptions{
					Tables: givenTables,
				},
			)
			is.NoErr(err)
			// cleanup
			is.NoErr(DropPublication(ctx, pool, pub, DropPublicationOptions{}))
		})
	}
}

func TestDropPublication(t *testing.T) {
	ctx := test.Context(t)
	is := is.New(t)
	pub := test.RandomIdentifier(t)

	pool := test.ConnectPool(ctx, t, test.RegularConnString)
	err := DropPublication(
		ctx,
		pool,
		pub,
		DropPublicationOptions{
			IfExists: false, // fail if pub doesn't exist
		},
	)
	test.IsPgError(is, err, "42704")

	// next connect with repmgr
	err = DropPublication(
		ctx,
		pool,
		pub,
		DropPublicationOptions{
			IfExists: true, // fail if pub doesn't exist
		},
	)
	is.NoErr(err)
}
