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

package main

import (
	"context"
	"strings"
	"testing"

	postgres "github.com/conduitio/conduit-connector-postgres"
	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestRun_StandaloneRefusesDebeziumMode pins decision 18 at the place it
// takes effect: the standalone binary's entry point marks the process before
// serving, so a source configured with debezium.enabled fails validation
// with debezium.config_invalid, while the default mode still validates.
func TestRun_StandaloneRefusesDebeziumMode(t *testing.T) {
	is := is.New(t)
	var served bool
	run(func(sdk.Connector) {
		served = true
		is.True(debezium.IsStandalone()) // marked before serving
	})
	is.True(served)

	ctx := context.Background()
	configure := func(extra map[string]string) error {
		cfg := map[string]string{
			"url": "postgres://127.0.0.1:1/db", "tables": "t", "snapshotMode": "never",
		}
		for k, v := range extra {
			cfg[k] = v
		}
		src := postgres.Connector.NewSource()
		if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), postgres.Connector.NewSpecification().SourceParams); err != nil {
			return err
		}
		return src.Config().Validate(ctx)
	}
	is.NoErr(configure(nil))
	err := configure(map[string]string{"debezium.enabled": "true", "debezium.topic.prefix": "dbz"})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "debezium.config_invalid"))
	is.True(strings.Contains(err.Error(), "standalone"))
}
