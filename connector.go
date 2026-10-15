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

//go:generate conn-sdk-cli specgen

package postgres

import (
	_ "embed"

	"github.com/conduitio/conduit-connector-postgres/source/debezium"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

var version = "(devel)"

var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs, version),
	NewSource:        NewSource,
	NewDestination:   NewDestination,
}

// MarkStandalone records that this process runs the connector as a
// standalone plugin (cmd/connector). The source then refuses
// debezium.enabled with debezium.config_invalid: records crossing the plugin
// boundary are protobuf Structs, which turn int64 into float64 and bytes
// into base64 strings, so the Debezium mode's typed values would not survive
// (decision 18). The built-in connector never calls it.
func MarkStandalone() { debezium.MarkStandalone() }
