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

package main

import (
	postgres "github.com/conduitio/conduit-connector-postgres"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	run(sdk.Serve)
}

// run marks the process as a standalone plugin, then serves the connector.
// The mark must come first: it is what makes the source refuse the Debezium
// mode, which needs the built-in connector (decision 18).
func run(serve func(sdk.Connector)) {
	postgres.MarkStandalone()
	serve(postgres.Connector)
}
