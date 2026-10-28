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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

// TestSourceSpec_BatchingPinned guards the pin in NewSource(): the regenerated
// connector.yaml must show a zero default for sdk.batch.size and
// sdk.batch.delay.
//
// Since conduit-connector-sdk v0.18.0 the SDK default is 100 (adaptive drain).
// For this connector that would shrink the CDC batch from its internal 1000 to
// 100 (effectiveCDCBatchSize) and the snapshot FETCH from 50000 to 100. The
// runtime applies defaults from the embedded spec, not the struct tags, so the
// spec is what has to say 0. Design:
// docs/design-documents/20261027-default-source-batching.md in ConduitIO/conduit.
func TestSourceSpec_BatchingPinned(t *testing.T) {
	is := is.New(t)

	spec := Connector.NewSpecification()

	size := spec.SourceParams["sdk.batch.size"]
	delay := spec.SourceParams["sdk.batch.delay"]

	is.Equal(size.Default, "0") // sdk.batch.size must default to 0 (passthrough)

	// specgen renders a pinned zero duration as "0s" (the unpinned tag renders
	// "0"). Both parse to zero, which is what matters.
	d, err := time.ParseDuration(delay.Default)
	is.NoErr(err)
	is.Equal(d, time.Duration(0)) // sdk.batch.delay must default to 0

	// What the SDK actually applies to a config that sets neither parameter.
	applied := config.Config{}.ApplyDefaults(spec.SourceParams)
	is.Equal(applied["sdk.batch.size"], "0")
	d, err = time.ParseDuration(applied["sdk.batch.delay"])
	is.NoErr(err)
	is.Equal(d, time.Duration(0))
}
