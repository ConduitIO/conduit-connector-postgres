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

//go:build !conduitchaos

package chaospoint

// Reach is a no-op in the default (released) build. It exists so production
// call sites can call chaospoint.Reach(name) unconditionally, without an
// #ifdef-style branch at each site, while the released binary — built with
// no -tags by both .goreleaser.yml and .github/workflows/publish.yml —
// contains no fault-injection behavior at all. The compiler inlines this
// away; it costs nothing at the call site.
//
// The parking implementation lives in chaospoint_on.go, gated by the
// conduitchaos build tag, and is never linked into a released artifact.
func Reach(_ string) {}
