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

package internal

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// keyHashSalt is random per process, so a logged hash cannot be reversed by
// hashing guessed keys (record keys are often low-entropy, like sequential
// ids) and cannot be matched across runs or between processes.
var keyHashSalt = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("cannot read random bytes for the log key hash: %v", err))
	}
	return b
}()

// KeyHash returns a short, salted digest of a record key for log lines. It
// lets two log lines about the same key in one process be correlated without
// writing the key itself to the log. Never log the key, only this.
func KeyHash(key map[string]any) string {
	names := make([]string, 0, len(key))
	for name := range key {
		names = append(names, name)
	}
	sort.Strings(names)

	mac := hmac.New(sha256.New, keyHashSalt)
	for _, name := range names {
		fmt.Fprintf(mac, "%q=%#v;", name, key[name])
	}
	return hex.EncodeToString(mac.Sum(nil)[:8])
}

// RowRef names a row in an error by the columns of its key and a salted hash
// of the key values, never by the values themselves. Errors reach the DLQ, the
// API and CLI output.
func RowRef(key map[string]any) string {
	names := make([]string, 0, len(key))
	for name := range key {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprintf("columns %s, key hash %s", strings.Join(names, ","), KeyHash(key))
}
