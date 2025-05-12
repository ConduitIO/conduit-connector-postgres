// Copyright © 2025 Meroxa, Inc.
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
	"sync"

	"github.com/conduitio/conduit-commons/opencdc"
)

type SharedQueue struct {
	lock    sync.Mutex
	data    []opencdc.Record
	emptied *sync.Cond
}

func NewSharedQueue(capacity int) *SharedQueue {
	return &SharedQueue{
		data:    make([]opencdc.Record, 0, capacity),
		emptied: sync.NewCond(&sync.Mutex{}),
	}
}

func (q *SharedQueue) Push(r opencdc.Record) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.data) == cap(q.data) {
		q.lock.Unlock()
		q.emptied.Wait()

		q.lock.Lock()
	}

	q.data = append(q.data, r)

	if len(q.data) == cap(q.data) {
		q.emptied.L.Lock()
		defer q.emptied.L.Unlock()
	}

}

func (q *SharedQueue) PopAll() []opencdc.Record {
	q.lock.Lock()
	defer q.lock.Unlock()

	d := q.data[:len(q.data)]
	q.data = make([]opencdc.Record, 0, cap(q.data))
	q.emptied.Signal()

	return d
}
