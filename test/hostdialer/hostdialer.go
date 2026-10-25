// Copyright © 2024 Meroxa, Inc.
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

// Package hostdialer provides a recording, optionally refusing dialer and a
// counting listener for tests of the connector's host-dialer support.
package hostdialer

import (
	"context"
	"net"
	"sync"
	"testing"
)

// Dialer is a dialer a test puts on the context with
// dialer.WithDialContext. It records every address it is asked to dial. With
// Err set it refuses every call with that error and never touches the
// network. Without Err it forwards to Target (or to the requested address when
// Target is empty), so a test can use a made-up host name in the URL and still
// reach a real server.
type Dialer struct {
	// Err, when non-nil, is returned for every dial.
	Err error
	// Target, when set, replaces the requested address on forwarded dials.
	Target string

	mu    sync.Mutex
	addrs []string
}

// DialContext implements dialer.DialContextFunc. It is safe for concurrent use.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, network+" "+address)
	d.mu.Unlock()
	if d.Err != nil {
		return nil, d.Err
	}
	if d.Target != "" {
		address = d.Target
		network = "tcp"
	}
	var nd net.Dialer
	return nd.DialContext(ctx, network, address)
}

// Calls returns the "network address" pairs dialed so far.
func (d *Dialer) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

// Count returns how many dials were requested so far.
func (d *Dialer) Count() int { return len(d.Calls()) }

// CountingListener listens on a loopback port, closes every connection it
// accepts, and reports how many it accepted. It returns the host:port and a
// function with the accepted count. A test points a connector at it to show
// that a refused dial never reached the network.
func CountingListener(t *testing.T) (addr string, accepted func() int) {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	n := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close(); <-done })
	return l.Addr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}
