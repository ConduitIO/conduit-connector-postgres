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

//go:build ignore

// Command proxy is a TLS re-terminating Postgres proxy for test/tls/run.sh.
//
// It accepts the client's SSLRequest, terminates TLS with its own certificate,
// opens a second TLS connection to the upstream server and then copies bytes
// both ways, so the SASL exchange passes through unchanged. Postgres therefore
// sees a different TLS channel than the client: SCRAM-SHA-256 works,
// SCRAM-SHA-256-PLUS (channel binding) fails. It is a test fixture and
// verifies nothing about the upstream certificate.
//
// Usage: go run test/tls/proxy.go -cert C -key K -up host:port [-listen addr]
// It prints "listening host:port" once it accepts connections.
package main

import (
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
)

// sslRequestCode is the protocol code of the SSLRequest startup message.
const sslRequestCode = 80877103

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	up := flag.String("up", "", "upstream Postgres host:port (must have ssl=on)")
	certFile := flag.String("cert", "", "proxy certificate (PEM)")
	keyFile := flag.String("key", "", "proxy private key (PEM)")
	flag.Parse()

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("listening %s\n", ln.Addr())

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer c.Close()
			if err := serve(c, *up, &tls.Config{Certificates: []tls.Certificate{cert}}); err != nil {
				log.Printf("connection from %s: %v", c.RemoteAddr(), err)
			}
		}()
	}
}

func serve(c net.Conn, up string, serverCfg *tls.Config) error {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return fmt.Errorf("read startup: %w", err)
	}
	if binary.BigEndian.Uint32(hdr[4:]) != sslRequestCode {
		return fmt.Errorf("client did not send an SSLRequest")
	}
	if _, err := c.Write([]byte("S")); err != nil {
		return err
	}
	client := tls.Server(c, serverCfg)
	if err := client.Handshake(); err != nil {
		return fmt.Errorf("client tls: %w", err)
	}

	u, err := net.Dial("tcp", up)
	if err != nil {
		return fmt.Errorf("dial upstream: %w", err)
	}
	defer u.Close()
	if _, err := u.Write(hdr); err != nil {
		return err
	}
	ans := make([]byte, 1)
	if _, err := io.ReadFull(u, ans); err != nil || ans[0] != 'S' {
		return fmt.Errorf("upstream refused TLS")
	}
	upstream := tls.Client(u, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test fixture
	if err := upstream.Handshake(); err != nil {
		return fmt.Errorf("upstream tls: %w", err)
	}

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
	return nil
}
