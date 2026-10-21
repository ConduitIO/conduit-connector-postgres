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
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Slot identity beacons (#442, docs/design-documents/20261021-slot-identity.md).
//
// Postgres gives a replication slot no identity: a slot dropped and created
// again under the same name looks exactly like a healthy idle one. The
// connector therefore writes its own. A beacon is a logical decoding message
// (pg_logical_emit_message), alone in its own transaction, authenticated with
// the slot identity of the pipeline's positions. The subscription never lets
// the slot's confirmed_flush_lsn move past the last beacon it has received by
// advancing idle (see Subscription.Beacons and reportedPositions), so the slot
// always still decodes a beacon of that identity. A slot created after the
// pipeline stopped starts after every beacon and decodes none, which is how a
// replaced slot is told from the original (FindBeacon).
//
// The identity is a secret key. It lives only in the pipeline's positions
// (Conduit's position store), never in the database and never in a log. A
// beacon carries a nonce, the WAL insert position read before it was written,
// and HMAC-SHA256(key, slot, nonce, position). Any role may call
// pg_logical_emit_message (PUBLIC has EXECUTE by default), so the content
// must not be forgeable from what the database shows: without the key no
// valid MAC can be made, and a genuine beacon copied into a replaced slot is
// refused because its position is below that slot's confirmed_flush_lsn
// (verifyBeacon).

// BeaconPrefix is the logical decoding message prefix of a beacon. Other
// consumers of the database that decode messages see beacons under it.
const BeaconPrefix = "conduit.postgres.slot_identity"

// minBeaconWAL is how much WAL past the last received beacon the server must
// have written before the subscription emits another one to advance an idle
// slot. It keeps an idle database idle: a beacon makes the background writer
// log one xl_running_xacts record (56 bytes on Postgres 17), which must not
// trigger the next beacon. A TRUNCATE is about 850 bytes.
const minBeaconWAL = 512

// staleBeaconAfter is how long an emitted beacon may stay unreceived before
// the subscription stops waiting for it and may emit a new one. A beacon that
// arrives later is ignored, which is safe: only a received beacon raises the
// cap.
const staleBeaconAfter = 5 * time.Minute

// beaconEmitTimeout bounds one emission.
const beaconEmitTimeout = 30 * time.Second

// MinBeaconServerVersion is the first Postgres version whose pgoutput can
// send logical decoding messages (the "messages" option, Postgres 14).
const MinBeaconServerVersion = 140000

// NewSlotIdentity returns a new random slot identity: a 256-bit key, hex.
func NewSlotIdentity() string {
	return randomHex(32)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err)) // never fails on supported platforms
	}
	return hex.EncodeToString(b)
}

// IdentityFingerprint is what logs may show of a slot identity: the first 8
// hex digits of its SHA-256. It tells two identities apart and reveals
// nothing usable to forge a beacon.
func IdentityFingerprint(identity string) string {
	if identity == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])[:8]
}

// beaconMAC authenticates a beacon of slot, written after WAL position start,
// with the identity key.
func beaconMAC(key, slot, nonce string, start pglogrepl.LSN) []byte {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte("conduit.postgres.slot_identity/v2\x00" + slot + "\x00" + nonce + "\x00" + start.String()))
	return m.Sum(nil)
}

func beaconContent(key, slot, nonce string, start pglogrepl.LSN) string {
	return "v=2;n=" + nonce + ";l=" + start.String() + ";mac=" + hex.EncodeToString(beaconMAC(key, slot, nonce, start))
}

// verifyBeacon reports whether content is a beacon authenticated with key for
// slot, written after minStart. The MAC is compared in constant time.
//
// minStart is the slot's confirmed_flush_lsn. A genuine beacon that proves
// the slot was written after it: the subscription advances an idle slot at
// most to the start position of a received beacon (Beacons.Cap). A genuine
// beacon of the old slot that someone copied into a replaced slot carries a
// start position below the new slot's confirmed_flush_lsn, which is after
// every WAL position of the old slot's history, so it is refused.
func verifyBeacon(content []byte, key, slot string, minStart pglogrepl.LSN) bool {
	var nonce, start, mac string
	for _, field := range strings.Split(string(content), ";") {
		k, v, _ := strings.Cut(field, "=")
		switch k {
		case "v":
			if v != "2" {
				return false
			}
		case "n":
			nonce = v
		case "l":
			start = v
		case "mac":
			mac = v
		}
	}
	if nonce == "" || start == "" || mac == "" {
		return false
	}
	lsn, err := pglogrepl.ParseLSN(start)
	if err != nil || lsn < minStart {
		return false
	}
	got, err := hex.DecodeString(mac)
	if err != nil {
		return false
	}
	return hmac.Equal(got, beaconMAC(key, slot, nonce, lsn))
}

// ServerInfo is what decides whether this server can carry beacons.
type ServerInfo struct {
	// Version is server_version_num.
	Version int
	// InRecovery is pg_is_in_recovery(): a standby cannot write WAL.
	InRecovery bool
}

// BeaconsSupported reports whether beacons can be written and decoded.
func (s ServerInfo) BeaconsSupported() bool {
	return s.Version >= MinBeaconServerVersion && !s.InRecovery
}

// ReadServerInfo reads the server version and whether it is a standby.
func ReadServerInfo(ctx context.Context, pool *pgxpool.Pool) (ServerInfo, error) {
	var s ServerInfo
	err := pool.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int, pg_is_in_recovery()").Scan(&s.Version, &s.InRecovery)
	if err != nil {
		return ServerInfo{}, fmt.Errorf("failed to read the server version: %w", err)
	}
	return s, nil
}

// Peek limits for FindBeacon. A healthy slot holds a beacon right after its
// confirmed_flush_lsn (the subscription advances an idle slot only up to a
// beacon), so the first round almost always finds it; the rounds grow for a
// slot with a backlog of in-flight changes in front of the beacon.
const findBeaconFirstRound = 1024

// findBeaconMaxRows is the most changes FindBeacon reads from a slot; a
// variable for tests.
var findBeaconMaxRows = 1 << 24

// nextFindBeaconLimit is the change limit of the round after one with limit,
// clamped to maxRows.
func nextFindBeaconLimit(limit, maxRows int) int {
	if limit >= maxRows/8 {
		return maxRows
	}
	return limit * 8
}

// ErrBeaconSearchExhausted means FindBeacon read findBeaconMaxRows changes
// from the slot without reaching the end of the WAL, so it can say neither
// yes nor no.
var ErrBeaconSearchExhausted = errors.New("the slot holds too many changes to search for a beacon")

// FindBeacon reports whether the slot still decodes a beacon authenticated
// with the identity key and written after confirmed (the slot's
// confirmed_flush_lsn, see verifyBeacon). It peeks (never consumes) the slot
// from its confirmed_flush_lsn to the current end of the WAL, so the slot
// must not be in use. The connecting role needs the REPLICATION attribute,
// as for streaming.
//
// Postgres skips a transaction whose commit record starts before the slot's
// confirmed_flush_lsn, so a found beacon is one the slot would also stream,
// and its LSN (the message record's end, as Postgres reports it) is after
// confirmed_flush_lsn.
func FindBeacon(ctx context.Context, pool *pgxpool.Pool, slot, publication, identity string, confirmed pglogrepl.LSN) (bool, pglogrepl.LSN, error) {
	needle := []byte(BeaconPrefix + "\x00")
	maxRows := findBeaconMaxRows
	for limit := min(findBeaconFirstRound, maxRows); ; limit = nextFindBeaconLimit(limit, maxRows) {
		var rows int64
		var candidates [][]byte
		var lsns []string
		err := pool.QueryRow(ctx, `
			SELECT count(*),
			       coalesce(array_agg(data) FILTER (WHERE get_byte(data, 0) = 77 AND position($4::bytea IN data) > 0), '{}'),
			       coalesce(array_agg(lsn::text) FILTER (WHERE get_byte(data, 0) = 77 AND position($4::bytea IN data) > 0), '{}')
			FROM pg_logical_slot_peek_binary_changes($1, NULL, $2,
			     'proto_version', '1', 'publication_names', $3, 'messages', 'true')`,
			slot, limit, publication, needle).Scan(&rows, &candidates, &lsns)
		if err != nil {
			return false, 0, fmt.Errorf("failed to peek replication slot %q for its identity beacon: %w", slot, err)
		}
		for i, data := range candidates {
			if !isBeaconFor(data, identity, slot, confirmed) {
				continue
			}
			lsn, err := pglogrepl.ParseLSN(lsns[i])
			if err != nil {
				return false, 0, fmt.Errorf("failed to parse beacon LSN %q: %w", lsns[i], err)
			}
			return true, lsn, nil
		}
		if rows < int64(limit) {
			return false, 0, nil // read to the end of the WAL
		}
		if limit >= maxRows {
			return false, 0, fmt.Errorf("replication slot %q: %w (read %d changes)", slot, ErrBeaconSearchExhausted, rows)
		}
	}
}

// isBeaconFor reports whether a pgoutput message is a beacon of slot
// authenticated with the identity key and written after minStart.
func isBeaconFor(data []byte, identity, slot string, minStart pglogrepl.LSN) bool {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return false
	}
	m, ok := msg.(*pglogrepl.LogicalDecodingMessage)
	if !ok || m.Prefix != BeaconPrefix {
		return false
	}
	return verifyBeacon(m.Content, identity, slot, minStart)
}

// Beacons writes the beacons of one subscription and tracks the last one the
// stream delivered back.
//
// Invariant 3 (#442): Cap is the WAL insert position read just before the
// last received beacon was written, so the beacon's record starts at or after
// it. The subscription never advances an idle slot past Cap, so after a stop
// the slot's confirmed_flush_lsn is at or below the start of a beacon it will
// decode again (or at or below the saved position, see
// position.CoveredThrough). Cap only moves when the stream delivers the
// beacon: the walsender sends only flushed WAL, so the beacon cannot be lost
// in a server crash, and with synchronized_standby_slots a failover standby
// has it too.
//
// Emit and the background emission started by maybeRequest touch only the
// pending beacon, under mu. observe, maybeRequest and Cap run on the
// subscription's Run goroutine.
type Beacons struct {
	pool     *pgxpool.Pool
	slot     string
	identity string // secret: never logged
	// now is time.Now and emit is Emit; test seams.
	now  func() time.Time
	emit func(context.Context) error

	mu      sync.Mutex
	pending *pendingBeacon

	// Only on the Run goroutine.
	cap     pglogrepl.LSN
	lastEnd pglogrepl.LSN
}

type pendingBeacon struct {
	content string
	start   pglogrepl.LSN // WAL insert position read before the emission
	at      time.Time
}

// EmitMessageSignature is the pg_logical_emit_message variant beacons call on
// a server of the given version (server_version_num), as GRANT names it.
// Postgres 17 added the flush argument to the same function.
func EmitMessageSignature(serverVersion int) string {
	if serverVersion >= 170000 {
		return "pg_logical_emit_message(boolean, text, text, boolean)"
	}
	return "pg_logical_emit_message(boolean, text, text)"
}

// NewBeacons returns the beacon writer of slot for the identity key.
func NewBeacons(pool *pgxpool.Pool, slot, identity string) *Beacons {
	b := &Beacons{pool: pool, slot: slot, identity: identity, now: time.Now}
	b.emit = b.Emit
	return b
}

// SetIdentity replaces the identity key before the stream starts (a rotated
// identity, see logrepl.slotIdentityCheck). It must not race an emission.
func (b *Beacons) SetIdentity(identity string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.identity = identity
}

// EmitMessagePublic reports whether PUBLIC may call the
// pg_logical_emit_message variant beacons use, which lets any role that can
// connect write messages into the WAL. Beacons stay unforgeable either way
// (they are authenticated); revoking it is defence in depth.
func EmitMessagePublic(ctx context.Context, pool *pgxpool.Pool, serverVersion int) (bool, error) {
	var public bool
	err := pool.QueryRow(ctx, "SELECT has_function_privilege('public', $1, 'EXECUTE')",
		EmitMessageSignature(serverVersion)).Scan(&public)
	return public, err
}

// Emit writes a beacon and waits until it is durable. It fails if the role
// may not call pg_logical_emit_message.
//
// The beacon is a transactional message committed with synchronous_commit
// on, so it is flushed when Emit returns: a restart right after it (or a
// server crash) still finds it. A non-transactional message is not flushed
// before Postgres 17, and a peek or a stream reads only flushed WAL; a quick
// restart then missed the beacon and refused an intact slot (measured on
// Postgres 14 to 16). Postgres decodes the message at its transaction's
// commit, which is after the insert position read first, so the slot still
// decodes it while its confirmed_flush_lsn is at or below Cap.
func (b *Beacons) Emit(ctx context.Context) error {
	p := &pendingBeacon{at: b.now()}
	// The insert position is read first and the pending beacon recorded
	// before the emission, so a beacon the stream delivers back at once is
	// always recognised. The stream must deliver exactly this content (its
	// nonce included) to move the cap.
	if err := b.pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&p.start); err != nil {
		return fmt.Errorf("failed to read the WAL insert position: %w", err)
	}
	b.mu.Lock()
	p.content = beaconContent(b.identity, b.slot, randomHex(16), p.start)
	b.pending = p
	b.mu.Unlock()

	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit TO on"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, $2::text)", BeaconPrefix, p.content)
		return err
	})
	if err != nil {
		b.mu.Lock()
		if b.pending == p {
			b.pending = nil
		}
		b.mu.Unlock()
		return fmt.Errorf("failed to write the slot identity beacon (pg_logical_emit_message): %w", err)
	}
	return nil
}

// Cap is the highest flush position an idle advance may report.
func (b *Beacons) Cap() pglogrepl.LSN { return b.cap }

// observe handles a logical decoding message from the stream. It reports
// whether the message was the pending beacon, in which case Cap moves up to
// that beacon's start.
func (b *Beacons) observe(m *pglogrepl.LogicalDecodingMessage) bool {
	if m.Prefix != BeaconPrefix {
		return false
	}
	b.mu.Lock()
	p := b.pending
	if p == nil || string(m.Content) != p.content {
		b.mu.Unlock()
		return false
	}
	b.pending = nil
	b.mu.Unlock()
	if p.start > b.cap {
		b.cap = p.start
	}
	if m.LSN > b.lastEnd {
		b.lastEnd = m.LSN
	}
	return true
}

// maybeRequest starts writing a new beacon in the background when an idle
// slot could advance past Cap and no beacon is on its way, because either
//
//   - records were acked past Cap (walFlushed): without a new beacon the slot
//     would stay below the last acked transaction's commit, and a restart
//     would decode that transaction again (the change key skips its changes,
//     so nothing is delivered twice); or
//   - the server has written at least minBeaconWAL bytes since the last
//     received beacon, which keeps an idle database idle.
func (b *Beacons) maybeRequest(ctx context.Context, serverWALEnd, walFlushed pglogrepl.LSN) {
	if serverWALEnd <= b.cap {
		return
	}
	if walFlushed <= b.cap && serverWALEnd < b.lastEnd+minBeaconWAL {
		return
	}
	b.mu.Lock()
	if b.pending != nil && b.now().Sub(b.pending.at) < staleBeaconAfter {
		b.mu.Unlock()
		return
	}
	// A pending beacon this old is stale: stop waiting for it. The
	// placeholder keeps a second status update from starting another
	// emission while this one runs; Emit replaces it.
	b.pending = &pendingBeacon{at: b.now()}
	b.mu.Unlock()

	go func() {
		ectx, cancel := context.WithTimeout(ctx, beaconEmitTimeout)
		defer cancel()
		if err := b.emit(ectx); err != nil {
			b.mu.Lock()
			if b.pending != nil && b.pending.content == "" {
				b.pending = nil
			}
			b.mu.Unlock()
			if ctx.Err() == nil {
				sdk.Logger(ctx).Warn().Err(err).
					Msg("could not write a slot identity beacon; the idle replication slot is not advanced until one is written")
			}
		}
	}()
}

// noCap is the idle-advance cap when beacons are not in use.
const noCap = pglogrepl.LSN(math.MaxUint64)

// beaconCap returns the idle-advance cap of b, or no cap when b is nil.
func beaconCap(b *Beacons) pglogrepl.LSN {
	if b == nil {
		return noCap
	}
	return b.Cap()
}
