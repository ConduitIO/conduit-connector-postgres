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
	"sync/atomic"
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
// A beacon also certifies how far the slot may be from the pipeline's point of
// view: r, a WAL position before which this run needed nothing more (every
// change committed before it was delivered and acked when the beacon was
// requested, or, for the stamp written when a run opens the slot, the slot
// position the run checked or created). A slot whose confirmed_flush_lsn is
// at or below r loses nothing, whoever moved it there; a slot past r would
// skip changes the pipeline never delivered (#447 round-4 B3: another client
// consuming an idle slot). FindBeacon therefore accepts a slot only on a
// beacon written after its confirmed_flush_lsn whose r is at or past it, and
// the subscription never advances an idle slot past the r of the last beacon
// it received (Cap).
//
// The identity is a secret key. It lives only in the pipeline's positions
// (Conduit's position store), never in the database and never in a log. A
// beacon carries a nonce, the WAL insert position read before it was written
// (l), r, and HMAC-SHA256(key, slot, nonce, l, r). Any role may call
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

// beaconMAC authenticates a beacon of slot, written after WAL position start
// and certifying covered (r), with the identity key.
func beaconMAC(key, slot, nonce string, start, covered pglogrepl.LSN) []byte {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte("conduit.postgres.slot_identity/v3\x00" + slot + "\x00" + nonce + "\x00" + start.String() + "\x00" + covered.String()))
	return m.Sum(nil)
}

func beaconContent(key, slot, nonce string, start, covered pglogrepl.LSN) string {
	return "v=3;n=" + nonce + ";l=" + start.String() + ";r=" + covered.String() +
		";mac=" + hex.EncodeToString(beaconMAC(key, slot, nonce, start, covered))
}

// verifyBeacon reports whether content is a beacon authenticated with key for
// slot that vouches for a slot at confirmed (its confirmed_flush_lsn): one
// written after confirmed (l >= confirmed) that certifies at least confirmed
// (r >= confirmed). The MAC is compared in constant time.
//
// Invariant 3 (#442, #447 round-4 B3): r >= confirmed means every change
// that commits before confirmed was delivered and acked (or was never needed,
// see the stamp), so streaming from confirmed loses nothing, whoever moved
// the slot there; the subscription advances an idle slot at most to the r of
// a received beacon (Beacons.Cap), so its own slot always has one. A slot
// another client consumed or advanced past r is refused even though it is
// the pipeline's slot: the changes it skipped were never delivered. A genuine
// beacon of the old slot copied into a replaced slot is refused because its
// l is below the new slot's confirmed_flush_lsn, which is after every WAL
// position of the old slot's history.
func verifyBeacon(content []byte, key, slot string, confirmed pglogrepl.LSN) bool {
	var nonce, start, covered, mac string
	for _, field := range strings.Split(string(content), ";") {
		k, v, _ := strings.Cut(field, "=")
		switch k {
		case "v":
			if v != "3" {
				return false
			}
		case "n":
			nonce = v
		case "l":
			start = v
		case "r":
			covered = v
		case "mac":
			mac = v
		}
	}
	if nonce == "" || start == "" || covered == "" || mac == "" {
		return false
	}
	l, err := pglogrepl.ParseLSN(start)
	if err != nil {
		return false
	}
	// r <= l (Emit never certifies past the insert position it read), so
	// r >= confirmed also puts l at or past confirmed.
	r, err := pglogrepl.ParseLSN(covered)
	if err != nil || r > l {
		return false
	}
	// guard: logrepl-beacon-covers-confirmed tests=^TestBeaconContent_Verify$|^TestSlotIdentity_ConsumedByAnotherClientBelowTheLastBeacon_Refused$ pkgs=./source/logrepl
	if r < confirmed {
		return false
	}
	got, err := hex.DecodeString(mac)
	if err != nil {
		return false
	}
	return hmac.Equal(got, beaconMAC(key, slot, nonce, l, r))
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

// Peek rounds for FindBeacon. A healthy slot holds a beacon right after its
// confirmed_flush_lsn (the subscription advances an idle slot only up to a
// beacon), so the first round almost always finds it; the rounds grow for a
// slot with a backlog of in-flight changes in front of the beacon.
const findBeaconFirstRound = 1024

// findBeaconMaxRows is the largest change limit of a round; a variable for
// tests. A round's limit is not a bound: pg_logical_slot_peek_binary_changes
// stops only at a transaction boundary, so a round returns every change of a
// transaction that reaches its limit (#447 fresh-context review S1). After
// the round at findBeaconMaxRows the search reads the slot to the end of the
// WAL without a limit, so a large transaction in front of the beacon costs
// time (bounded by logrepl.slotIdentity.checkTimeout), never a wrong answer.
var findBeaconMaxRows = 1 << 24

// nextFindBeaconLimit is the change limit of the round after one with limit,
// clamped to maxRows.
func nextFindBeaconLimit(limit, maxRows int) int {
	if limit >= maxRows/8 {
		return maxRows
	}
	return limit * 8
}

// FindBeacon reports whether the slot still decodes a beacon authenticated
// with the identity key that vouches for confirmed (the slot's
// confirmed_flush_lsn): written after it, and certifying at least it (see
// verifyBeacon). It peeks (never consumes) the slot from its
// confirmed_flush_lsn to the current end of the WAL, so the slot must not be
// in use. The connecting role needs the REPLICATION attribute, as for
// streaming. It always gives an answer; only the context bounds it.
//
// Postgres skips a transaction whose commit record starts before the slot's
// confirmed_flush_lsn, so a found beacon is one the slot would also stream,
// and its LSN (the message record's end, as Postgres reports it) is after
// confirmed_flush_lsn.
func FindBeacon(ctx context.Context, pool *pgxpool.Pool, slot, publication, identity string, confirmed pglogrepl.LSN) (bool, pglogrepl.LSN, error) {
	maxRows := findBeaconMaxRows
	for limit := min(findBeaconFirstRound, maxRows); ; limit = nextFindBeaconLimit(limit, maxRows) {
		found, lsn, rows, err := findBeaconRound(ctx, pool, slot, publication, identity, confirmed, &limit)
		if err != nil || found {
			return found, lsn, err
		}
		if rows < int64(limit) {
			return false, 0, nil // read to the end of the WAL
		}
		if limit >= maxRows {
			// The last limited round was full: read the rest without a
			// limit. A limit would return a transaction larger than it
			// whole each time and never get past it.
			found, lsn, _, err := findBeaconRound(ctx, pool, slot, publication, identity, confirmed, nil)
			return found, lsn, err
		}
	}
}

// findBeaconRound is one peek of FindBeacon, with upto_nchanges limit (nil:
// to the end of the WAL). rows is how many changes it returned.
func findBeaconRound(ctx context.Context, pool *pgxpool.Pool, slot, publication, identity string, confirmed pglogrepl.LSN, limit *int) (bool, pglogrepl.LSN, int64, error) {
	needle := []byte(BeaconPrefix + "\x00")
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
		return false, 0, 0, fmt.Errorf("failed to peek replication slot %q for its identity beacon: %w", slot, err)
	}
	for i, data := range candidates {
		if !isBeaconFor(data, identity, slot, confirmed) {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsns[i])
		if err != nil {
			return false, 0, 0, fmt.Errorf("failed to parse beacon LSN %q: %w", lsns[i], err)
		}
		return true, lsn, rows, nil
	}
	return false, 0, rows, nil
}

// isBeaconFor reports whether a pgoutput message is a beacon of slot
// authenticated with the identity key that vouches for a slot at confirmed.
func isBeaconFor(data []byte, identity, slot string, confirmed pglogrepl.LSN) bool {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return false
	}
	m, ok := msg.(*pglogrepl.LogicalDecodingMessage)
	if !ok || m.Prefix != BeaconPrefix || !m.Transactional {
		return false // beacons are always transactional (Emit)
	}
	return verifyBeacon(m.Content, identity, slot, confirmed)
}

// Beacons writes the beacons of one subscription and tracks the last one the
// stream delivered back.
//
// Invariant 3 (#442): Cap is the r of the last received beacon (see
// verifyBeacon), which is at or below the WAL insert position read just
// before it was written, so the beacon's record starts at or after it. The
// subscription never advances an idle slot past Cap, so after a stop the
// slot's confirmed_flush_lsn is at or below the r of a beacon it will decode
// again (or at or below the saved position, see position.CoveredThrough). Cap only moves when the stream delivers the
// beacon: the walsender sends only flushed WAL, so the beacon cannot be lost
// in a server crash, and with synchronized_standby_slots a failover standby
// has it too.
//
// A beacon may only be written into the slot this run holds (Emit's guard):
// one written into a slot that replaced it would vouch for the replacement.
//
// Emit and the background emission started by maybeRequest touch the
// attempts and failure state under mu. observe, maybeRequest and Cap run on
// the subscription's Run goroutine; cap is atomic so tests may read it.
type Beacons struct {
	pool     *pgxpool.Pool
	slot     string
	identity string // secret: never logged
	// failureTimeout is how long beacons may keep failing while they hold
	// an idle slot back before the cap is dropped (see Abandoned).
	failureTimeout time.Duration
	// now is time.Now and emit is emitCovering; test seams.
	now  func() time.Time
	emit func(ctx context.Context, covered pglogrepl.LSN) error

	// streamPID and streamStart identify this run's walsender once
	// START_REPLICATION succeeded: its backend PID and its
	// pg_stat_activity.backend_start (a PID alone can be reused by another
	// backend). Zero before. preStreamLimit is the highest
	// confirmed_flush_lsn the slot may have before the stream holds it.
	streamPID      atomic.Uint32
	streamStart    atomic.Int64 // backend_start, Unix microseconds
	preStreamLimit atomic.Uint64

	mu sync.Mutex
	// inflight: an emission is running (since inflightAt).
	inflight   bool
	inflightAt time.Time
	// attempts are the beacons written or attempted by this run, by nonce,
	// most recent last. A beacon whose Emit returned an error can still have
	// committed (the error came after the commit), so the stream delivering
	// one of them moves the cap.
	attempts []pendingBeacon
	// failingSince is when emissions started failing in a row, zero while
	// the last one succeeded. abandoned is set once they failed for longer
	// than failureTimeout. probe is, while abandoned, the r of the latest
	// attempt requested since: the idle advance waits at it until the
	// stream delivers that beacon, so the beacon certifies the reported
	// flush and re-arms the identity (see observe).
	failingSince time.Time
	abandoned    atomic.Bool
	probe        pglogrepl.LSN

	// cap is the r of the last received beacon.
	cap atomic.Uint64
	// lastEnd is only on the Run goroutine.
	lastEnd pglogrepl.LSN
}

type pendingBeacon struct {
	nonce   string
	content string
	start   pglogrepl.LSN // l: WAL insert position read before the emission
	covered pglogrepl.LSN // r: what the beacon certifies (verifyBeacon)
}

// maxBeaconAttempts bounds Beacons.attempts.
const maxBeaconAttempts = 16

// DefaultBeaconFailureTimeout is Beacons.failureTimeout unless configured.
const DefaultBeaconFailureTimeout = 5 * time.Minute

// EmitMessageSignature is the pg_logical_emit_message variant beacons call on
// a server of the given version (server_version_num), as GRANT names it.
// Postgres 17 added the flush argument to the same function.
func EmitMessageSignature(serverVersion int) string {
	if serverVersion >= 170000 {
		return "pg_logical_emit_message(boolean, text, text, boolean)"
	}
	return "pg_logical_emit_message(boolean, text, text)"
}

// NewBeacons returns the beacon writer of slot for the identity key. Before
// the stream starts, the slot may be written to only while its
// confirmed_flush_lsn is at or below preStreamLimit (SetPreStreamLimit).
func NewBeacons(pool *pgxpool.Pool, slot, identity string) *Beacons {
	b := &Beacons{pool: pool, slot: slot, identity: identity, failureTimeout: DefaultBeaconFailureTimeout, now: time.Now}
	b.emit = b.emitCovering
	return b
}

// SetFailureTimeout sets how long beacon failures may hold an idle slot back
// (see Abandoned). Call before the stream starts.
func (b *Beacons) SetFailureTimeout(d time.Duration) {
	if d > 0 {
		b.failureTimeout = d
	}
}

// SetPreStreamLimit sets the highest confirmed_flush_lsn the slot may have
// for a beacon written before the stream holds it: what the identity check
// accepted, or the slot's position right after this run created it. It is
// also what the stamp (Emit) certifies: the run accepted the slot there, so
// it needs nothing from before it.
func (b *Beacons) SetPreStreamLimit(lsn pglogrepl.LSN) { b.preStreamLimit.Store(uint64(lsn)) }

// SetStreamHolder records this run's walsender, from then on the only holder
// a beacon may be written for: its backend PID and its backend_start.
func (b *Beacons) SetStreamHolder(pid uint32, backendStart time.Time) {
	b.streamStart.Store(backendStart.UnixMicro())
	b.streamPID.Store(pid)
}

// WalsenderStart returns pg_stat_activity.backend_start of the backend pid,
// which together with the PID identifies one walsender (PIDs are reused).
func WalsenderStart(ctx context.Context, pool *pgxpool.Pool, pid uint32) (time.Time, error) {
	var start time.Time
	err := pool.QueryRow(ctx, "SELECT backend_start FROM pg_stat_activity WHERE pid = $1", int64(pid)).Scan(&start)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read the walsender's backend_start (PID %d): %w", pid, err)
	}
	return start, nil
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

// ErrSlotNotHeld means the beacon was not written because the slot is not
// the one this run holds (or verified before the stream started).
var ErrSlotNotHeld = errors.New("the replication slot is not the one this run holds")

// ErrSlotInUse is ErrSlotNotHeld for a slot another connection streams from
// before this run's stream started: the stream start fails on it anyway.
var ErrSlotInUse = fmt.Errorf("%w: it is in use by another connection", ErrSlotNotHeld)

// AfterBeaconGuard and AfterBeaconCommit are test seams, nil in production
// (only assigned in _test.go files, see TestSeamsOnlyAssignedInTests).
// AfterBeaconGuard runs inside Emit's transaction after the guard passed and
// before the insert position is read; AfterBeaconCommit runs after the
// beacon committed, and an error it returns is returned by Emit.
var (
	AfterBeaconGuard  func()
	AfterBeaconCommit func() error
)

// Emit writes a beacon and waits until it is durable. It fails if the role
// may not call pg_logical_emit_message, and it refuses to write a beacon
// into a slot this run does not hold (ErrSlotNotHeld).
//
// The guard, all inside the beacon's own transaction:
//
//  1. The transaction takes an xid first (pg_current_xact_id).
//  2. It checks the slot: while the stream runs, active_pid must be this
//     run's walsender, with the backend_start recorded when the stream
//     started (a reused PID is another backend); before it, the slot must
//     be inactive with a confirmed_flush_lsn at or below the limit this run
//     verified or created it with.
//  3. Only then it reads the WAL insert position and writes the message.
//
// A slot that passes the check is this run's slot at that moment. A slot
// created after the check cannot decode the beacon: creating a logical slot
// waits for every transaction that holds an xid when it starts building its
// snapshot to finish, and its consistent point is after their commits, so a
// slot created while the beacon's transaction is open never decodes it
// (Postgres skips transactions that committed before a slot's consistent
// point; TestBeacons_SlotCreatedAfterTheGuardDoesNotDecodeTheBeacon). A slot
// created before the check either fails the check (another walsender, or a
// confirmed_flush_lsn past the limit, since a slot created later starts
// later) or is this run's slot.
//
// The transaction commits with synchronous_commit = local: flushed to the
// local WAL before Emit returns, so a restart or a server crash right after
// still finds it, but never waiting for a synchronous standby that is down.
// A failover standby needs no stronger commit: the cap moves only when the
// stream delivers the beacon, which with synchronized_standby_slots happens
// only after the standbys confirmed it. Postgres decodes the message at its
// transaction's commit, which is after the insert position read in step 3,
// so the slot still decodes it while its confirmed_flush_lsn is at or below
// Cap.
//
// Emit writes the stamp: it certifies the pre-stream limit (see
// SetPreStreamLimit and verifyBeacon). The beacons the subscription asks for
// while it streams certify more (maybeRequest).
func (b *Beacons) Emit(ctx context.Context) error {
	return b.emitCovering(ctx, pglogrepl.LSN(b.preStreamLimit.Load()))
}

// emitCovering is Emit for a beacon that certifies covered (r, see
// verifyBeacon). The caller guarantees it: every change that commits before
// covered was delivered and acked, or is not needed by this run.
func (b *Beacons) emitCovering(ctx context.Context, covered pglogrepl.LSN) error {
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit TO local"); err != nil {
			return err
		}
		var xidErr error
		// guard: logrepl-beacon-xid-first tests=^TestBeacons_SlotCreatedAfterTheGuardDoesNotDecodeTheBeacon$
		assignXid(ctx, tx, &xidErr)
		if xidErr != nil {
			return fmt.Errorf("failed to assign the beacon's transaction id: %w", xidErr)
		}
		if err := b.checkHeld(ctx, tx); err != nil {
			return err
		}
		if AfterBeaconGuard != nil {
			AfterBeaconGuard()
		}
		var start pglogrepl.LSN
		if err := tx.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&start); err != nil {
			return fmt.Errorf("failed to read the WAL insert position: %w", err)
		}
		p := b.record(start, minLSN(covered, start))
		_, err := tx.Exec(ctx, "SELECT pg_logical_emit_message(true, $1, $2::text)", BeaconPrefix, p.content)
		return err
	})
	if err == nil && AfterBeaconCommit != nil {
		err = AfterBeaconCommit()
	}
	if err != nil {
		if errors.Is(err, ErrSlotNotHeld) {
			return err
		}
		return fmt.Errorf("failed to write the slot identity beacon (pg_logical_emit_message): %w", err)
	}
	return nil
}

// assignXid is Emit's step 1: the beacon's transaction takes an xid before
// the guard, so a slot created after the guard cannot decode the beacon. A
// call statement of its own so the guard mutation gate can remove it.
func assignXid(ctx context.Context, tx pgx.Tx, err *error) {
	_, *err = tx.Exec(ctx, "SELECT pg_current_xact_id()")
}

// checkHeld is Emit's guard (step 2).
func (b *Beacons) checkHeld(ctx context.Context, tx pgx.Tx) error {
	var active bool
	var pid *int32
	var confirmed *pglogrepl.LSN
	var backendStart *time.Time
	err := tx.QueryRow(ctx, `
		SELECT s.active, s.active_pid, s.confirmed_flush_lsn, a.backend_start
		FROM pg_replication_slots s LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
		WHERE s.slot_name = $1`,
		b.slot).Scan(&active, &pid, &confirmed, &backendStart)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: slot %q does not exist", ErrSlotNotHeld, b.slot)
	}
	if err != nil {
		return fmt.Errorf("failed to read replication slot %q: %w", b.slot, err)
	}
	if want := b.streamPID.Load(); want != 0 {
		// The holder must be this run's walsender: its PID, and (N1, #447)
		// the backend_start recorded at the stream start, since a PID can be
		// reused by another backend. One guard: either check alone catches a
		// different holder, since backend_start is read for the slot's
		// active_pid.
		samePID := pid != nil && int64(*pid) == int64(want)
		// guard: logrepl-beacon-held-by-this-walsender tests=^TestBeacons_EmitOnlyIntoTheHeldSlot$|^TestSlotIdentity_BeaconAfterWalsenderLoss_DoesNotVouchForReplacement$|^TestSlotIdentity_PIDReuse_Refused$ pkgs=./source/logrepl
		if !samePID || backendStart == nil || backendStart.UnixMicro() != b.streamStart.Load() {
			if !samePID {
				return fmt.Errorf("%w: slot %q is not held by this run's walsender (PID %d)", ErrSlotNotHeld, b.slot, want)
			}
			return fmt.Errorf("%w: slot %q is held by PID %d, which is no longer this run's walsender", ErrSlotNotHeld, b.slot, want)
		}
		return nil
	}
	limit := pglogrepl.LSN(b.preStreamLimit.Load())
	switch {
	case active:
		return fmt.Errorf("%w (slot %q)", ErrSlotInUse, b.slot)
	case confirmed == nil || *confirmed > limit:
		return fmt.Errorf("%w: slot %q moved past the position this run checked it at", ErrSlotNotHeld, b.slot)
	}
	return nil
}

// record remembers an attempt before it is written, so a beacon the stream
// delivers back at once is always recognised.
func (b *Beacons) record(start, covered pglogrepl.LSN) pendingBeacon {
	b.mu.Lock()
	defer b.mu.Unlock()
	nonce := randomHex(16)
	p := pendingBeacon{nonce: nonce, start: start, covered: covered, content: beaconContent(b.identity, b.slot, nonce, start, covered)}
	b.attempts = append(b.attempts, p)
	if len(b.attempts) > maxBeaconAttempts {
		b.attempts = b.attempts[len(b.attempts)-maxBeaconAttempts:]
	}
	return p
}

// Cap is the highest flush position an idle advance may report: the r of
// the last received beacon. Once beacons were abandoned, there is no cap,
// except while an attempt requested since is on its way: the advance then
// waits at that attempt's r so that, when the stream delivers it, it
// certifies the reported flush and re-arms the identity.
func (b *Beacons) Cap() pglogrepl.LSN {
	if b.abandoned.Load() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.inflight && b.probe != 0 {
			return b.probe
		}
		return noCap
	}
	return pglogrepl.LSN(b.cap.Load())
}

// Abandoned reports whether beacons kept failing for longer than the failure
// timeout, and none has been delivered back since (see observe). The
// subscription then advances an idle slot without a cap, so the server's
// disk is safe, and this run's slot cannot be proven: a restart in that
// state is refused (an outage, never a loss). The subscription keeps
// writing beacons, and the first one the stream delivers past the reported
// flush re-arms the identity.
func (b *Beacons) Abandoned() bool { return b.abandoned.Load() }

// RetainedBytes is how much WAL the cap holds back: serverWALEnd minus Cap.
func (b *Beacons) RetainedBytes(serverWALEnd pglogrepl.LSN) uint64 {
	c := b.Cap()
	if c == noCap || serverWALEnd <= c {
		return 0
	}
	return uint64(serverWALEnd - c)
}

// observe handles a logical decoding message from the stream. It reports
// whether the message was one of this run's beacons (matched by its exact
// content, nonce included), in which case Cap moves up to that beacon's r.
// reportedFlush is the highest flush position already reported: a beacon
// delivered while beacons were abandoned re-arms them only if its r is at or
// past it, because only then does it vouch for the slot (verifyBeacon).
func (b *Beacons) observe(ctx context.Context, m *pglogrepl.LogicalDecodingMessage, reportedFlush pglogrepl.LSN) bool {
	if m.Prefix != BeaconPrefix || !m.Transactional {
		return false // beacons are always transactional (Emit)
	}
	b.mu.Lock()
	var found *pendingBeacon
	for i := len(b.attempts) - 1; i >= 0; i-- {
		if b.attempts[i].content == string(m.Content) {
			p := b.attempts[i]
			found = &p
			b.attempts = append(b.attempts[:i], b.attempts[i+1:]...)
			break
		}
	}
	rearmed := false
	if found != nil {
		b.inflight = false
		switch {
		case !b.abandoned.Load():
			b.failingSince = time.Time{}
		case found.covered >= reportedFlush:
			b.abandoned.Store(false)
			b.failingSince, b.probe = time.Time{}, 0
			rearmed = true
		}
	}
	b.mu.Unlock()
	if found == nil {
		return false
	}
	if c := pglogrepl.LSN(b.cap.Load()); found.covered > c || rearmed {
		b.cap.Store(uint64(maxLSN(found.covered, c)))
	}
	if m.LSN > b.lastEnd {
		b.lastEnd = m.LSN
	}
	if rearmed {
		sdk.Logger(ctx).Info().
			Str("code", CodeBeaconUnavailable).
			Str("slot", b.slot).
			Msg("slot identity beacons are written again; the replication slot can be proven again")
	}
	return true
}

// maybeRequest starts writing a new beacon in the background when an idle
// slot could advance past Cap and no beacon is on its way. The subscription
// calls it only while every emitted record is acked, so the beacon certifies
// serverWALEnd (r, see verifyBeacon): the walsender sent every transaction
// that commits before the WAL end it reported, and all of them were
// delivered and acked. It asks because either
//
//   - records were acked past Cap (walFlushed): without a new beacon the slot
//     would stay below the last acked transaction's commit, and a restart
//     would decode that transaction again (the change key skips its changes,
//     so nothing is delivered twice); or
//   - the server has written at least minBeaconWAL bytes since the last
//     received beacon, which keeps an idle database idle.
//
// It also decides when to give up (see Abandoned): once emissions have been
// failing for longer than the failure timeout while the cap holds WAL back.
// After that it keeps trying, every status update, so the identity re-arms
// once beacons can be written again.
func (b *Beacons) maybeRequest(ctx context.Context, serverWALEnd, walFlushed pglogrepl.LSN) {
	abandoned := b.abandoned.Load()
	c := pglogrepl.LSN(b.cap.Load())
	if !abandoned {
		if serverWALEnd <= c {
			return
		}
		if walFlushed <= c && serverWALEnd < b.lastEnd+minBeaconWAL {
			return
		}
	}
	now := b.now()
	b.mu.Lock()
	if !abandoned && !b.failingSince.IsZero() && now.Sub(b.failingSince) > b.failureTimeout {
		b.abandoned.Store(true)
		b.inflight = false
		b.mu.Unlock()
		sdk.Logger(ctx).Error().
			Str("code", CodeBeaconUnavailable).
			Str("slot", b.slot).
			Uint64("retained_wal_bytes", uint64(serverWALEnd-c)).
			Dur("failing_for", now.Sub(b.failingSince)).
			Msg("slot identity beacons kept failing; the replication slot is advanced without them from now on so its WAL " +
				"does not fill the disk. Until a beacon is written and delivered again, the slot cannot be proven and a " +
				"restart is refused with postgres.source.slot_replaced. The connector keeps trying and recovers on its own " +
				"once beacons can be written")
		return
	}
	if b.inflight && now.Sub(b.inflightAt) < staleBeaconAfter {
		b.mu.Unlock()
		return
	}
	if b.inflight && b.failingSince.IsZero() {
		// Written but never delivered back: as good as failed.
		b.failingSince = b.inflightAt
	}
	// An emission this old is stale: stop waiting for it (its beacon still
	// counts if the stream delivers it, see attempts).
	b.inflight, b.inflightAt = true, now
	if abandoned && serverWALEnd > b.probe {
		b.probe = serverWALEnd
	}
	b.mu.Unlock()

	go func() {
		ectx, cancel := context.WithTimeout(ctx, beaconEmitTimeout)
		defer cancel()
		err := b.emit(ectx, serverWALEnd)
		b.mu.Lock()
		if err != nil {
			b.inflight = false
			if b.failingSince.IsZero() {
				b.failingSince = b.now()
			}
		}
		b.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			sdk.Logger(ctx).Warn().Err(err).
				Str("code", CodeBeaconUnavailable).
				Str("slot", b.slot).
				Uint64("retained_wal_bytes", b.RetainedBytes(serverWALEnd)).
				Bool("abandoned", b.abandoned.Load()).
				Msg("could not write a slot identity beacon; the idle replication slot is not advanced past the last one until one is written")
		}
	}()
}

// CodeBeaconUnavailable is the code logged while beacons cannot be written
// at runtime (logrepl.CodeSlotBeaconUnavailable).
const CodeBeaconUnavailable = "postgres.source.slot_beacon_unavailable"

// noCap is the idle-advance cap when beacons are not in use.
const noCap = pglogrepl.LSN(math.MaxUint64)

// beaconCap returns the idle-advance cap of b, or no cap when b is nil.
func beaconCap(b *Beacons) pglogrepl.LSN {
	if b == nil {
		return noCap
	}
	return b.Cap()
}
