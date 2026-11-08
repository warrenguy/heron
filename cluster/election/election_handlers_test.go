package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
	"luas.fast/cluster/consensus"
	"luas.fast/cluster/election"
)

func TestHandleConsensusMessage_roundtrip(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}

	cases := []struct {
		name    string
		publish func()
		spy     func(*testing.T, *recordingNodeState, *FakeEpochClock)
	}{
		{
			name: "CanvassPosition",
			publish: func() {
				pub.CanvassPosition(
					dst,
					codecs.CanvassPositionFields{
						LogLeadershipTermID: 1,
						LogPosition:         2,
						LeadershipTermID:    3,
						FollowerMemberID:    4,
					},
				)
			},
		},
		{
			name: "RequestVote",
			publish: func() {
				pub.RequestVote(
					dst,
					codecs.RequestVoteFields{
						LogLeadershipTermID: 10,
						LogPosition:         20,
						CandidateTermID:     30,
						CandidateMemberID:   40,
					},
				)
			},
			spy: func(t *testing.T, nodeState *recordingNodeState, epochClock *FakeEpochClock) {
				t.Helper()
				require.Len(t, nodeState.proposeCalls, 1)
				require.Equal(t, int64(30), nodeState.proposeCalls[0].candidateTermID)
				require.Equal(t, int64(20), nodeState.proposeCalls[0].logPosition)
				require.Equal(t, epochClock.nowMs, nodeState.proposeCalls[0].timestampMs)
			},
		},
		{
			name: "Vote",
			publish: func() {
				pub.PlaceVote(
					dst,
					codecs.VoteFields{
						CandidateTermID:     1,
						LogLeadershipTermID: 2,
						LogPosition:         3,
						CandidateMemberID:   4,
						FollowerMemberID:    5,
						Vote:                clusterwire.BooleanType_TRUE,
					},
				)
			},
		},
		{
			name: "NewLeadershipTerm",
			publish: func() {
				pub.NewLeadershipTerm(
					dst,
					codecs.NewLeadershipTermFields{
						LogLeadershipTermID:  1,
						NextLeadershipTermID: 2,
						LeaderMemberID:       3,
					},
				)
			},
		},
		{
			name: "AppendPosition",
			publish: func() {
				pub.AppendPosition(
					dst,
					codecs.AppendPositionFields{
						LeadershipTermID: 7,
						LogPosition:      8,
						FollowerMemberID: 9,
						Flags:            0x01,
					},
				)
			},
		},
		{
			name: "CommitPosition",
			publish: func() {
				pub.CommitPosition(
					dst,
					codecs.CommitPositionFields{
						LeadershipTermID: 11,
						LogPosition:      12,
						LeaderMemberID:   13,
					},
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, deps, _, epochClock, nodeState := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			pub.Reset()
			tc.publish()
			require.Len(t, pub.Sent, 1)
			require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
			if tc.spy != nil {
				tc.spy(t, nodeState, epochClock)
			}
		})
	}
}

func TestHandleConsensusMessage_rejectsBadInput(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	wire := pub.Sent[0].Wire

	t.Run("bad schema id", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[4] = 0
		bad[5] = 0
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})

	t.Run("truncated buffer", func(t *testing.T) {
		require.False(t, e.HandleConsensusMessage(wire[:4], 0))
	})

	t.Run("truncated body", func(t *testing.T) {
		headerLen := codecs.MessageHeaderEncodedLen()
		require.Greater(t, len(wire), headerLen+4)
		require.False(t, e.HandleConsensusMessage(wire[:headerLen+4], 0))
	})

	t.Run("unknown template", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[2] = 0xFF
		bad[3] = 0xFF
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})
}

func TestHandleConsensusMessage_gatedInInit(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.RequestVote(
		dst,
		codecs.RequestVoteFields{
			LogLeadershipTermID: 10,
			LogPosition:         20,
			CandidateTermID:     30,
			CandidateMemberID:   40,
		},
	)
	require.Len(t, pub.Sent, 1)
	require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
	require.Empty(t, nodeState.proposeCalls)
}

func TestElection_HandleError_resetsToInit(t *testing.T) {
	t.Parallel()

	t.Run("non-nil error", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 80
		cfg, deps, _, _, _ := testElectionDeps(t)
		require.Greater(t, cfg.LogPosition, hostCommit)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		observer := &recordingStateObserver{}
		deps.StateObserver = observer
		err := errors.New("test error")
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		appendBefore := e.AppendPosition()

		e.HandleError(5_000, err)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, err, e.LastError())
		require.Equal(t, hostCommit, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
		require.Equal(t, []election.ElectionState{
			election.ElectionStateInit,
			election.ElectionStateInit,
		}, observer.states)
	})

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 40
		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		observer := &recordingStateObserver{}
		deps.StateObserver = observer
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		e.HandleError(1_000, errors.New("prior"))
		require.Error(t, e.LastError())

		e.HandleError(5_000, nil)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.NoError(t, e.LastError())
		require.Equal(t, hostCommit, e.LogPosition())
		// Same-state Init is a no-op for entry side effects and the observer.
		require.Equal(t, []election.ElectionState{
			election.ElectionStateInit,
			election.ElectionStateInit,
		}, observer.states)
	})

	t.Run("ErrorHandler set", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 40
		cfg, deps, _, _, _ := testElectionDeps(t)
		require.Greater(t, cfg.LogPosition, hostCommit)
		deps.Host = &recordingElectionHost{commitPosition: hostCommit}

		// Capture pre-transition fields inside the handler. LastError is set first.
		// State and logPosition stay pre-HandleError until the handler returns.
		type handlerSnapshot struct {
			err         error
			lastError   error
			state       election.ElectionState
			logPosition int64
		}
		var e *election.Election
		var snaps []handlerSnapshot
		deps.ErrorHandler = func(err error) {
			snaps = append(snaps, handlerSnapshot{
				err:         err,
				lastError:   e.LastError(),
				state:       e.State(),
				logPosition: e.LogPosition(),
			})
		}
		e = election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		appendBefore := e.AppendPosition()
		err := errors.New("handler sees this")

		e.HandleError(5_000, err)

		require.Equal(t, []handlerSnapshot{{
			err:         err,
			lastError:   err,
			state:       election.ElectionStateCanvass,
			logPosition: cfg.LogPosition,
		}}, snaps)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, hostCommit, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())

		// Nil error skips the handler and clears LastError.
		e.HandleError(6_000, nil)

		require.Len(t, snaps, 1)
		require.NoError(t, e.LastError())
	})

	t.Run("ErrorHandler nil", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		deps.Host = &recordingElectionHost{}
		e := election.NewElection(cfg, deps)

		require.NotPanics(t, func() {
			e.HandleError(5_000, errors.New("no handler"))
		})
		require.Equal(t, election.ElectionStateInit, e.State())
		require.EqualError(t, e.LastError(), "no handler")
	})

	t.Run("host commit not notified commit", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 77
		const notifiedCommit int64 = 999
		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		// Install sets NotifiedCommitPosition from the wire CommitPosition field.
		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.CommitPosition = notifiedCommit
		e.OnNewLeadershipTerm(fields)
		require.Equal(t, notifiedCommit, e.NotifiedCommitPosition())
		require.NotEqual(t, hostCommit, e.NotifiedCommitPosition())
		appendBefore := e.AppendPosition()

		e.HandleError(5_000, errors.New("commit source"))

		require.Equal(t, hostCommit, e.LogPosition())
		require.NotEqual(t, e.NotifiedCommitPosition(), e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
	})

	t.Run("sticky role after HandleError", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{role: consensus.RoleLeader}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateLeaderReady)

		e.HandleError(5_000, errors.New("keep role"))

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, consensus.RoleLeader, host.Role())
	})
}

func TestElection_HandleError_truncateAndPrepare(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		hostCommit int64
		wantLog    int64
	}{
		{
			name:       "truncate success commit at or below newPosition",
			hostCommit: truncateInstallBase - 8,
			wantLog:    truncateInstallBase - 8,
		},
		{
			name:       "truncate success commit above newPosition",
			hostCommit: truncateInstallBase + 64,
			wantLog:    truncateInstallBase,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := runTruncateInstall(t, truncateInstallErrs{}, tc.hostCommit)

			require.Equal(t, truncateInstallBase, r.e.AppendPosition())
			require.Equal(t, tc.wantLog, r.e.LogPosition())
			require.EqualError(t, r.e.LastError(), "election: truncating cluster log")
			require.Equal(t, election.ElectionStateInit, r.e.State())
		})
	}

	t.Run("truncate success zero commit prepare collapses append", func(t *testing.T) {
		t.Parallel()

		// Host commit 0 and prepare return of logPosition document the collapse path.
		r := runTruncateInstall(t, truncateInstallErrs{}, 0)
		require.Equal(t, truncateInstallBase, r.e.AppendPosition())
		require.Equal(t, int64(0), r.e.LogPosition())

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = false
		cfg.LogPosition = r.e.LogPosition()
		cfg.AppendPosition = r.e.AppendPosition()
		deps.Host = &recordingElectionHost{commitPosition: 0}
		deps.Handler = &recordingAgentHandler{appendPositionReturn: r.e.LogPosition()}
		e := election.NewElection(cfg, deps)
		require.Equal(t, truncateInstallBase, e.AppendPosition())
		require.Equal(t, int64(0), e.LogPosition())

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, int64(0), e.AppendPosition())
		require.Equal(t, int64(0), e.LogPosition())
	})

	t.Run("truncate success stop-derived prepare", func(t *testing.T) {
		t.Parallel()

		const hostCommit = truncateInstallBase - 16
		const stopPos int64 = 180
		r := runTruncateInstall(t, truncateInstallErrs{}, hostCommit)
		require.Equal(t, hostCommit, r.e.LogPosition())
		require.Equal(t, truncateInstallBase, r.e.AppendPosition())

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = false
		cfg.LogPosition = r.e.LogPosition()
		cfg.AppendPosition = r.e.AppendPosition()
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		deps.Handler = &recordingAgentHandler{appendPositionReturn: stopPos}
		e := election.NewElection(cfg, deps)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, stopPos, e.AppendPosition())
		require.Equal(t, hostCommit, e.LogPosition())
	})
}

// catchupPositionFailPublisher fails CatchupPosition. Other publishes pass
// through to the embedded SbePublisher.
type catchupPositionFailPublisher struct {
	*election.SbePublisher
}

func (p *catchupPositionFailPublisher) CatchupPosition(
	election.PublicationTarget,
	codecs.CatchupPositionFields,
) bool {
	return false
}

// errorClassRun holds the inputs of one TestElection_errorClasses row.
type errorClassRun struct {
	cfg   election.ElectionConfig
	deps  election.ElectionDeps
	clock *FakeClock
}

func TestElection_errorClasses(t *testing.T) {
	t.Parallel()

	errPrepare := errors.New("prepare stop failed")
	for _, tc := range []struct {
		name string

		// want is nil for an unexported error. text identifies it.
		want error
		text string

		// atDeadline ticks once before the leader heartbeat deadline, expecting
		// workBeforeDeadline and no error, then once at the deadline.
		atDeadline         bool
		workBeforeDeadline int

		drive func(t *testing.T, r *errorClassRun) *election.Election
	}{
		{
			name: "canvass above the election term in leader ready",
			want: election.ErrNewElectionFromCanvass,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				useRecordingHost(&r.deps, r.clock, r.cfg, consensus.RoleLeader)
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateLeaderReady)
				election.SetLogSessionIDForTest(e, fixtureLogSessionID)
				e.OnCanvassPosition(codecs.CanvassPositionFields{
					LogLeadershipTermID: r.cfg.LeadershipTermID + 1,
					LogPosition:         50,
					LeadershipTermID:    r.cfg.LeadershipTermID + 1,
					FollowerMemberID:    1,
				})
				return e
			},
		},
		{
			name: "commit position from a newer leader in leader ready",
			want: election.ErrNewLeaderFromCommitPosition,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateLeaderReady)
				e.OnCommitPosition(codecs.CommitPositionFields{
					LeadershipTermID: r.cfg.LeadershipTermID + 1,
					LogPosition:      500,
					LeaderMemberID:   1,
				})
				return e
			},
		},
		{
			name: "truncation on install",
			want: election.ErrTruncatingClusterLog,
			drive: func(t *testing.T, r *errorClassRun) *election.Election {
				errs := truncateInstallErrs{}
				return runTruncateInstallOver(t, r.cfg, r.deps, r.clock, errs, truncateInstallBase).e
			},
		},
		{
			name: "log await join position mismatch",
			want: election.ErrLogJoinPositionMismatch,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:                   r.clock,
					localLogRecordingID:     int64(election.NullValue),
					followerLogJoinOK:       true,
					followerLogJoinPosition: r.cfg.LogPosition + 1,
				}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)
				e.DoWork(r.clock.nowNs)
				return e
			},
		},
		{
			name:       "catchup await join position mismatch at the deadline",
			want:       election.ErrLogJoinPositionMismatch,
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:                   r.clock,
					followerLogJoinOK:       true,
					followerLogJoinPosition: r.cfg.LogPosition + 50,
				}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)
				election.SetLogSessionIDForTest(e, 7)
				return e
			},
		},
		{
			name: "commit timeout with a done helper and notified commit below the append position",
			want: election.ErrCommitPositionTimeout,
			drive: func(t *testing.T, r *errorClassRun) *election.Election {
				lr := &countingLogReplicationForTest{
					done:     true,
					position: r.cfg.AppendPosition + replicationStopAhead,
				}
				e := newReplicationFixtureOver(t, r.cfg, r.deps, r.clock, lr).e

				t0 := r.clock.nowNs
				require.Equal(t, 1, e.DoWork(t0))
				require.Equal(t, 1, e.DoWork(t0+1))
				require.Less(t, e.NotifiedCommitPosition(), e.AppendPosition())
				require.NoError(t, e.LastError())
				e.DoWork(t0 + r.cfg.LeaderHeartbeatTimeoutNs)
				return e
			},
		},
		{
			name:       "commit timeout without a helper",
			want:       election.ErrCommitPositionTimeout,
			atDeadline: true,
			drive: func(t *testing.T, r *errorClassRun) *election.Election {
				e := election.NewElection(r.cfg, r.deps)
				stopPosition := r.cfg.AppendPosition + replicationStopAhead
				driveToFollowerLogReplication(t, e, r.cfg, stopPosition, election.NullPosition)
				return e
			},
		},
		{
			name: "invalid newLeadershipTerm",
			text: "election: invalid newLeadershipTerm",
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{clock: r.clock}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateCanvass)
				fields := installNewLeadershipTermFields(r.cfg, 1, r.cfg.LeadershipTermID)
				fields.TermBaseLogPosition = r.cfg.AppendPosition + 100
				e.OnNewLeadershipTerm(fields)
				return e
			},
		},
		{
			name: "PrepareForNewLeadership error",
			want: errPrepare,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.cfg.IsNodeStartup = false
				r.deps.Host = &recordingElectionHost{}
				r.deps.Handler = &recordingAgentHandler{prepareErr: errPrepare}
				e := election.NewElection(r.cfg, r.deps)
				e.DoWork(r.clock.nowNs)
				return e
			},
		},
		{
			name:       "catchup init without a leader at the deadline",
			text:       "election: no leader or catchup endpoint for catchup position",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{clock: r.clock}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerCatchupInit)
				return e
			},
		},
		{
			name:       "catchup init destination open failure at the deadline",
			text:       "election: failed to open catchup log destination",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{clock: r.clock}
				e := installLeaderForTest(r.cfg, r.deps, election.ElectionStateFollowerCatchupInit)
				election.MemberForTest(e, r.cfg.MemberID).CatchupEndpoint = "127.0.0.1:9006"
				return e
			},
		},
		{
			name:               "catchup init send failure at the deadline",
			text:               "election: failed to send catchup position",
			atDeadline:         true,
			workBeforeDeadline: 1,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:                       r.clock,
					ensureCatchupLogDestination: "aeron:udp?endpoint=127.0.0.1:9007",
				}
				r.deps.Publisher = &catchupPositionFailPublisher{SbePublisher: &election.SbePublisher{}}
				e := installLeaderForTest(r.cfg, r.deps, election.ElectionStateFollowerCatchupInit)
				election.MemberForTest(e, r.cfg.MemberID).CatchupEndpoint = "127.0.0.1:9007"
				return e
			},
		},
		{
			name:       "catchup await image not ready at the deadline",
			text:       "election: failed to join catchup log",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{clock: r.clock}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)
				return e
			},
		},
		{
			name:       "catchup await join rejected at the deadline",
			text:       "election: failed to join catchup log as follower",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:                   r.clock,
					followerLogJoinOK:       true,
					followerLogJoinPosition: r.cfg.LogPosition,
				}
				r.deps.Handler = &recordingAgentHandler{}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)
				return e
			},
		},
		{
			name:       "log await image not ready at the deadline",
			text:       "election: failed to join live log",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:               r.clock,
					localLogRecordingID: int64(election.NullValue),
				}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)
				return e
			},
		},
		{
			name:       "log await join rejected at the deadline",
			text:       "election: failed to join live log as follower",
			atDeadline: true,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Host = &recordingElectionHost{
					clock:                   r.clock,
					localLogRecordingID:     int64(election.NullValue),
					followerLogJoinOK:       true,
					followerLogJoinPosition: r.cfg.LogPosition,
				}
				r.deps.Handler = &recordingAgentHandler{}
				e := election.NewElection(r.cfg, r.deps)
				election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)
				return e
			},
		},
		{
			name:               "ready follower notify failure at the deadline",
			text:               "election: ready follower failed to notify leader",
			atDeadline:         true,
			workBeforeDeadline: 1,
			drive: func(_ *testing.T, r *errorClassRun) *election.Election {
				r.deps.Handler = &recordingAgentHandler{}
				r.deps.Publisher = &appendPositionFailPublisher{SbePublisher: &election.SbePublisher{}}
				return installLeaderForTest(r.cfg, r.deps, election.ElectionStateFollowerReady)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			var reports []error
			deps.ErrorHandler = func(err error) { reports = append(reports, err) }

			e := tc.drive(t, &errorClassRun{cfg: cfg, deps: deps, clock: clock})
			if tc.atDeadline {
				deadline := clock.nowNs + cfg.LeaderHeartbeatTimeoutNs
				require.Equal(t, tc.workBeforeDeadline, e.DoWork(deadline-1))
				require.NoError(t, e.LastError())
				e.DoWork(deadline)
			}

			require.Len(t, reports, 1)
			err := reports[0]
			require.Equal(t, err, e.LastError())
			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, errors.Is(tc.want, election.ErrEvent), errors.Is(err, election.ErrEvent))
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			} else {
				require.EqualError(t, err, tc.text)
			}
		})
	}
}
