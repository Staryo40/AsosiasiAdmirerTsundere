package aggregator

import (
	"testing"
	"time"
)

func TestOutboxWaitScheduling(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	maxBackoff := 5 * time.Minute

	tests := []struct {
		name    string
		message outboxMessage
		want    time.Duration
	}{
		{"initial future row is immediate", outboxMessage{NextAttemptAt: now.Add(time.Hour)}, 0},
		{"valid retry keeps bounded delay", outboxMessage{Attempts: 1, NextAttemptAt: now.Add(2 * time.Minute)}, 2 * time.Minute},
		{"skewed retry beyond bound is due", outboxMessage{Attempts: 1, NextAttemptAt: now.Add(maxBackoff + time.Second)}, 0},
		{"past retry has no negative wait", outboxMessage{Attempts: 1, NextAttemptAt: now.Add(-time.Second)}, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := outboxWait(test.message, now, maxBackoff); got != test.want {
				t.Fatalf("outboxWait() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestOutboxWaitDoesNotBlockIndependentShard(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	maxBackoff := 5 * time.Minute
	shardHead := outboxMessage{Attempts: 1, NextAttemptAt: now.Add(time.Minute)}
	independentShardMessage := outboxMessage{Attempts: 0, NextAttemptAt: now.Add(time.Hour)}

	if wait := outboxWait(shardHead, now, maxBackoff); wait != time.Minute {
		t.Fatalf("delayed shard head wait = %s, want %s", wait, time.Minute)
	}
	if wait := outboxWait(independentShardMessage, now, maxBackoff); wait != 0 {
		t.Fatalf("independent shard message wait = %s, want immediate delivery", wait)
	}
}
