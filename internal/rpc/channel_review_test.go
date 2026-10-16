package rpc

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	appchannels "telesrv/internal/app/channels"
	"telesrv/internal/store/memory"
)

// Channel review: while a channel is under moderator review an ordinary viewer
// must not be able to read its posts. Administrators are exempt by design -- an
// operator who cannot read what they are moderating cannot moderate it -- and a
// deadline lifts the block on its own with no sweep job.

func reviewRouter(t *testing.T, now int64) (*Router, *memory.ChannelStore) {
	t.Helper()
	store := memory.NewChannelStore()
	return New(Config{DC: 2, IP: "127.0.0.1", Port: 2398}, Deps{
		Channels: appchannels.NewService(store),
	}, zaptest.NewLogger(t), fixedClock{now: time.Unix(now, 0)}), store
}

// TestChannelReviewBlocksOrdinaryViewer is the feature itself.
func TestChannelReviewBlocksOrdinaryViewer(t *testing.T) {
	r, store := reviewRouter(t, 1000)
	ctx := context.Background()

	if _, err := store.SetChannelUnderReview(ctx, 500, "Канал на проверке модераторов", 0, 1000); err != nil {
		t.Fatalf("set review: %v", err)
	}
	if err := r.ensureChannelReadableForViewer(ctx, 7, 500); err == nil {
		t.Fatal("an ordinary viewer read a channel that is under review")
	}
}

// TestChannelReviewLeavesNormalChannelsAlone matters because the check sits on
// the hot path of every history read: a false positive here would break reading
// every channel on the platform.
func TestChannelReviewLeavesNormalChannelsAlone(t *testing.T) {
	r, _ := reviewRouter(t, 1000)
	if err := r.ensureChannelReadableForViewer(context.Background(), 7, 500); err != nil {
		t.Fatalf("a channel that was never reviewed was blocked: %v", err)
	}
}

// TestChannelReviewExpiresWithoutASweep: the deadline is evaluated on read, so
// a temporary review ends even if the server was down when it passed.
func TestChannelReviewExpiresWithoutASweep(t *testing.T) {
	ctx := context.Background()
	before, store := reviewRouter(t, 1000)
	if _, err := store.SetChannelUnderReview(ctx, 500, "Канал на проверке", 2000, 1000); err != nil {
		t.Fatalf("set review: %v", err)
	}
	if err := before.ensureChannelReadableForViewer(ctx, 7, 500); err == nil {
		t.Fatal("the channel was readable before its deadline")
	}

	after, _ := reviewRouter(t, 2001)
	if err := after.ensureChannelReadableForViewer(ctx, 7, 500); err != nil {
		t.Fatalf("the channel stayed blocked after its deadline: %v", err)
	}
}

// TestChannelReviewLiftedManually: clearing the review restores reading at once.
func TestChannelReviewLiftedManually(t *testing.T) {
	ctx := context.Background()
	r, store := reviewRouter(t, 1000)

	if _, err := store.SetChannelUnderReview(ctx, 500, "Канал на проверке", 0, 1000); err != nil {
		t.Fatalf("set review: %v", err)
	}
	if err := r.ensureChannelReadableForViewer(ctx, 7, 500); err == nil {
		t.Fatal("expected the channel to be blocked")
	}
	if _, err := store.SetChannelUnderReview(ctx, 500, "", 0, 1000); err != nil {
		t.Fatalf("clear review: %v", err)
	}
	if err := r.ensureChannelReadableForViewer(ctx, 7, 500); err != nil {
		t.Fatalf("the channel stayed blocked after being cleared: %v", err)
	}
}
