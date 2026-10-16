package domain

import "testing"

// TestChannelReviewActive: the whole point of an optional deadline is that a
// channel comes back on its own once it passes, with no sweep job and no server
// restart. If Active ever returned true past the deadline the channel would stay
// blocked forever while the panel claimed it was temporary.
func TestChannelReviewActive(t *testing.T) {
	for _, tc := range []struct {
		name   string
		review ChannelReview
		now    int
		want   bool
	}{
		{
			name:   "no review",
			review: ChannelReview{},
			now:    1000,
			want:   false,
		},
		{
			name:   "open ended review stays active forever",
			review: ChannelReview{StartedAt: 100, Until: 0, Reason: "проверка"},
			now:    99999999,
			want:   true,
		},
		{
			name:   "before the deadline",
			review: ChannelReview{StartedAt: 100, Until: 200, Reason: "проверка"},
			now:    150,
			want:   true,
		},
		{
			name:   "exactly at the deadline is over",
			review: ChannelReview{StartedAt: 100, Until: 200, Reason: "проверка"},
			now:    200,
			want:   false,
		},
		{
			name:   "after the deadline",
			review: ChannelReview{StartedAt: 100, Until: 200, Reason: "проверка"},
			now:    201,
			want:   false,
		},
		{
			// A half-written row must not block a channel with no explanation.
			name:   "started but no reason is not a review",
			review: ChannelReview{StartedAt: 100, Until: 200},
			now:    150,
			want:   false,
		},
	} {
		if got := tc.review.Active(tc.now); got != tc.want {
			t.Errorf("%s: Active(%d) = %v, want %v", tc.name, tc.now, got, tc.want)
		}
	}
}

// TestValidateChannelReview: a blocked channel with nothing to show its viewers,
// or a deadline that is already past, both write a confusing row rather than a
// useful one.
func TestValidateChannelReview(t *testing.T) {
	const now = 1_000_000

	if err := ValidateChannelReview("проверка", 0, now); err != nil {
		t.Errorf("an open ended review was rejected: %v", err)
	}
	if err := ValidateChannelReview("проверка", now+3600, now); err != nil {
		t.Errorf("a future deadline was rejected: %v", err)
	}
	if err := ValidateChannelReview("", 0, now); err == nil {
		t.Error("an empty reason was accepted")
	}
	if err := ValidateChannelReview("проверка", now, now); err == nil {
		t.Error("a deadline equal to now was accepted; it would block and unblock at once")
	}
	if err := ValidateChannelReview("проверка", now-1, now); err == nil {
		t.Error("a deadline in the past was accepted")
	}
	if err := ValidateChannelReview(longString(MaxChannelReviewReasonRunes+1), 0, now); err == nil {
		t.Error("an over-long reason was accepted")
	}
	if err := ValidateChannelReview("проверка", now+(MaxChannelReviewDays+1)*24*60*60, now); err == nil {
		t.Error("a deadline beyond the maximum was accepted")
	}
}

func longString(n int) string {
	out := make([]rune, n)
	for i := range out {
		out[i] = 'x'
	}
	return string(out)
}
