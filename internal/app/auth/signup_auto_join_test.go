package auth

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

type mockSignupAutoJoinChannelService struct {
	joined []struct {
		userID    int64
		channelID int64
	}
	failChannelID int64
}

func (m *mockSignupAutoJoinChannelService) JoinChannel(ctx context.Context, userID, channelID int64, date int) (domain.CreateChannelResult, error) {
	if m.failChannelID != 0 && channelID == m.failChannelID {
		return domain.CreateChannelResult{}, errors.New("channel banned or unavailable")
	}
	m.joined = append(m.joined, struct {
		userID    int64
		channelID int64
	}{userID: userID, channelID: channelID})
	return domain.CreateChannelResult{}, nil
}

func TestSignUpAutoJoinChannels(t *testing.T) {
	ctx := context.Background()
	mockChannels := &mockSignupAutoJoinChannelService{failChannelID: 200}
	var observedFailures []int64
	failureObserver := func(_ context.Context, userID, channelID int64, err error) {
		observedFailures = append(observedFailures, channelID)
	}

	svc := NewService(
		memory.NewUserStore(),
		memory.NewAuthorizationStore(),
		memory.NewCodeStore(),
		nil,
		nil,
		"12345",
		WithSignupAutoJoin(mockChannels, []int64{100, 200, 300}),
		WithSignupAutoJoinFailureObserver(failureObserver),
	)

	hash, err := svc.SendCode(ctx, "+15550005501")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	verifyCodeForSignUp(t, svc, "+15550005501", hash, "12345")
	u, _, err := svc.SignUp(ctx, domain.Authorization{}, "+15550005501", hash, "Auto", "Join")
	if err != nil {
		t.Fatalf("SignUp failed: %v", err)
	}
	if u.ID == 0 {
		t.Fatal("expected created user to have non-zero ID")
	}

	if len(mockChannels.joined) != 2 {
		t.Fatalf("expected 2 joined channels, got %d", len(mockChannels.joined))
	}
	if mockChannels.joined[0].channelID != 100 || mockChannels.joined[1].channelID != 300 {
		t.Fatalf("unexpected joined channels: %+v", mockChannels.joined)
	}
	if len(observedFailures) != 1 || observedFailures[0] != 200 {
		t.Fatalf("expected observed failure on channel 200, got: %+v", observedFailures)
	}
}
