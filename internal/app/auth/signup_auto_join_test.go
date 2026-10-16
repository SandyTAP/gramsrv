package auth

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

// recordingAutoJoinChannel 记录 JoinChannel 调用并对指定频道返回错误，用来验证
// 「一个频道失败不阻断其余频道、也不阻断注册」这条契约。
type recordingAutoJoinChannel struct {
	joined   []int64
	failWith map[int64]error
}

func (r *recordingAutoJoinChannel) JoinChannel(_ context.Context, userID, channelID int64, _ int) (domain.CreateChannelResult, error) {
	if err, ok := r.failWith[channelID]; ok {
		return domain.CreateChannelResult{}, err
	}
	r.joined = append(r.joined, channelID)
	return domain.CreateChannelResult{Channel: domain.Channel{ID: channelID}}, nil
}

// TestSignUpAutoJoinChannels 验证新账号注册成功后按配置顺序订阅全部目标频道。
func TestSignUpAutoJoinChannels(t *testing.T) {
	ctx := context.Background()
	channels := &recordingAutoJoinChannel{}
	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), nil, nil, "12345",
		WithSignupAutoJoin(channels, []int64{932, 1295, 1296}))

	hash, err := svc.SendCode(ctx, "+15550004501")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	verifyCodeForSignUp(t, svc, "+15550004501", hash, "12345")
	if _, _, err := svc.SignUp(ctx, domain.Authorization{}, "+15550004501", hash, "Auto", "Join"); err != nil {
		t.Fatalf("SignUp: %v", err)
	}
	want := []int64{932, 1295, 1296}
	if len(channels.joined) != len(want) {
		t.Fatalf("joined = %v, want %v", channels.joined, want)
	}
	for i, id := range want {
		if channels.joined[i] != id {
			t.Fatalf("joined = %v, want %v", channels.joined, want)
		}
	}
}

// TestSignUpAutoJoinFailureDoesNotBlockSignUp 验证订阅失败只上报 observer：账号
// 必须照常创建，其余频道照常订阅。账号此时已落库且 auth_key 已绑定，回滚注册比
// 不订阅更糟。
func TestSignUpAutoJoinFailureDoesNotBlockSignUp(t *testing.T) {
	ctx := context.Background()
	channels := &recordingAutoJoinChannel{failWith: map[int64]error{
		1295: domain.ErrChannelUserBanned,
	}}
	type failure struct {
		userID    int64
		channelID int64
		err       error
	}
	var failures []failure
	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), nil, nil, "12345",
		WithSignupAutoJoin(channels, []int64{932, 1295, 1296}),
		WithSignupAutoJoinFailureObserver(func(_ context.Context, userID, channelID int64, err error) {
			failures = append(failures, failure{userID: userID, channelID: channelID, err: err})
		}))

	hash, err := svc.SendCode(ctx, "+15550004502")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	verifyCodeForSignUp(t, svc, "+15550004502", hash, "12345")
	u, _, err := svc.SignUp(ctx, domain.Authorization{}, "+15550004502", hash, "Auto", "Join")
	if err != nil {
		t.Fatalf("SignUp must succeed even when auto-join fails: %v", err)
	}
	if u.ID == 0 {
		t.Fatal("SignUp returned a zero user")
	}
	if len(failures) != 1 || failures[0].channelID != 1295 || !errors.Is(failures[0].err, domain.ErrChannelUserBanned) {
		t.Fatalf("failures = %+v, want one ErrChannelUserBanned for channel 1295", failures)
	}
	if failures[0].userID != u.ID {
		t.Fatalf("observer user_id = %d, want %d", failures[0].userID, u.ID)
	}
	if len(channels.joined) != 2 {
		t.Fatalf("joined = %v, want the 2 healthy channels", channels.joined)
	}
}

// TestSignUpAutoJoinDisabled 验证未配置时不发生任何订阅（默认行为不能被隐式改变）。
func TestSignUpAutoJoinDisabled(t *testing.T) {
	ctx := context.Background()
	channels := &recordingAutoJoinChannel{}
	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), nil, nil, "12345",
		WithSignupAutoJoin(channels, nil))

	hash, err := svc.SendCode(ctx, "+15550004503")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	verifyCodeForSignUp(t, svc, "+15550004503", hash, "12345")
	if _, _, err := svc.SignUp(ctx, domain.Authorization{}, "+15550004503", hash, "No", "Join"); err != nil {
		t.Fatalf("SignUp: %v", err)
	}
	if len(channels.joined) != 0 {
		t.Fatalf("joined = %v, want none when the list is empty", channels.joined)
	}
}
