package domain

import (
	"strings"
	"testing"
)

// TestChatStreakDayWord pins the Russian plural. This is the text every user
// reads on the day the mechanic starts, and "1 дней" on day one is the kind of
// detail that makes the whole thing feel unowned.
func TestChatStreakDayWord(t *testing.T) {
	cases := map[int]string{
		0: "дней", 1: "день", 2: "дня", 4: "дня", 5: "дней",
		11: "дней", 12: "дней", 14: "дней", 21: "день", 22: "дня",
		25: "дней", 101: "день", 111: "дней", 112: "дней", 121: "день",
		365: "дней", 1000: "дней",
	}
	for days, want := range cases {
		if got := ChatStreakDayWord(days); got != want {
			t.Fatalf("ChatStreakDayWord(%d) = %q, want %q", days, got, want)
		}
	}
}

// TestChatStreakNotificationBody checks each outcome reads as its own thing and
// always carries the current count.
func TestChatStreakNotificationBody(t *testing.T) {
	started := ChatStreakNotificationBody(ChatStreak{Days: 1}, ChatStreakStarted)
	if !strings.Contains(started, "1 день") || !strings.Contains(started, "🔥") {
		t.Fatalf("started body = %q", started)
	}
	advanced := ChatStreakNotificationBody(ChatStreak{Days: 5}, ChatStreakAdvanced)
	if !strings.Contains(advanced, "5 дней") {
		t.Fatalf("advanced body = %q", advanced)
	}
	milestone := ChatStreakNotificationBody(ChatStreak{Days: 30}, ChatStreakAdvanced)
	if !strings.Contains(milestone, "30 дней") || !strings.Contains(milestone, "Отличная серия") {
		t.Fatalf("milestone body = %q", milestone)
	}
	if plain := ChatStreakNotificationBody(ChatStreak{Days: 6}, ChatStreakAdvanced); strings.Contains(plain, "Отличная серия") {
		t.Fatalf("a non-milestone must not celebrate: %q", plain)
	}
	resumed := ChatStreakNotificationBody(ChatStreak{Days: 9, FrozenDays: 1}, ChatStreakResumed)
	if !strings.Contains(resumed, "Заморозка") || !strings.Contains(resumed, "9 дней") {
		t.Fatalf("resumed body = %q", resumed)
	}
	// A reset must never read like an ordinary advance.
	broken := ChatStreakNotificationBody(ChatStreak{Days: 1}, ChatStreakBroken)
	if !strings.Contains(broken, "заново") {
		t.Fatalf("broken body = %q, must explain the reset", broken)
	}
	if plain := ChatStreakNotificationBody(ChatStreak{Days: 1}, ChatStreakStarted); plain == broken {
		t.Fatal("a reset and a first day must not read identically")
	}
}

// TestShouldNotifyChatStreak: the unchanged day is silent in every mode, and
// daily is the only mode that posts on an arbitrary day.
func TestShouldNotifyChatStreak(t *testing.T) {
	for _, mode := range []ChatStreakNotifyMode{ChatStreakNotifyDaily, ChatStreakNotifyMilestones, ChatStreakNotifyOff} {
		if ShouldNotifyChatStreak(mode, ChatStreak{Days: 3}, ChatStreakUnchanged) {
			t.Fatalf("%s: an unchanged day must never notify", mode)
		}
	}
	if !ShouldNotifyChatStreak(ChatStreakNotifyDaily, ChatStreak{Days: 6}, ChatStreakAdvanced) {
		t.Fatal("daily mode must post on an arbitrary day")
	}
	if ShouldNotifyChatStreak(ChatStreakNotifyMilestones, ChatStreak{Days: 6}, ChatStreakAdvanced) {
		t.Fatal("milestones mode must stay quiet on a non-threshold day")
	}
	if !ShouldNotifyChatStreak(ChatStreakNotifyMilestones, ChatStreak{Days: 1}, ChatStreakStarted) {
		t.Fatal("milestones mode must always post the first day")
	}
	if !ShouldNotifyChatStreak(ChatStreakNotifyMilestones, ChatStreak{Days: 30}, ChatStreakAdvanced) {
		t.Fatal("milestones mode must post on a threshold")
	}
	if ShouldNotifyChatStreak(ChatStreakNotifyOff, ChatStreak{Days: 30}, ChatStreakAdvanced) {
		t.Fatal("off mode must never notify")
	}
	// A streak that somehow carries no days has nothing to announce.
	if ShouldNotifyChatStreak(ChatStreakNotifyDaily, ChatStreak{Days: 0}, ChatStreakAdvanced) {
		t.Fatal("a zero-day streak must not notify")
	}
}

// TestOfficialChatStreakMessage: the message must arrive from the official
// system account into the recipient's own box, or the client renders it as an
// ordinary incoming message instead of a platform notice.
func TestOfficialChatStreakMessage(t *testing.T) {
	msg, err := OfficialChatStreakMessage(4242, ChatStreak{Days: 7}, ChatStreakAdvanced, 1700000000)
	if err != nil {
		t.Fatalf("OfficialChatStreakMessage: %v", err)
	}
	if msg.OwnerUserID != 4242 {
		t.Fatalf("OwnerUserID = %d, want 4242", msg.OwnerUserID)
	}
	if msg.From.ID != OfficialSystemUserID || msg.Peer.ID != OfficialSystemUserID {
		t.Fatalf("message must come from the official system account, got from=%d peer=%d",
			msg.From.ID, msg.Peer.ID)
	}
	if msg.Body == "" {
		t.Fatal("body must not be empty")
	}
	if _, err := OfficialChatStreakMessage(777000, ChatStreak{Days: 7}, ChatStreakAdvanced, 1); err == nil {
		t.Fatal("a system account must not receive its own notification")
	}
	if _, err := OfficialChatStreakMessage(4242, ChatStreak{Days: 7}, ChatStreakAdvanced, -1); err == nil {
		t.Fatal("a negative date must be rejected")
	}
}
