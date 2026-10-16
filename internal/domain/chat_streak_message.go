package domain

import (
	"fmt"
	"math"
	"strconv"
)

// The "огонёк" service message.
//
// A streak that only lives in a table row is invisible, and an invisible
// mechanic is not a mechanic. This builds the account-visible message the
// official system account posts into each participant's private chat when the
// streak moves, using the same shape as OfficialWelcomeMessage: from 777000, so
// it reads like a platform notice rather than a third party reading the user's
// conversations.
//
// Telegram has no streak field on the wire, so this message is the only place
// the number is ever shown to the people who earned it.

// ChatStreakNotifyMode selects which streak moves produce a message.
type ChatStreakNotifyMode string

const (
	// ChatStreakNotifyDaily posts on every new day. It is the loudest mode and
	// the one that actually drives the behaviour it is trying to reward.
	ChatStreakNotifyDaily ChatStreakNotifyMode = "daily"
	// ChatStreakNotifyMilestones posts only on the thresholds in
	// ChatStreakMilestone plus the very first day. Quieter, for an audience that
	// finds a daily badge noisy.
	ChatStreakNotifyMilestones ChatStreakNotifyMode = "milestones"
	// ChatStreakNotifyOff posts nothing. The counter keeps counting; nobody is
	// told. Useful for turning the noise off without losing the state.
	ChatStreakNotifyOff ChatStreakNotifyMode = "off"
)

// ValidChatStreakNotifyMode reports whether the mode is modelled. An unset or
// unrecognised value resolves to daily rather than silently muting a mechanic an
// operator believes is on.
func ValidChatStreakNotifyMode(mode ChatStreakNotifyMode) bool {
	switch mode {
	case ChatStreakNotifyDaily, ChatStreakNotifyMilestones, ChatStreakNotifyOff:
		return true
	default:
		return false
	}
}

// ShouldNotifyChatStreak reports whether a streak move is worth a message under
// the given mode.
//
// An unchanged day never notifies no matter the mode: the same pair writing six
// times on Tuesday gets one "🔥" on Tuesday, not six. That is the difference
// between a mechanic and a nuisance, and it is the only suppression rule that
// does not need configuration.
func ShouldNotifyChatStreak(mode ChatStreakNotifyMode, streak ChatStreak, outcome ChatStreakOutcome) bool {
	if outcome == ChatStreakUnchanged || streak.Days <= 0 {
		return false
	}
	switch mode {
	case ChatStreakNotifyOff:
		return false
	case ChatStreakNotifyMilestones:
		if streak.Days == 1 {
			return true
		}
		_, ok := ChatStreakMilestone(streak.Days)
		return ok
	default:
		return true
	}
}

// ChatStreakDayWord renders the Russian noun for a number of days with the
// right ending: 1 день, 2 дня, 5 дней, 11 дней, 21 день.
//
// Russian plurals are not "n == 1", and a streak feature that writes "1 дней"
// on the very first day it exists has already failed at being taken seriously.
func ChatStreakDayWord(days int) string {
	if days <= 0 {
		return "дней"
	}
	if days%10 == 1 && days%100 != 11 {
		return "день"
	}
	if days%10 >= 2 && days%10 <= 4 && (days%100 < 12 || days%100 > 14) {
		return "дня"
	}
	return "дней"
}

// ChatStreakNotificationBody is the text of the streak message for one move.
func ChatStreakNotificationBody(streak ChatStreak, outcome ChatStreakOutcome) string {
	days := streak.Days
	switch {
	case outcome == ChatStreakStarted:
		return "🔥 Огонёк: 1 день\nПишите друг другу каждый день — и он будет расти."
	case outcome == ChatStreakBroken:
		// Never announce a reset as if nothing happened. Somebody who had a
		// three-week run open to "🔥 Огонёк: 1 день" with no explanation reads
		// it as a bug -- or worse, as the number resetting at random.
		return "🔥 Огонёк начался заново: 1 день\nВы несколько дней не писали — счётчик сбросился. Завтра начинаем новую серию."
	case outcome == ChatStreakResumed:
		return fmt.Sprintf("🔥 Огонёк: %d %s\nЗаморозка выдержала пропуск.", days, ChatStreakDayWord(days))
	default:
		body := "🔥 Огонёк: " + strconv.Itoa(days) + " " + ChatStreakDayWord(days)
		if _, milestone := ChatStreakMilestone(days); milestone {
			body += "\nОтличная серия!"
		}
		return body
	}
}

// OfficialChatStreakMessage builds the account-visible incoming message that
// announces a streak move to one participant.
//
// It is deliberately shaped exactly like OfficialWelcomeMessage -- peer and from
// are the official system account, and the recipient owns the box -- so the
// client renders it as an ordinary incoming message with no special casing.
func OfficialChatStreakMessage(userID int64, streak ChatStreak, outcome ChatStreakOutcome, date int) (Message, error) {
	if userID <= 0 || IsSystemUserID(userID) || date < 0 || date > math.MaxInt32 {
		return Message{}, fmt.Errorf("chat streak notification: user=%d date=%d", userID, date)
	}
	body := ChatStreakNotificationBody(streak, outcome)
	if body == "" {
		return Message{}, fmt.Errorf("chat streak notification: empty body")
	}
	return Message{
		OwnerUserID: userID,
		Peer:        Peer{Type: PeerTypeUser, ID: OfficialSystemUserID},
		From:        Peer{Type: PeerTypeUser, ID: OfficialSystemUserID},
		Date:        date,
		Body:        body,
	}, nil
}
