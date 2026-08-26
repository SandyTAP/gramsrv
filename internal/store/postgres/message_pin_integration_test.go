package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestMessageStorePinPrivateMessageSharedAndOneside(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice, err := users.Create(ctx, domain.User{AccessHash: 81, Phone: "+1671" + suffix + "01", FirstName: "PinAlice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 82, Phone: "+1671" + suffix + "02", FirstName: "PinBob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{alice.ID, bob.ID})
	})

	messages := newTestMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    alice.ID,
		RecipientUserID: bob.ID,
		RandomID:        831001,
		Message:         "pin me",
		Date:            1700000600,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// 共享置顶：双侧翻转，各产生一条 pinned_messages 事件（自带账号 pts）。
	res, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID:   sent.SenderMessage.ID,
		Pinned:      true,
		Date:        1700000601,
	})
	if err != nil {
		t.Fatalf("shared pin: %v", err)
	}
	if len(res.Updated) != 2 {
		t.Fatalf("shared pin updated sides = %d, want 2", len(res.Updated))
	}
	self := res.Self()
	if !self.Pinned || len(self.MessageIDs) != 1 || self.MessageIDs[0] != sent.SenderMessage.ID || self.Event.Pts == 0 {
		t.Fatalf("self side = %+v, want alice box pinned with pts", self)
	}
	var bobSide domain.PinnedMessagesForUser
	for _, side := range res.Updated {
		if side.UserID == bob.ID {
			bobSide = side
		}
	}
	if len(bobSide.MessageIDs) != 1 || bobSide.MessageIDs[0] != sent.RecipientMessage.ID {
		t.Fatalf("bob side = %+v, want bob 视角 box id %d", bobSide, sent.RecipientMessage.ID)
	}
	events, err := NewUpdateEventStore(pool).ListAfter(ctx, bob.ID, bobSide.Event.Pts-1, 5)
	if err != nil || len(events) == 0 {
		t.Fatalf("bob events: %v %v", events, err)
	}
	if events[0].Type != domain.UpdateEventPinnedMessages || !events[0].Bool ||
		!sameInts(events[0].MessageIDs, []int{sent.RecipientMessage.ID}) ||
		events[0].Peer != (domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID}) {
		t.Fatalf("bob pinned event = %+v, want pinned_messages with own box id and alice peer", events[0])
	}

	// message.pinned 回填到历史读取。
	bobList, err := messages.ListByUser(ctx, bob.ID, domain.MessageFilter{
		HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID}, Limit: 10,
	})
	if err != nil || len(bobList.Messages) != 1 || !bobList.Messages[0].Pinned {
		t.Fatalf("bob list = %+v err %v, want pinned message", bobList.Messages, err)
	}
	pinnedOnly, err := messages.ListByUser(ctx, alice.ID, domain.MessageFilter{
		HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		PinnedOnly: true, NeedTotalCount: true, Limit: 5,
	})
	if err != nil || len(pinnedOnly.Messages) != 1 || pinnedOnly.Count != 1 {
		t.Fatalf("pinned-only list = %+v count %d err %v, want exactly the pinned message", pinnedOnly.Messages, pinnedOnly.Count, err)
	}

	// 幂等重复 pin：no-op，不烧 pts。
	repeat, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID:   sent.SenderMessage.ID,
		Pinned:      true,
		Date:        1700000602,
	})
	if err != nil || repeat.Changed() {
		t.Fatalf("repeat pin = %+v err %v, want no-op", repeat, err)
	}

	// unpin 双侧传播。
	unpin, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: bob.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID},
		MessageID:   sent.RecipientMessage.ID,
		Pinned:      false,
		Date:        1700000603,
	})
	if err != nil || len(unpin.Updated) != 2 {
		t.Fatalf("unpin = %+v err %v, want both sides cleared", unpin, err)
	}
	var pinnedCount int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*)::int FROM message_boxes
WHERE owner_user_id = ANY($1::bigint[]) AND pinned AND NOT deleted
`, []int64{alice.ID, bob.ID}).Scan(&pinnedCount); err != nil {
		t.Fatalf("count pinned: %v", err)
	}
	if pinnedCount != 0 {
		t.Fatalf("pinned rows after unpin = %d, want 0", pinnedCount)
	}

	// pm_oneside：仅本侧置顶，对端不动。
	oneside, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID:   sent.SenderMessage.ID,
		Pinned:      true,
		PmOneside:   true,
		Date:        1700000604,
	})
	if err != nil || len(oneside.Updated) != 1 || oneside.Updated[0].UserID != alice.ID {
		t.Fatalf("oneside pin = %+v err %v, want alice side only", oneside, err)
	}
	bobPinned, err := messages.ListByUser(ctx, bob.ID, domain.MessageFilter{
		HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID},
		PinnedOnly: true, Limit: 5,
	})
	if err != nil || len(bobPinned.Messages) != 0 {
		t.Fatalf("bob pinned after oneside = %+v err %v, want empty", bobPinned.Messages, err)
	}
}

func TestMessageStoreUnpinAllPrivateMessagesSweepsBothSides(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice, err := users.Create(ctx, domain.User{AccessHash: 83, Phone: "+1672" + suffix + "01", FirstName: "UnpinAlice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 84, Phone: "+1672" + suffix + "02", FirstName: "UnpinBob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{alice.ID, bob.ID})
	})

	messages := newTestMessageStore(pool)
	first, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: alice.ID, RecipientUserID: bob.ID, RandomID: 832001, Message: "shared", Date: 1700000610,
	})
	if err != nil {
		t.Fatalf("send shared: %v", err)
	}
	second, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: alice.ID, RecipientUserID: bob.ID, RandomID: 832002, Message: "oneside", Date: 1700000611,
	})
	if err != nil {
		t.Fatalf("send oneside: %v", err)
	}
	if _, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID: first.SenderMessage.ID, Pinned: true, Date: 1700000612,
	}); err != nil {
		t.Fatalf("shared pin: %v", err)
	}
	if _, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID: second.SenderMessage.ID, Pinned: true, PmOneside: true, Date: 1700000613,
	}); err != nil {
		t.Fatalf("oneside pin: %v", err)
	}

	res, err := messages.UnpinAllPrivateMessages(ctx, domain.UnpinAllPrivateMessagesRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		Date:        1700000614,
	})
	if err != nil {
		t.Fatalf("unpinAll: %v", err)
	}
	if len(res.Updated) != 2 {
		t.Fatalf("unpinAll sides = %d (%+v), want alice(2 ids) + bob(1 id)", len(res.Updated), res.Updated)
	}
	self := res.Self()
	if len(self.MessageIDs) != 2 || self.Pinned {
		t.Fatalf("alice unpinAll side = %+v, want both own pins cleared", self)
	}
	var bobSide domain.PinnedMessagesForUser
	for _, side := range res.Updated {
		if side.UserID == bob.ID {
			bobSide = side
		}
	}
	if len(bobSide.MessageIDs) != 1 || bobSide.MessageIDs[0] != first.RecipientMessage.ID {
		t.Fatalf("bob unpinAll side = %+v, want only shared pin cleared with bob 视角 id %d", bobSide, first.RecipientMessage.ID)
	}

	var pinnedCount int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*)::int FROM message_boxes
WHERE owner_user_id = ANY($1::bigint[]) AND pinned AND NOT deleted
`, []int64{alice.ID, bob.ID}).Scan(&pinnedCount); err != nil {
		t.Fatalf("count pinned: %v", err)
	}
	if pinnedCount != 0 {
		t.Fatalf("pinned rows after unpinAll = %d, want 0", pinnedCount)
	}

	// 再次 unpinAll：no-op。
	empty, err := messages.UnpinAllPrivateMessages(ctx, domain.UnpinAllPrivateMessagesRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		Date:        1700000615,
	})
	if err != nil || empty.Changed() {
		t.Fatalf("repeat unpinAll = %+v err %v, want no-op", empty, err)
	}
}
