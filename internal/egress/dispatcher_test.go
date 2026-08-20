package egress

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func newTestOutboxDispatcher(events store.UpdateEventStore, outbox store.DispatchOutboxStore, delivery edgecontrol.OutboxDeliverer, log *zap.Logger, opts ...OutboxOption) *OutboxDispatcher {
	testBuilder := WithOutboxUpdateBuilder(func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		out := make([][]byte, len(requests))
		for i, req := range requests {
			viewerUserID := req.TargetUserID
			if viewerUserID == 0 {
				viewerUserID = req.Event.UserID
			}
			out[i] = encodeTestOutboxUpdate(testOutboxUpdateForViewer(req.Event, viewerUserID))
		}
		return out, nil
	})
	opts = append([]OutboxOption{testBuilder}, opts...)
	return NewOutboxDispatcher(events, outbox, delivery, log, opts...)
}

func TestOutboxDispatcherPropagatesUpdateBuilderError(t *testing.T) {
	want := errors.New("encode failed")
	dispatcher := &OutboxDispatcher{updateBuilder: func(context.Context, []OutboxUpdateRequest) ([][]byte, error) {
		return nil, want
	}}
	if _, err := dispatcher.buildOutboxUpdates(context.Background(), []OutboxUpdateRequest{{TargetUserID: 1}}); !errors.Is(err, want) {
		t.Fatalf("buildOutboxUpdates err = %v, want %v", err, want)
	}
}

func TestOutboxDispatcherBuilderErrorFailsClaimWithoutDelivery(t *testing.T) {
	want := errors.New("encode outbox update: broken payload")
	const userID = int64(1000000002)
	event := domain.UpdateEvent{UserID: userID, Type: domain.UpdateEventPeerSettings, Pts: 9,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001}}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{event}}}
	captured := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID: 71, TargetUserID: userID, Pts: event.Pts, EventType: event.Type,
	}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: captured}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t),
		WithOutboxUpdateBuilder(func(context.Context, []OutboxUpdateRequest) ([][]byte, error) {
			return nil, want
		}),
	)
	if !dispatcher.DispatchOnce(context.Background()) {
		t.Fatal("DispatchOnce did not claim the test row")
	}
	if !captured.failed || captured.failedError != want.Error() {
		t.Fatalf("failed=%v error=%q, want failed with %q", captured.failed, captured.failedError, want)
	}
	if captured.delivered || len(outbox.deliveredBatch) != 0 {
		t.Fatalf("delivered=%v batch=%+v, builder failure must not mark delivered", captured.delivered, outbox.deliveredBatch)
	}
	if len(deliverer.requests) != 0 {
		t.Fatalf("delivery requests = %+v, want none", deliverer.requests)
	}
}

func TestOutboxDispatcherRejectsNilBuilderResultForNonNoop(t *testing.T) {
	const userID = int64(1000000002)
	event := domain.UpdateEvent{UserID: userID, Type: domain.UpdateEventPeerSettings, Pts: 9,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001}}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{event}}}
	captured := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID: 72, TargetUserID: userID, Pts: event.Pts, EventType: event.Type,
	}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: captured}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t),
		WithOutboxUpdateBuilder(func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
			return make([][]byte, len(requests)), nil
		}),
	)
	if !dispatcher.DispatchOnce(context.Background()) {
		t.Fatal("DispatchOnce did not claim the test row")
	}
	if !captured.failed || !strings.Contains(captured.failedError, errOutboxUpdateBuilderEmpty.Error()) {
		t.Fatalf("failed=%v error=%q, want empty non-noop builder failure", captured.failed, captured.failedError)
	}
	if captured.delivered || len(outbox.deliveredBatch) != 0 || len(deliverer.requests) != 0 {
		t.Fatalf("delivered=%v batch=%+v requests=%+v, empty non-noop must fail closed", captured.delivered, outbox.deliveredBatch, deliverer.requests)
	}
}

func testOutboxUpdateForViewer(event domain.UpdateEvent, viewerUserID int64) *tg.Updates {
	if viewerUserID == 0 {
		viewerUserID = event.UserID
	}
	switch event.Type {
	case domain.UpdateEventNoop:
		return nil
	case domain.UpdateEventNewMessage:
		if event.Message.Deleted {
			ids := []int{}
			if event.Message.ID > 0 && event.Message.ID <= domain.MaxMessageBoxID {
				ids = append(ids, event.Message.ID)
			}
			return &tg.Updates{
				Updates: []tg.UpdateClass{&tg.UpdateDeleteMessages{Messages: ids, Pts: event.Pts, PtsCount: event.PtsCount}},
				Date:    nonZeroInt(event.Date, event.Message.Date),
			}
		}
		users := make([]tg.UserClass, 0, len(event.Users))
		for _, u := range event.Users {
			users = append(users, &tg.User{ID: u.ID, FirstName: u.FirstName})
		}
		return &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:     event.Message.ID,
					PeerID: tgPeerForTest(event.Message.Peer),
					FromID: tgPeerForTest(event.Message.From),
					Date:   nonZeroInt(event.Message.Date, event.Date),
				},
				Pts:      event.Pts,
				PtsCount: event.PtsCount,
			}},
			Users: users,
			Date:  nonZeroInt(event.Date, event.Message.Date),
		}
	case domain.UpdateEventReadHistoryInbox:
		return &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateReadHistoryInbox{
				Peer:             &tg.PeerUser{UserID: event.Peer.ID},
				MaxID:            event.MaxID,
				StillUnreadCount: event.StillUnreadCount,
				Pts:              event.Pts,
				PtsCount:         event.PtsCount,
			}},
			Date: event.Date,
		}
	case domain.UpdateEventReadHistoryOutbox:
		return &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateReadHistoryOutbox{
				Peer:     &tg.PeerUser{UserID: event.Peer.ID},
				MaxID:    event.MaxID,
				Pts:      event.Pts,
				PtsCount: event.PtsCount,
			}},
			Date: event.Date,
		}
	default:
		return &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateDeleteMessages{Messages: []int{}, Pts: event.Pts, PtsCount: event.PtsCount}},
			Date:    event.Date,
		}
	}
}

func nonZeroInt(values ...int) int {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func tgPeerForTest(peer domain.Peer) tg.PeerClass {
	if peer.Type == domain.PeerTypeChannel {
		return &tg.PeerChannel{ChannelID: peer.ID}
	}
	return &tg.PeerUser{UserID: peer.ID}
}

func TestOutboxDispatcherPushesNewMessageAndMarksDelivered(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}
	deliverer := &captureOutboxDeliverer{}
	metrics := &captureOutboxMetrics{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.delivered || outbox.failed {
		t.Fatalf("delivered=%v failed=%v, want outbox delivered after Edge service ACK", outbox.delivered, outbox.failed)
	}
	req := deliverer.lastRequest()
	if req.TargetUserID != msg.OwnerUserID || req.ExcludeSessionID != 99 || req.MessageType != proto.MessageFromServer {
		t.Fatalf("push target = user %d exclude %d type %v, want outbox target/exclude", req.TargetUserID, req.ExcludeSessionID, req.MessageType)
	}
	updates := decodeTestOutboxUpdate(t, req.UpdateBytes)
	if len(updates.Updates) != 1 || len(updates.Users) != 1 {
		t.Fatalf("updates = %+v, want one update and sender user", updates)
	}
	update, ok := updates.Updates[0].(*tg.UpdateNewMessage)
	if !ok || update.Pts != msg.Pts {
		t.Fatalf("update = %#v, want UpdateNewMessage pts=%d", updates.Updates[0], msg.Pts)
	}
	if metrics.claimed != 1 || metrics.delivered != 1 || metrics.failed != 0 {
		t.Fatalf("metrics = claimed %d delivered %d failed %d, want 1/1/0", metrics.claimed, metrics.delivered, metrics.failed)
	}
}

func TestOutboxDispatcherUsesScopedAuthKeyExclusion(t *testing.T) {
	var excludeAuthKeyID [8]byte
	excludeAuthKeyID[0] = 7
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               57,
		TargetUserID:     1000000002,
		Pts:              9,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: excludeAuthKeyID,
		ExcludeSessionID: 99,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   1000000002,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      9,
		PtsCount: 1,
		Date:     1700000302,
		Peer:     peer,
	}}}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	req := deliverer.lastRequest()
	if req.ExcludeAuthKeyID != excludeAuthKeyID || req.ExcludeSessionID != 99 || req.TargetUserID != 1000000002 {
		t.Fatalf("scoped push = auth %x session %d user %d, want precise outbox exclusion", req.ExcludeAuthKeyID, req.ExcludeSessionID, req.TargetUserID)
	}
}

func TestOutboxDispatcherRejectsPartialSessionExclusion(t *testing.T) {
	tests := []struct {
		name      string
		authKeyID [8]byte
		sessionID int64
	}{
		{name: "auth key only", authKeyID: [8]byte{1}},
		{name: "session only", sessionID: 99},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const userID = int64(1000000002)
			outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
				ID:               58,
				TargetUserID:     userID,
				Pts:              10,
				EventType:        domain.UpdateEventPeerSettings,
				ExcludeAuthKeyID: tt.authKeyID,
				ExcludeSessionID: tt.sessionID,
			}}}
			events := &captureUpdateEventStore{}
			deliverer := &captureOutboxDeliverer{}
			metrics := &captureOutboxMetrics{}
			dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
			dispatcher.DispatchOnce(context.Background())

			if !outbox.failed || outbox.delivered {
				t.Fatalf("outbox failed=%v delivered=%v, want failed without delivery", outbox.failed, outbox.delivered)
			}
			if outbox.failedError != ErrInvalidOutboxExclusionPair.Error() {
				t.Fatalf("failed error = %q, want %q", outbox.failedError, ErrInvalidOutboxExclusionPair)
			}
			if len(deliverer.requests) != 0 {
				t.Fatalf("invalid exclusion unexpectedly pushed %+v", deliverer.requests)
			}
			if metrics.failed != 1 || metrics.delivered != 0 {
				t.Fatalf("metrics failed=%d delivered=%d, want 1/0", metrics.failed, metrics.delivered)
			}
		})
	}
}

func TestOutboxDispatcherBatchRejectsPartialExclusionBeforeNoop(t *testing.T) {
	const userID = int64(1000000002)
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID: userID,
		Type:   domain.UpdateEventNoop,
		Pts:    10,
	}}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               59,
		TargetUserID:     userID,
		Pts:              10,
		EventType:        domain.UpdateEventNoop,
		ExcludeAuthKeyID: [8]byte{1},
	}}}}
	dispatcher := newTestOutboxDispatcher(events, outbox, &captureOutboxDeliverer{}, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.failed || outbox.delivered || len(outbox.deliveredBatch) != 0 {
		t.Fatalf("batch invalid pair failed=%v delivered=%v batch=%v", outbox.failed, outbox.delivered, outbox.deliveredBatch)
	}
	if outbox.failedError != ErrInvalidOutboxExclusionPair.Error() {
		t.Fatalf("failed error = %q, want %q", outbox.failedError, ErrInvalidOutboxExclusionPair)
	}
	if len(events.batchCursors) != 0 {
		t.Fatalf("invalid pair reached batch event loader: %+v", events.batchCursors)
	}
}

func TestOutboxDispatcherBatchPath(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
	}}}}
	deliverer := &captureOutboxDeliverer{}
	metrics := &captureOutboxMetrics{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if len(events.batchCursors) != 1 || events.batchCursors[0] != (store.EventCursor{UserID: msg.OwnerUserID, Pts: msg.Pts}) {
		t.Fatalf("batch cursors = %+v, want one cursor for (%d,%d)", events.batchCursors, msg.OwnerUserID, msg.Pts)
	}
	req := deliverer.lastRequest()
	if req.TargetUserID != msg.OwnerUserID || req.ExcludeSessionID != 99 {
		t.Fatalf("push target = user %d exclude %d, want batch push to outbox target", req.TargetUserID, req.ExcludeSessionID)
	}
	if len(outbox.deliveredBatch) != 1 || outbox.deliveredBatch[0].ID != 55 {
		t.Fatalf("delivered batch = %+v, want one item id=55", outbox.deliveredBatch)
	}
	if outbox.delivered {
		t.Fatalf("batch path should not call per-item MarkDelivered")
	}
	if metrics.claimed != 1 || metrics.delivered != 1 || metrics.failed != 0 {
		t.Fatalf("metrics = claimed %d delivered %d failed %d, want 1/1/0", metrics.claimed, metrics.delivered, metrics.failed)
	}
}

func TestOutboxDispatcherBatchPathUsesUpdateBuilder(t *testing.T) {
	items := []store.DispatchOutboxItem{
		{ID: 3, TargetUserID: 1000000003, Pts: 12, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 1, TargetUserID: 1000000002, Pts: 10, EventType: domain.UpdateEventReadHistoryInbox},
	}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{
		outboxReadEvent(1000000002, 10),
		outboxReadEvent(1000000003, 12),
	}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: items}}
	deliverer := &orderedOutboxDeliverer{}
	var gotRequests []OutboxUpdateRequest
	builder := func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		gotRequests = append([]OutboxUpdateRequest(nil), requests...)
		out := make([][]byte, len(requests))
		for i, req := range requests {
			out[i] = encodeTestOutboxUpdate(testOutboxUpdateForViewer(req.Event, req.TargetUserID))
		}
		return out, nil
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxUpdateBuilder(builder))
	dispatcher.DispatchOnce(context.Background())

	if len(gotRequests) != 2 {
		t.Fatalf("builder requests = %+v, want two requests", gotRequests)
	}
	if gotRequests[0].TargetUserID != 1000000002 || gotRequests[0].Event.Pts != 10 || gotRequests[1].TargetUserID != 1000000003 || gotRequests[1].Event.Pts != 12 {
		t.Fatalf("builder requests = %+v, want sorted by target user then pts", gotRequests)
	}
	if got := deliverer.pushedPts(); !reflect.DeepEqual(got, []int{10, 12}) {
		t.Fatalf("pushed pts = %v, want builder updates in sorted order", got)
	}
	if len(outbox.deliveredBatch) != 2 {
		t.Fatalf("delivered batch = %+v, want two service-confirmed items", outbox.deliveredBatch)
	}
}

func TestOutboxDispatcherBatchBuilderFailureIsolatesSuccessfulSingletons(t *testing.T) {
	items := []store.DispatchOutboxItem{
		{ID: 2, TargetUserID: 1000000003, Pts: 12, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 1, TargetUserID: 1000000002, Pts: 10, EventType: domain.UpdateEventReadHistoryInbox},
	}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{
		outboxReadEvent(1000000002, 10),
		outboxReadEvent(1000000003, 12),
	}}}
	captured := &captureDispatchOutbox{items: items}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: captured}
	deliverer := &orderedOutboxDeliverer{}
	var builderBatchSizes []int
	builder := func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		builderBatchSizes = append(builderBatchSizes, len(requests))
		if len(requests) > 1 {
			return nil, errors.New("aggregate projection exceeded capacity")
		}
		return [][]byte{encodeTestOutboxUpdate(testOutboxUpdateForViewer(requests[0].Event, requests[0].TargetUserID))}, nil
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxUpdateBuilder(builder))
	dispatcher.DispatchOnce(context.Background())

	if got, want := builderBatchSizes, []int{2, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("builder batch sizes = %v, want %v", got, want)
	}
	if got, want := deliverer.pushedPts(), []int{10, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pushed pts = %v, want %v", got, want)
	}
	if len(captured.deliveredItems) != 2 || len(captured.failedItems) != 0 {
		t.Fatalf("delivered=%+v failed=%+v, want both isolated singletons delivered", captured.deliveredItems, captured.failedItems)
	}
	if len(outbox.deliveredBatch) != 0 {
		t.Fatalf("delivered batch = %+v, recovery must use fenced singleton completion", outbox.deliveredBatch)
	}
	if events.listAfterCalls != 0 {
		t.Fatalf("ListAfter calls = %d, want hydrated BatchByCursor events reused", events.listAfterCalls)
	}
}

func TestOutboxDispatcherBatchBuilderFailureOnlyFailsBadSingletonAndBlocksLane(t *testing.T) {
	const (
		blockedUser = int64(1000000002)
		otherUser   = int64(1000000003)
	)
	items := []store.DispatchOutboxItem{
		{ID: 12, TargetUserID: blockedUser, Pts: 12, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 5, TargetUserID: otherUser, Pts: 5, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 11, TargetUserID: blockedUser, Pts: 11, EventType: domain.UpdateEventReadHistoryInbox},
	}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{
		outboxReadEvent(blockedUser, 11),
		outboxReadEvent(blockedUser, 12),
		outboxReadEvent(otherUser, 5),
	}}}
	captured := &captureDispatchOutbox{items: items}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: captured}
	deliverer := &orderedOutboxDeliverer{}
	badSingleton := errors.New("malformed event projection")
	var singletonPts []int
	builder := func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		if len(requests) > 1 {
			return nil, errors.New("aggregate projection failed")
		}
		singletonPts = append(singletonPts, requests[0].Event.Pts)
		if requests[0].TargetUserID == blockedUser && requests[0].Event.Pts == 11 {
			return nil, badSingleton
		}
		return [][]byte{encodeTestOutboxUpdate(testOutboxUpdateForViewer(requests[0].Event, requests[0].TargetUserID))}, nil
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxUpdateBuilder(builder))
	dispatcher.DispatchOnce(context.Background())

	if got, want := singletonPts, []int{11, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("singleton pts = %v, want %v (pts=12 must stay behind the failed lane head)", got, want)
	}
	if got, want := deliverer.pushedPts(), []int{5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pushed pts = %v, want only unrelated user %v", got, want)
	}
	if len(captured.failedItems) != 1 || captured.failedItems[0].ID != 11 || captured.failedError != badSingleton.Error() {
		t.Fatalf("failed=%+v error=%q, want only id=11 with %q", captured.failedItems, captured.failedError, badSingleton)
	}
	if len(captured.deliveredItems) != 1 || captured.deliveredItems[0].ID != 5 {
		t.Fatalf("delivered=%+v, want only unrelated id=5", captured.deliveredItems)
	}
	if len(outbox.deliveredBatch) != 0 {
		t.Fatalf("delivered batch = %+v, recovery must use fenced singleton completion", outbox.deliveredBatch)
	}
	if events.listAfterCalls != 0 {
		t.Fatalf("ListAfter calls = %d, want hydrated BatchByCursor events reused", events.listAfterCalls)
	}
}

func TestOutboxDispatcherOrdersClaimedItemsByUserPts(t *testing.T) {
	const targetUserID int64 = 1000000002
	items := []store.DispatchOutboxItem{
		{ID: 3, TargetUserID: targetUserID, Pts: 12, EventType: domain.UpdateEventNewMessage},
		{ID: 1, TargetUserID: targetUserID, Pts: 10, EventType: domain.UpdateEventNewMessage},
		{ID: 2, TargetUserID: targetUserID, Pts: 11, EventType: domain.UpdateEventNewMessage},
	}
	events := make([]domain.UpdateEvent, 0, len(items))
	for _, pts := range []int{10, 11, 12} {
		msg := domain.Message{
			ID:          pts,
			OwnerUserID: targetUserID,
			Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
			From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
			Date:        1700000300 + pts,
			Body:        "ordered",
			Pts:         pts,
		}
		events = append(events, domain.UpdateEvent{
			UserID:   targetUserID,
			Type:     domain.UpdateEventNewMessage,
			Pts:      pts,
			PtsCount: 1,
			Date:     msg.Date,
			Message:  msg,
			Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
		})
	}
	eventStore := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: events}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: items}}
	deliverer := &orderedOutboxDeliverer{}
	dispatcher := newTestOutboxDispatcher(eventStore, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	want := []int{10, 11, 12}
	if got := deliverer.pushedPts(); !reflect.DeepEqual(got, want) {
		t.Fatalf("pushed pts = %v, want %v", got, want)
	}
	if got := eventStore.batchCursors; len(got) != len(want) {
		t.Fatalf("batch cursors = %+v, want %d cursors", got, len(want))
	} else {
		for i, cursor := range got {
			if cursor.UserID != targetUserID || cursor.Pts != want[i] {
				t.Fatalf("batch cursor[%d] = %+v, want user=%d pts=%d", i, cursor, targetUserID, want[i])
			}
		}
	}
}

func TestOutboxLogicalShardsAreDisjointAndStable(t *testing.T) {
	for _, workers := range []int{1, 2, 4, 7, 64, outboxLogicalShards} {
		seen := make([]int, outboxLogicalShards)
		for worker := 0; worker < workers; worker++ {
			for _, shard := range logicalShardsForWorker(worker, workers) {
				if shard < 0 || shard >= outboxLogicalShards {
					t.Fatalf("workers=%d worker=%d returned invalid shard %d", workers, worker, shard)
				}
				seen[shard]++
			}
		}
		for shard, owners := range seen {
			if owners != 1 {
				t.Fatalf("workers=%d shard=%d owners=%d, want exactly one", workers, shard, owners)
			}
		}
	}
	if got := normalizedOutboxWorkers(8, false); got != 1 {
		t.Fatalf("non-sharded workers = %d, want 1", got)
	}
	if got := normalizedOutboxWorkers(outboxLogicalShards+100, true); got != outboxLogicalShards {
		t.Fatalf("overprovisioned workers = %d, want clamp %d", got, outboxLogicalShards)
	}
}

func TestOutboxDispatcherBatchFailureBlocksHigherUserPts(t *testing.T) {
	const (
		blockedUser = int64(1000000002)
		otherUser   = int64(1000000003)
	)
	items := []store.DispatchOutboxItem{
		{ID: 12, TargetUserID: blockedUser, Pts: 12, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 5, TargetUserID: otherUser, Pts: 5, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 11, TargetUserID: blockedUser, Pts: 11, EventType: domain.UpdateEventReadHistoryInbox},
	}
	events := make([]domain.UpdateEvent, 0, len(items))
	for _, item := range items {
		events = append(events, outboxReadEvent(item.TargetUserID, item.Pts))
	}
	eventStore := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: events}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: items}}
	deliverer := &selectiveFailOutboxDeliverer{failUserID: blockedUser, failPts: 11}
	dispatcher := newTestOutboxDispatcher(eventStore, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	wantAttempts := []outboxPushAttempt{{userID: blockedUser, pts: 11}, {userID: otherUser, pts: 5}}
	if got := deliverer.pushAttempts(); !reflect.DeepEqual(got, wantAttempts) {
		t.Fatalf("push attempts = %+v, want %+v (blocked user's pts=12 must not overtake failed pts=11)", got, wantAttempts)
	}
	if !outbox.failed || len(outbox.deliveredBatch) != 1 || outbox.deliveredBatch[0].TargetUserID != otherUser {
		t.Fatalf("outbox failed=%v delivered=%+v, want failed head and only other user delivered", outbox.failed, outbox.deliveredBatch)
	}
}

func TestOutboxDispatcherBatchLoadDegradedPathStillBlocksHigherUserPts(t *testing.T) {
	const (
		blockedUser = int64(1000000004)
		otherUser   = int64(1000000005)
	)
	items := []store.DispatchOutboxItem{
		{ID: 22, TargetUserID: blockedUser, Pts: 22, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 21, TargetUserID: blockedUser, Pts: 21, EventType: domain.UpdateEventReadHistoryInbox},
		{ID: 6, TargetUserID: otherUser, Pts: 6, EventType: domain.UpdateEventReadHistoryInbox},
	}
	events := make([]domain.UpdateEvent, 0, len(items))
	for _, item := range items {
		events = append(events, outboxReadEvent(item.TargetUserID, item.Pts))
	}
	eventStore := &failingBatchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: events}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: items}}
	deliverer := &selectiveFailOutboxDeliverer{failUserID: blockedUser, failPts: 21}
	dispatcher := newTestOutboxDispatcher(eventStore, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	wantAttempts := []outboxPushAttempt{{userID: blockedUser, pts: 21}, {userID: otherUser, pts: 6}}
	if got := deliverer.pushAttempts(); !reflect.DeepEqual(got, wantAttempts) {
		t.Fatalf("degraded push attempts = %+v, want %+v", got, wantAttempts)
	}
}

func TestOutboxDispatcherUsesBoundedDeliveryPush(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}
	for _, tt := range []struct {
		name      string
		authKeyID [8]byte
		sessionID int64
	}{
		{name: "exclude origin", authKeyID: [8]byte{1}, sessionID: 99},
		{name: "exclude none"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
				ID:               55,
				TargetUserID:     msg.OwnerUserID,
				Pts:              msg.Pts,
				EventType:        domain.UpdateEventNewMessage,
				ExcludeAuthKeyID: tt.authKeyID,
				ExcludeSessionID: tt.sessionID,
			}}}
			deliverer := &captureOutboxDeliverer{}
			dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxPushTimeout(50*time.Millisecond))
			dispatcher.DispatchOnce(context.Background())

			if got := deliverer.lastRequest().DeliveryTimeout; got != 50*time.Millisecond {
				t.Fatalf("delivery timeout = %v, want 50ms", got)
			}
			if !outbox.delivered || outbox.failed {
				t.Fatalf("outbox delivered=%v failed=%v, want delivered after accepted bounded push", outbox.delivered, outbox.failed)
			}
		})
	}
}

func TestOutboxDispatcherUsesNoopAsDelivered(t *testing.T) {
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:           56,
		TargetUserID: 1000000002,
		Pts:          8,
		EventType:    domain.UpdateEventNoop,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID: 1000000002,
		Type:   domain.UpdateEventNoop,
		Pts:    8,
		Date:   1700000301,
	}}}
	metrics := &captureOutboxMetrics{}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.delivered || outbox.failed {
		t.Fatalf("noop delivered=%v failed=%v, want delivered without push", outbox.delivered, outbox.failed)
	}
	if len(deliverer.requests) != 0 {
		t.Fatalf("noop pushed unexpectedly: %+v", deliverer.requests)
	}
	if metrics.delivered != 1 {
		t.Fatalf("noop delivered metrics = %d, want 1", metrics.delivered)
	}
}

func TestOutboxDispatcherFailsClosedWhenUpdateBuilderMissing(t *testing.T) {
	const userID = int64(1000000002)
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:           60,
		TargetUserID: userID,
		Pts:          8,
		EventType:    domain.UpdateEventPeerSettings,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID: userID,
		Type:   domain.UpdateEventPeerSettings,
		Pts:    8,
		Date:   1700000300,
		Peer:   domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := NewOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.failed || outbox.delivered {
		t.Fatalf("outbox failed=%v delivered=%v, want failed closed", outbox.failed, outbox.delivered)
	}
	if outbox.failedError != errOutboxUpdateBuilderMissing.Error() {
		t.Fatalf("failed error = %q, want %q", outbox.failedError, errOutboxUpdateBuilderMissing)
	}
	if len(deliverer.requests) != 0 {
		t.Fatalf("missing builder unexpectedly delivered %+v", deliverer.requests)
	}
}

func TestOutboxDispatcherFailsClosedWhenUpdateBuilderMismatchesCount(t *testing.T) {
	const userID = int64(1000000002)
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:           61,
		TargetUserID: userID,
		Pts:          9,
		EventType:    domain.UpdateEventPeerSettings,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID: userID,
		Type:   domain.UpdateEventPeerSettings,
		Pts:    9,
		Date:   1700000300,
		Peer:   domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	deliverer := &captureOutboxDeliverer{}
	dispatcher := NewOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t),
		WithOutboxUpdateBuilder(func(context.Context, []OutboxUpdateRequest) ([][]byte, error) {
			return nil, nil
		}),
	)
	dispatcher.DispatchOnce(context.Background())

	if !outbox.failed || outbox.delivered {
		t.Fatalf("outbox failed=%v delivered=%v, want failed closed", outbox.failed, outbox.delivered)
	}
	if want := "outbox update builder returned mismatched count: got 0 want 1"; outbox.failedError != want {
		t.Fatalf("failed error = %q, want %q", outbox.failedError, want)
	}
	if len(deliverer.requests) != 0 {
		t.Fatalf("mismatched builder unexpectedly delivered %+v", deliverer.requests)
	}
}

func TestOutboxDispatcherDefersOnPushInterruption(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
	}}}
	deliverer := &interruptedOutboxDeliverer{}
	metrics := &captureOutboxMetrics{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxPushTimeout(50*time.Millisecond), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if deliverer.attempts != 1 {
		t.Fatalf("bounded push attempts = %d, want 1", deliverer.attempts)
	}
	if outbox.delivered {
		t.Fatalf("outbox delivered=true, want retained dispatching row for lease retry")
	}
	if outbox.failed {
		t.Fatalf("outbox failed=true, want interruption to stay retryable")
	}
	if metrics.failed != 0 {
		t.Fatalf("metrics.failed=%d, want 0", metrics.failed)
	}
}

func TestOutboxDispatcherDefersOnIndeterminateEdgeDelivery(t *testing.T) {
	const userID = int64(1000000002)
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   userID,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      7,
		PtsCount: 1,
		Date:     1700000300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     userID,
		Pts:              7,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         3,
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryIndeterminate},
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxPushTimeout(50*time.Millisecond))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("outbox deliverer calls=%d, want 1", len(deliverer.requests))
	}
	req := deliverer.lastRequest()
	if req.DeliveryTimeout != 50*time.Millisecond || req.TargetUserID != userID || req.ExcludeSessionID != 99 {
		t.Fatalf("outbox request = %+v, want target/exclusion/timeout propagated", req)
	}
	if got, want := req.DeliveryRef, (edgecontrol.OutboxDeliveryRef{OutboxID: 55, TargetUserID: userID, Pts: 7, Attempt: 3}); got != want {
		t.Fatalf("outbox delivery ref = %+v, want %+v", got, want)
	}
	if outbox.delivered || outbox.failed {
		t.Fatalf("outbox delivered=%v failed=%v, want lease retained for indeterminate Edge delivery", outbox.delivered, outbox.failed)
	}
}

func TestOutboxDispatcherAbandonsIndeterminateAtAttemptLimit(t *testing.T) {
	const userID = int64(1000000002)
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   userID,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      7,
		PtsCount: 1,
		Date:     1700000300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     userID,
		Pts:              7,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         maxOnlineDeliveryAttempts,
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryIndeterminate},
	}
	metrics := &captureOutboxMetrics{}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("outbox deliverer calls=%d, want 1 final attempt", len(deliverer.requests))
	}
	if !outbox.abandoned || outbox.abandonedID != 55 {
		t.Fatalf("outbox abandoned=%v id=%d, want exhausted online task abandoned", outbox.abandoned, outbox.abandonedID)
	}
	if outbox.delivered || outbox.failed {
		t.Fatalf("outbox delivered=%v failed=%v, want abandon only", outbox.delivered, outbox.failed)
	}
	if !errors.Is(onlineDeliveryAttemptsExhaustedError(maxOnlineDeliveryAttempts, edgecontrol.ErrOutboxDeliveryIndeterminate), errOutboxOnlineDeliveryAttemptsExhausted) {
		t.Fatal("attempt exhaustion error must be identifiable")
	}
	if metrics.failed != 1 {
		t.Fatalf("metrics.failed=%d, want 1", metrics.failed)
	}
}

func TestOutboxDispatcherAbandonsExceededOnlineAttemptsWithoutPush(t *testing.T) {
	const userID = int64(1000000002)
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   userID,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      7,
		PtsCount: 1,
		Date:     1700000300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     userID,
		Pts:              7,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         maxOnlineDeliveryAttempts + 1,
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryDelivered, Sent: 1},
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 0 {
		t.Fatalf("outbox deliverer calls=%d, want no push after exhausted online attempts", len(deliverer.requests))
	}
	if !outbox.abandoned {
		t.Fatal("exhausted online attempts did not abandon online task")
	}
	if outbox.delivered || outbox.failed {
		t.Fatalf("outbox delivered=%v failed=%v, want abandon only", outbox.delivered, outbox.failed)
	}
}

func TestOutboxDispatcherBatchAbandonsIndeterminateAtAttemptLimit(t *testing.T) {
	const userID = int64(1000000002)
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   userID,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      7,
		PtsCount: 1,
		Date:     1700000300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     userID,
		Pts:              7,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         maxOnlineDeliveryAttempts,
	}}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryIndeterminate},
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("outbox deliverer calls=%d, want 1 final attempt", len(deliverer.requests))
	}
	if !outbox.abandoned {
		t.Fatal("batch path did not abandon exhausted online task")
	}
	if len(outbox.deliveredBatch) != 0 {
		t.Fatalf("batch delivered=%d, want abandon outside completion batches", len(outbox.deliveredBatch))
	}
}

func TestOutboxDispatcherMarksNoKnownOnlineTargetsDelivered(t *testing.T) {
	const userID = int64(1000000002)
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   userID,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      8,
		PtsCount: 1,
		Date:     1700000300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:           56,
		TargetUserID: userID,
		Pts:          8,
		EventType:    domain.UpdateEventPeerSettings,
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryNoKnownOnlineTargets},
	}
	dispatcher := newTestOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("outbox deliverer calls=%d, want 1", len(deliverer.requests))
	}
	if !outbox.delivered || outbox.failed {
		t.Fatalf("outbox delivered=%v failed=%v, want no-online-target confirmation delivered", outbox.delivered, outbox.failed)
	}
}

func TestOutboxPushInterruptedRejectsDeterministicErrors(t *testing.T) {
	if !outboxPushInterrupted(context.Canceled) ||
		!outboxPushInterrupted(context.DeadlineExceeded) ||
		!outboxPushInterrupted(edgecontrol.ErrOutboxDeliveryIndeterminate) {
		t.Fatal("context shutdown/deadline must remain retriable")
	}
	if outboxPushInterrupted(errors.New("encode update: invalid constructor")) {
		t.Fatal("deterministic encoding error must fail the lane head instead of lease-retrying forever")
	}
}

type captureOutboxDeliverer struct {
	result   edgecontrol.OutboxPushResult
	err      error
	requests []edgecontrol.OutboxPushRequest
}

func (d *captureOutboxDeliverer) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.requests = append(d.requests, req)
	if d.err != nil || d.result.Status != edgecontrol.OutboxDeliveryUnknown || d.result.Sent != 0 {
		return d.result, d.err
	}
	return edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryDelivered}, nil
}

func (d *captureOutboxDeliverer) lastRequest() edgecontrol.OutboxPushRequest {
	if len(d.requests) == 0 {
		return edgecontrol.OutboxPushRequest{}
	}
	return d.requests[len(d.requests)-1]
}

type orderedOutboxDeliverer struct {
	captureOutboxDeliverer
	pushed []int
}

func (d *orderedOutboxDeliverer) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.requests = append(d.requests, req)
	d.pushed = append(d.pushed, firstOutboxUpdateBytesPts(req.UpdateBytes))
	return edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryDelivered}, nil
}

func (d *orderedOutboxDeliverer) pushedPts() []int {
	return append([]int(nil), d.pushed...)
}

type outboxPushAttempt struct {
	userID int64
	pts    int
}

type selectiveFailOutboxDeliverer struct {
	failUserID int64
	failPts    int
	attempts   []outboxPushAttempt
}

func (d *selectiveFailOutboxDeliverer) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	pts := firstOutboxUpdateBytesPts(req.UpdateBytes)
	d.attempts = append(d.attempts, outboxPushAttempt{userID: req.TargetUserID, pts: pts})
	if req.TargetUserID == d.failUserID && pts == d.failPts {
		return edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryUnknown}, errors.New("injected outbox push failure")
	}
	return edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryDelivered}, nil
}

func (d *selectiveFailOutboxDeliverer) pushAttempts() []outboxPushAttempt {
	return append([]outboxPushAttempt(nil), d.attempts...)
}

type interruptedOutboxDeliverer struct {
	attempts int
}

func (d *interruptedOutboxDeliverer) PushOutboxUpdate(context.Context, edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.attempts++
	return edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryIndeterminate}, context.DeadlineExceeded
}

func firstOutboxUpdatePts(updates *tg.Updates) int {
	if updates == nil || len(updates.Updates) == 0 {
		return 0
	}
	switch update := updates.Updates[0].(type) {
	case *tg.UpdateNewMessage:
		return update.Pts
	case *tg.UpdateReadHistoryInbox:
		return update.Pts
	case *tg.UpdateReadHistoryOutbox:
		return update.Pts
	default:
		return 0
	}
}

func firstOutboxUpdateBytesPts(raw []byte) int {
	decoded, err := edgecontrol.DecodeOutboxUpdate(raw)
	if err != nil {
		return 0
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok {
		return 0
	}
	return firstOutboxUpdatePts(updates)
}

func encodeTestOutboxUpdate(update *tg.Updates) []byte {
	if update == nil {
		return nil
	}
	raw, err := edgecontrol.EncodeOutboxUpdate(update)
	if err != nil {
		panic(err)
	}
	return raw
}

func decodeTestOutboxUpdate(t *testing.T, raw []byte) *tg.Updates {
	t.Helper()
	decoded, err := edgecontrol.DecodeOutboxUpdate(raw)
	if err != nil {
		t.Fatalf("DecodeOutboxUpdate: %v", err)
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok {
		t.Fatalf("decoded update = %T, want *tg.Updates", decoded)
	}
	return updates
}

func outboxReadEvent(userID int64, pts int) domain.UpdateEvent {
	return domain.UpdateEvent{
		UserID:           userID,
		Type:             domain.UpdateEventReadHistoryInbox,
		Pts:              pts,
		PtsCount:         1,
		Date:             1700000000 + pts,
		Peer:             domain.Peer{Type: domain.PeerTypeUser, ID: 999},
		MaxID:            pts,
		StillUnreadCount: 0,
	}
}

type batchEventStore struct {
	*captureUpdateEventStore
	batchCursors []store.EventCursor
}

type failingBatchEventStore struct {
	*captureUpdateEventStore
}

func (s *failingBatchEventStore) BatchByCursor(context.Context, []store.EventCursor) ([]domain.UpdateEvent, error) {
	return nil, errors.New("injected batch event load failure")
}

func (s *failingBatchEventStore) ListAfter(_ context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	var next domain.UpdateEvent
	for _, event := range s.events {
		if event.UserID != userID || event.Pts <= pts {
			continue
		}
		if next.Pts == 0 || event.Pts < next.Pts {
			next = event
		}
	}
	if next.Pts == 0 {
		return nil, nil
	}
	return []domain.UpdateEvent{next}, nil
}

func (s *batchEventStore) BatchByCursor(_ context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
	s.batchCursors = cursors
	out := make([]domain.UpdateEvent, 0, len(cursors))
	for _, c := range cursors {
		for _, event := range s.events {
			if event.UserID == c.UserID && event.Pts == c.Pts {
				out = append(out, event)
			}
		}
	}
	return out, nil
}

type batchDispatchOutbox struct {
	*captureDispatchOutbox
	deliveredBatch []store.DispatchOutboxItem
}

func (s *batchDispatchOutbox) MarkDeliveredBatch(_ context.Context, items []store.DispatchOutboxItem) error {
	s.deliveredBatch = append(s.deliveredBatch, items...)
	return nil
}

type captureUpdateEventStore struct {
	events         []domain.UpdateEvent
	listAfterCalls int
}

func (s *captureUpdateEventStore) Append(context.Context, int64, domain.UpdateEvent) error {
	return nil
}

func (s *captureUpdateEventStore) AppendAllocated(_ context.Context, userID int64, event domain.UpdateEvent) (domain.UpdateEvent, error) {
	if event.PtsCount <= 0 {
		event.PtsCount = 1
	}
	event.UserID = userID
	maxPts := 0
	for _, existing := range s.events {
		if existing.UserID == userID && existing.Pts > maxPts {
			maxPts = existing.Pts
		}
	}
	event.Pts = maxPts + event.PtsCount
	s.events = append(s.events, event)
	return event, nil
}

func (s *captureUpdateEventStore) ListAfter(_ context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error) {
	s.listAfterCalls++
	out := make([]domain.UpdateEvent, 0, len(s.events))
	for _, event := range s.events {
		if event.UserID != userID || event.Pts <= pts {
			continue
		}
		out = append(out, event)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *captureUpdateEventStore) MaxContiguousPts(context.Context, int64) (int, error) {
	present := make(map[int]struct{}, len(s.events))
	for _, event := range s.events {
		present[event.Pts] = struct{}{}
	}
	contiguous := 0
	for {
		if _, ok := present[contiguous+1]; !ok {
			break
		}
		contiguous++
	}
	return contiguous, nil
}

type captureDispatchOutbox struct {
	items           []store.DispatchOutboxItem
	delivered       bool
	deliveredUserID int64
	deliveredID     int64
	deliveredItems  []store.DispatchOutboxItem
	abandoned       bool
	abandonedID     int64
	abandonedError  string
	failed          bool
	failedError     string
	failedItems     []store.DispatchOutboxItem
}

func (s *captureDispatchOutbox) ClaimPending(context.Context, int) ([]store.DispatchOutboxItem, error) {
	items := s.items
	s.items = nil
	return items, nil
}

func (s *captureDispatchOutbox) MarkDelivered(_ context.Context, item store.DispatchOutboxItem) error {
	s.delivered = true
	s.deliveredUserID = item.TargetUserID
	s.deliveredID = item.ID
	s.deliveredItems = append(s.deliveredItems, item)
	return nil
}

func (s *captureDispatchOutbox) MarkAbandoned(_ context.Context, item store.DispatchOutboxItem, lastError string) error {
	s.abandoned = true
	s.abandonedID = item.ID
	s.abandonedError = lastError
	return nil
}

func (s *captureDispatchOutbox) MarkFailed(_ context.Context, item store.DispatchOutboxItem, lastError string) error {
	s.failed = true
	s.failedError = lastError
	s.failedItems = append(s.failedItems, item)
	return nil
}

func (s *captureDispatchOutbox) DeleteFailed(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

type captureOutboxMetrics struct {
	claimed   int
	delivered int
	failed    int
}

func (m *captureOutboxMetrics) OutboxClaimed(count int) {
	m.claimed += count
}

func (m *captureOutboxMetrics) OutboxDelivered(time.Duration) {
	m.delivered++
}

func (m *captureOutboxMetrics) OutboxFailed(error) {
	m.failed++
}
