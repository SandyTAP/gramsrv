package memory

import (
	"context"
	"slices"
	"testing"

	"telesrv/internal/domain"
)

// 回归：礼物实例行在所有权转移时被复用 —— id 保持不变、gift_date 被重置为
// 转移时刻。资料页必须按收到时刻倒序，否则被转送的礼物会停在旧 id 的位置，
// 而不是收礼人列表的顶部。
func TestStarGiftProfileOrdersByReceivedDate(t *testing.T) {
	ctx := context.Background()
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1002}
	store := NewStarGiftStore()

	// id 更小的行拿到最新的收到时刻（转移后的实例行）。
	transferred, err := store.Create(ctx, domain.SavedStarGift{
		Owner: owner, GiftID: 8001, RevisionID: 9001, MsgID: 200, Date: 1700000900,
	})
	if err != nil {
		t.Fatalf("create transferred: %v", err)
	}
	regular, err := store.Create(ctx, domain.SavedStarGift{
		Owner: owner, GiftID: 8001, RevisionID: 9001, MsgID: 201, Date: 1700000100,
	})
	if err != nil {
		t.Fatalf("create regular: %v", err)
	}

	page, err := store.ListByOwner(ctx, owner, false, "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []int64{transferred, regular}
	var got []int64
	for _, gift := range page.Gifts {
		got = append(got, gift.ID)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("date order = %v, want %v (newest received first)", got, want)
	}

	// 逐页行走：游标必须同时带日期，否则第二页按 id 续排会重复/漏项。
	got = got[:0]
	offset := ""
	for pageNumber := 0; ; pageNumber++ {
		paged, err := store.ListByOwner(ctx, owner, false, offset, 1)
		if err != nil {
			t.Fatalf("list page %d: %v", pageNumber, err)
		}
		if len(paged.Gifts) != 1 {
			t.Fatalf("page %d = %d gifts, want 1", pageNumber, len(paged.Gifts))
		}
		got = append(got, paged.Gifts[0].ID)
		if paged.NextOffset == "" {
			break
		}
		offset = paged.NextOffset
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged date order = %v, want %v", got, want)
	}
}

func TestStarGiftProfilePinOrderAndPagination(t *testing.T) {
	ctx := context.Background()
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	store := NewStarGiftStore()
	ids := make([]int64, 4)
	for i := range ids {
		id, err := store.Create(ctx, domain.SavedStarGift{
			Owner: owner, GiftID: 8001, RevisionID: 9001, MsgID: 100 + i, Date: 1700000000 + i,
		})
		if err != nil {
			t.Fatalf("create gift %d: %v", i, err)
		}
		ids[i] = id
	}

	if err := store.SetPinned(ctx, owner, []int64{ids[0], ids[2]}); err != nil {
		t.Fatalf("set pinned: %v", err)
	}

	want := []int64{ids[0], ids[2], ids[3], ids[1]}
	var got []int64
	offset := ""
	for pageNumber := 0; ; pageNumber++ {
		page, err := store.ListByOwner(ctx, owner, false, offset, 1)
		if err != nil {
			t.Fatalf("list page %d: %v", pageNumber, err)
		}
		if page.Count != len(ids) || len(page.Gifts) != 1 {
			t.Fatalf("page %d = %+v, want count=%d and one gift", pageNumber, page, len(ids))
		}
		got = append(got, page.Gifts[0].ID)
		if page.NextOffset == "" {
			break
		}
		offset = page.NextOffset
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged order = %v, want %v", got, want)
	}
	if ok, err := store.SetUnsaved(ctx, domain.SavedStarGiftRef{Owner: owner, MsgID: 100}, true); err != nil || !ok {
		t.Fatalf("hide pinned gift = %v err %v", ok, err)
	}
	hidden, found, err := store.GetByRef(ctx, domain.SavedStarGiftRef{Owner: owner, MsgID: 100})
	if err != nil || !found || !hidden.Unsaved || hidden.PinnedOrder != 0 {
		t.Fatalf("hidden pinned gift = %+v found %v err %v", hidden, found, err)
	}
	remaining, found, err := store.GetByRef(ctx, domain.SavedStarGiftRef{Owner: owner, MsgID: 102})
	if err != nil || !found || remaining.PinnedOrder != 1 {
		t.Fatalf("remaining pin = %+v found %v err %v", remaining, found, err)
	}
	if err := store.SetPinned(ctx, owner, []int64{ids[0], ids[2]}); err != nil {
		t.Fatalf("repin hidden gift: %v", err)
	}
	repinned, found, err := store.GetByRef(ctx, domain.SavedStarGiftRef{Owner: owner, MsgID: 100})
	if err != nil || !found || repinned.Unsaved || repinned.PinnedOrder != 1 {
		t.Fatalf("repinned gift = %+v found %v err %v", repinned, found, err)
	}

	if err := store.SetPinned(ctx, owner, nil); err != nil {
		t.Fatalf("clear pinned: %v", err)
	}
	page, err := store.ListByOwner(ctx, owner, false, "", 10)
	if err != nil {
		t.Fatalf("list after clear: %v", err)
	}
	want = []int64{ids[3], ids[2], ids[1], ids[0]}
	got = got[:0]
	for _, gift := range page.Gifts {
		got = append(got, gift.ID)
		if gift.PinnedOrder != 0 {
			t.Fatalf("gift %d pinned_order=%d after clear", gift.ID, gift.PinnedOrder)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("order after clear = %v, want %v", got, want)
	}
}
