package messages

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"telesrv/internal/app/userprojection"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service 提供消息历史、搜索与已读业务。
type Service struct {
	messages  store.MessageStore
	dialogs   store.DialogStore
	contacts  store.ContactStore
	photos    userprojection.ProfilePhotoProvider
	privacy   userprojection.PrivacyEvaluator
	freezes   userprojection.AccountFreezeProvider
	phones    userprojection.CollectiblePhoneProvider
	versions  store.ReadModelVersionStore
	projector *userprojection.Projector
	// viewerProjectionComplete is true only when every viewer-scoped user
	// overlay used by the shared RPC Users service is configured here too.
	// A partially configured service may still project the dependencies it has,
	// but RPC must not trust that partial envelope as authoritative.
	viewerProjectionComplete bool
	botResponder             BotResponder
	sendGate                 SendPermissionChecker
	// recipientUsers resolves the private recipient without viewer projection.
	// It is only a tombstone probe: viewer-scoped fields are irrelevant to
	// whether the peer still accepts writes.
	recipientUsers RecipientUserReader
	business       *businessAutomationConfig
	// chatStreaks advances the "огонёк" read model on every accepted private
	// message. Nil disables the mechanic entirely, which is what every test that
	// does not opt in relies on.
	//
	// It sits here rather than in the RPC layer because sendPrivateText is the
	// one chokepoint every private write passes through -- client sends,
	// forwards, bot replies, gift receipts, the Bot API gateway. A hook placed
	// in messages.sendMessage would silently miss all of them.
	chatStreaks ChatStreakAdvancer
	// chatStreakFailure observes a streak advance that did not land. The package
	// deliberately has no logger -- it is reached on the hot send path and every
	// existing failure here is reported by a callback the wiring site installs.
	chatStreakFailure func(context.Context, int64, int64, error)
	// chatStreakNotifyMode selects which streak moves announce themselves.
	// Empty means the domain default (daily); an unrecognised value is treated
	// as off rather than guessed, so a typo in config cannot turn a mechanic
	// into a spam machine.
	chatStreakNotifyMode domain.ChatStreakNotifyMode
	// chatStreakSenderUserID is the account the announcement is posted from.
	// Zero means the official system account, which is a safe default that works
	// before an operator has created anything. A dedicated bot reads better in
	// the chat list and can carry its own name.
	chatStreakSenderUserID int64

	privateMediaCountCache *privateMediaCountReadModelCache
}

// ChatStreakAdvancer is the minimal surface the send path needs from the
// streak mechanic: advance one pair and report what happened. The notification
// and the admin surfaces live behind their own dependencies, so the hottest
// path in the server pulls in nothing but the counter.
type ChatStreakAdvancer interface {
	AdvanceChatStreak(ctx context.Context, userA, userB int64, now time.Time) (domain.ChatStreak, domain.ChatStreakOutcome, error)
}

type SendPermissionChecker interface {
	CanSendMessages(ctx context.Context, userID int64) error
}

// RecipientUserReader is the minimal unprojected user lookup the send gate
// needs. There is no viewer: the answer only depends on the target row.
type RecipientUserReader interface {
	ByID(ctx context.Context, userID int64) (domain.User, bool, error)
}

// BotResponder 响应投递给服务端内置 bot（BotFather）的私聊消息。
// 实现方在用户消息已成功入库后被同步调用；回复失败只能记日志，
// 绝不允许影响用户消息发送结果。
type BotResponder interface {
	// HandlesBot 报告 botUserID 是否为该 responder 负责的内置 bot。
	HandlesBot(botUserID int64) bool
	// OnPrivateMessage 处理一条投递给内置 bot 的消息；msg 为 bot 视角收件 box 行。
	OnPrivateMessage(ctx context.Context, botUserID int64, msg domain.Message, session domain.ClientSessionMetadata)
}

// Option adjusts optional message service dependencies.
type Option func(*Service)

// WithContactStore enables viewer-specific user projection for message history.
func WithContactStore(c store.ContactStore) Option {
	return func(s *Service) { s.contacts = c }
}

// WithPhotoProvider enables current profile photo enrichment for message users.
func WithPhotoProvider(p userprojection.ProfilePhotoProvider) Option {
	return func(s *Service) { s.photos = p }
}

// WithPrivacyEvaluator enables viewer-specific privacy projection for message users.
func WithPrivacyEvaluator(p userprojection.PrivacyEvaluator) Option {
	return func(s *Service) { s.privacy = p }
}

func WithAccountFreezeProvider(p userprojection.AccountFreezeProvider) Option {
	return func(s *Service) { s.freezes = p }
}

func WithCollectiblePhoneProvider(p userprojection.CollectiblePhoneProvider) Option {
	return func(s *Service) { s.phones = p }
}

// WithBotResponder 启用服务端内置 bot（BotFather）对私聊消息的自动应答。
func WithBotResponder(r BotResponder) Option {
	return func(s *Service) { s.botResponder = r }
}

// WithChatStreaks enables the "огонёк" counter on the private send path.
// Passing nil disables it, which is the default: the mechanic must be wired
// explicitly so a test that constructs a Service without it cannot start writing
// rows it never asserts on.
func WithChatStreaks(a ChatStreakAdvancer) Option {
	return func(s *Service) { s.chatStreaks = a }
}

// WithChatStreakFailureObserver reports a streak advance that did not land.
// It never influences the send result: the message is durable by the time it is
// called, so the worst a streak failure can cost is one day on a counter.
func WithChatStreakFailureObserver(observer func(context.Context, int64, int64, error)) Option {
	return func(s *Service) { s.chatStreakFailure = observer }
}

// WithChatStreakNotifyMode chooses how loudly a streak announces itself.
// Unset keeps the domain default; an unmodelled value disables announcements
// rather than defaulting to the loudest option.
func WithChatStreakNotifyMode(mode domain.ChatStreakNotifyMode) Option {
	return func(s *Service) { s.chatStreakNotifyMode = mode }
}

// WithChatStreakSender selects the account a streak announcement is posted from.
// Zero keeps the official system account.
func WithChatStreakSender(userID int64) Option {
	return func(s *Service) { s.chatStreakSenderUserID = userID }
}

func WithSendPermissionChecker(c SendPermissionChecker) Option {
	return func(s *Service) { s.sendGate = c }
}

// WithRecipientUserReader wires the tombstone probe that blocks private writes to
// deleted accounts.
func WithRecipientUserReader(r RecipientUserReader) Option {
	return func(s *Service) { s.recipientUsers = r }
}

// WithReadModelVersions enables durable hash-token guarded media count caching.
func WithReadModelVersions(v store.ReadModelVersionStore) Option {
	return func(s *Service) { s.versions = v }
}

// NewService 创建 messages 服务。
func NewService(messages store.MessageStore, dialogs store.DialogStore, opts ...Option) *Service {
	s := &Service{
		messages:               messages,
		dialogs:                dialogs,
		privateMediaCountCache: newPrivateMediaCountReadModelCache(defaultPrivateMediaCountReadModelTTL),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.projector = userprojection.New(
		userprojection.WithContactStore(s.contacts),
		userprojection.WithPhotoProvider(s.photos),
		userprojection.WithPrivacyEvaluator(s.privacy),
		userprojection.WithAccountFreezeProvider(s.freezes),
		userprojection.WithCollectiblePhoneProvider(s.phones),
	)
	s.viewerProjectionComplete = s.contacts != nil && s.photos != nil && s.privacy != nil && s.freezes != nil && s.phones != nil
	return s
}

// ProjectsMessageUsersForViewer reports that history/search results returned by
// this service have already passed through the viewer-specific user projection
// boundary. RPC may reuse that envelope and resolve only nested message refs;
// raw stores and test doubles do not implicitly gain this trust marker.
func (s *Service) ProjectsMessageUsersForViewer() bool {
	return s != nil && s.viewerProjectionComplete
}

// SendPrivateText 发送一条私聊文本消息。
func (s *Service) SendPrivateText(ctx context.Context, userID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.SendPrivateTextResult{}, nil
	}
	if req.SenderUserID == 0 {
		req.SenderUserID = userID
	}
	if req.SenderUserID != userID {
		return domain.SendPrivateTextResult{}, domain.ErrAuthenticatedScopeInvalid
	}
	return s.sendPrivateText(ctx, userID, req)
}

// PostPrivateServiceMessage 让服务端自己落一条私聊服务消息，它的发送者不是当前
// 认证用户。Bot Stars 付款凭证属于这一类：消息来自付款人，落到 bot 的会话里，
// 而服务器此时 acting 的身份是 bot 本身，SendPrivateText 的发送者校验无法表达它。
// 不开认证作用域检查是安全的——调用方是服务端逻辑，没有请求方可控的入口。
func (s *Service) PostPrivateServiceMessage(ctx context.Context, recipientUserID, fromUserID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error) {
	if s == nil || s.messages == nil || recipientUserID == 0 || fromUserID == 0 {
		return domain.SendPrivateTextResult{}, domain.ErrMessageEmpty
	}
	if req.Media == nil || req.Media.Kind != domain.MessageMediaKindService {
		return domain.SendPrivateTextResult{}, domain.ErrMessageEmpty
	}
	req.SenderUserID = fromUserID
	req.RecipientUserID = recipientUserID
	return s.sendPrivateText(ctx, fromUserID, req)
}

func (s *Service) sendPrivateText(ctx context.Context, userID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error) {
	if err := domain.ValidateReplyMarkup(req.ReplyMarkup); err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	if req.RandomID != 0 && !req.IdempotencyPreflighted {
		fingerprint, err := store.PrivateSendFingerprint(req)
		if err != nil {
			return domain.SendPrivateTextResult{}, err
		}
		req.IdempotencyFingerprint = fingerprint
		if replayStore, ok := s.messages.(store.PrivateSendReplayStore); ok {
			replay, found, err := replayStore.LookupPrivateSendReplay(ctx, domain.PrivateSendReplayRequest{
				SenderUserID:           req.SenderUserID,
				RecipientUserID:        req.RecipientUserID,
				RandomID:               req.RandomID,
				IdempotencyFingerprint: fingerprint,
			})
			if err != nil || found {
				return replay, err
			}
			req.IdempotencyPreflighted = true
		}
	}
	if err := s.ensureCanSend(ctx, req.SenderUserID); err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	if err := s.ensureCanSendRecipient(ctx, req.RecipientUserID); err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	automation, automationOK := s.prepareBusinessAutomation(ctx, req)
	res, err := s.messages.SendPrivateText(ctx, req)
	if err == nil && !res.Duplicate && automationOK {
		s.runBusinessAutomation(ctx, req, res, automation)
	}
	// 内置 bot 应答：用户消息已提交（幂等重放除外）后同步触发；responder 自行
	// 兜错，不回传失败。bot 自己发出的消息不触发（SenderUserID 不会是内置 bot
	// 的对话对象集合里关心的方向——hook 只看收件人）。
	if err == nil && !res.Duplicate && req.BusinessAutomationKind == "" && s.botResponder != nil && s.botResponder.HandlesBot(req.RecipientUserID) {
		s.botResponder.OnPrivateMessage(ctx, req.RecipientUserID, res.RecipientMessage, req.OriginClientSession)
	}
	s.advanceChatStreak(ctx, req, res, err)
	return res, err
}

// advanceChatStreak advances the "огонёк" for a pair after a private message
// was accepted.
//
// Best effort by construction, and deliberately so: the message is already
// durable at this point with its own id and pts, so a streak failure can only
// cost a day on a counter. Failing the send instead would turn a cosmetic
// read model into a reason a user cannot write to their friend.
//
// Three guards keep the mechanic honest:
//
//   - A replayed send must not count. res.Duplicate means the message was
//     already stored by an earlier attempt with the same random_id, and
//     counting it would let a client that retries on a flaky connection inflate
//     the pair's streak on its own.
//   - Neither side may be a built-in service account. A streak is a
//     conversation between two people; a server service message -- a payment
//     receipt, a login notice -- lands on this same path and would otherwise
//     create a permanent day-1 streak nobody earned.
//   - Service messages are skipped outright for the same reason.
func (s *Service) advanceChatStreak(ctx context.Context, req domain.SendPrivateTextRequest, res domain.SendPrivateTextResult, sendErr error) {
	if s == nil || s.chatStreaks == nil || sendErr != nil || res.Duplicate {
		return
	}
	if req.Media != nil && req.Media.Kind == domain.MessageMediaKindService {
		return
	}
	if domain.IsSystemUserID(req.SenderUserID) || domain.IsSystemUserID(req.RecipientUserID) {
		return
	}
	// A note to self is ordinary usage, not an error. The store rejects a
	// self-pair, and letting that rejection reach the observer turned "Избранное"
	// -- which routes a send with sender == recipient -- into a steady stream of
	// WARN lines that all say the same thing.
	if req.SenderUserID == req.RecipientUserID {
		return
	}
	// A bot is not a person, on either side. The Bot API gateway sends through
	// this same path, so without checking both ends every bot message would mint
	// a day-1 streak between the bot and whoever it wrote to -- a number nobody
	// earned and nobody can grow.
	if s.isChatStreakBot(ctx, req.SenderUserID) || s.isChatStreakBot(ctx, req.RecipientUserID) {
		return
	}
	streak, outcome, err := s.chatStreaks.AdvanceChatStreak(ctx, req.SenderUserID, req.RecipientUserID, time.Now())
	if err != nil {
		if s.chatStreakFailure != nil {
			s.chatStreakFailure(ctx, req.SenderUserID, req.RecipientUserID, err)
		}
		return
	}
	s.notifyChatStreak(ctx, req, streak, outcome)
}

// chatStreakRandomID mints the random_id a streak announcement is sent under.
//
// It has to be unique per announcement, not per day: the private-send path keys
// its idempotency fingerprint on random_id, so reusing one would make the second
// participant's copy of the same announcement look like a replay of the first
// and silently never be written. crypto/rand because the value is an
// idempotency key, not a counter, and a predictable one would let a caller
// collide with a real send.
func chatStreakRandomID() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("chat streak random id: %w", err)
	}
	id := int64(binary.LittleEndian.Uint64(b[:]))
	if id == 0 {
		// Zero is reserved as "no random id" by the send path's validation.
		id = 1
	}
	return id, nil
}

// isChatStreakBot reports whether an account is a bot or already a tombstone.//
// The lookup is an unprojected PK read rather than a cached set: the set of bot
// ids is unbounded and changes whenever someone runs a bot, so any cache here
// would need invalidation that does not exist. It costs one indexed read on a
// path that already performs several, and only on the streak-enabled build.
// A lookup failure is answered "a real person": a transient read error must not
// silently retire somebody's streak.
//
// Deleted accounts are excluded for the same reason as bots. The tombstone is
// permanent, so a streak against one is a number that can only ever be frozen.
func (s *Service) isChatStreakBot(ctx context.Context, userID int64) bool {
	if s == nil || s.recipientUsers == nil {
		return false
	}
	user, found, err := s.recipientUsers.ByID(ctx, userID)
	if err != nil || !found {
		return false
	}
	return user.Bot || user.Deleted
}

// notifyChatStreak posts the streak announcement into both participants' private
// chats. The counter exists to be seen; without this the mechanic writes a row
// nobody can observe and the only symptom is that nothing appears to happen.
//
// Both sides are told because the streak is shared: the person who wrote did not
// earn it alone, and a badge only on the recipient's side would make the sender
// wonder what happened to their own number.
//
// It goes through the ordinary private-send path -- sendPrivateText -- and that
// is load-bearing rather than incidental. An earlier version called
// s.messages.Create and upserted the inbox by hand, mirroring the welcome
// message. That writes the row and bumps the dialog, and it is why the feature
// appeared to do nothing: a bare store write emits no entry in user_update_events,
// so no client is ever told the message exists. The row was there the whole time,
// invisible to every open client until something else happened to trigger a full
// dialog refetch. A streak badge nobody receives in real time is the same as no
// badge at all.
//
// Recursion: the announcement itself is a private send, so it re-enters this
// path. It is stopped upstream by isChatStreakBot -- the sender is a bot -- and
// the same-day rule would stop it even if that were not true.
func (s *Service) notifyChatStreak(ctx context.Context, req domain.SendPrivateTextRequest, streak domain.ChatStreak, outcome domain.ChatStreakOutcome) {
	if s == nil || s.messages == nil {
		return
	}
	mode := s.chatStreakNotifyMode
	if mode == "" {
		mode = domain.ChatStreakNotifyDaily
	}
	if !domain.ValidChatStreakNotifyMode(mode) || !domain.ShouldNotifyChatStreak(mode, streak, outcome) {
		return
	}
	senderUserID := s.chatStreakSenderUserID
	if senderUserID <= 0 {
		senderUserID = domain.OfficialSystemUserID
	}
	body := domain.ChatStreakNotificationBody(streak, outcome)
	if body == "" {
		return
	}
	date := int(time.Now().UTC().Unix())
	for _, userID := range [...]int64{req.SenderUserID, req.RecipientUserID} {
		randomID, err := chatStreakRandomID()
		if err != nil {
			return
		}
		// A failure here is reported and dropped: the streak is already recorded,
		// and losing an announcement must never cost the user their message.
		if _, err := s.sendPrivateText(ctx, senderUserID, domain.SendPrivateTextRequest{
			SenderUserID:    senderUserID,
			RecipientUserID: userID,
			RandomID:        randomID,
			Message:         body,
			Date:            date,
		}); err != nil && s.chatStreakFailure != nil {
			s.chatStreakFailure(ctx, senderUserID, userID, err)
		}
	}
}

// LookupPrivateSendReplay exposes the immutable receipt to the RPC boundary without executing
// send permission checks, business automation or bot responders. Sender identity is still bound
// to the authenticated app-service caller.
func (s *Service) LookupPrivateSendReplay(ctx context.Context, userID int64, req domain.PrivateSendReplayRequest) (domain.SendPrivateTextResult, bool, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.SendPrivateTextResult{}, false, nil
	}
	if req.SenderUserID == 0 {
		req.SenderUserID = userID
	}
	if req.SenderUserID != userID || req.RecipientUserID == 0 || req.RandomID == 0 {
		return domain.SendPrivateTextResult{}, false, fmt.Errorf("private send replay: invalid authenticated scope")
	}
	replayStore, ok := s.messages.(store.PrivateSendReplayStore)
	if !ok {
		return domain.SendPrivateTextResult{}, false, nil
	}
	return replayStore.LookupPrivateSendReplay(ctx, req)
}

func (s *Service) ensureCanSend(ctx context.Context, userID int64) error {
	if s == nil || s.sendGate == nil || userID == 0 {
		return nil
	}
	return s.sendGate.CanSendMessages(ctx, userID)
}

// SetChatTheme updates the shared private-chat theme and records the timeline service message.
// ensureCanSendRecipient blocks private writes to a tombstone. A deleted account
// keeps its rows for history, so the store happily accepts the message; without
// this gate the sender's dialog would show a delivered message nobody can ever
// read back. Rejecting with PEER_ID_INVALID is the official shape: the peer is
// no longer a writable contact.
func (s *Service) ensureCanSendRecipient(ctx context.Context, recipientUserID int64) error {
	if s == nil || recipientUserID == 0 || s.recipientUsers == nil {
		return nil
	}
	u, found, err := s.recipientUsers.ByID(ctx, recipientUserID)
	if err != nil {
		return err
	}
	if !found || u.Deleted {
		return domain.ErrPeerDeleted
	}
	return nil
}

func (s *Service) SetChatTheme(ctx context.Context, userID int64, req domain.SetPrivateChatThemeRequest) (domain.SetPrivateChatThemeResult, error) {
	out := domain.SetPrivateChatThemeResult{
		OwnerUserID: userID,
		Peer:        req.Peer,
		Emoticon:    req.Emoticon,
	}
	if s == nil || s.messages == nil || s.dialogs == nil || userID == 0 {
		return out, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 {
		return out, domain.ErrMessageIDInvalid
	}
	changedSelf, err := s.dialogs.SetChatTheme(ctx, userID, req.Peer, req.Emoticon)
	if err != nil {
		return out, err
	}
	otherPeer := domain.Peer{Type: domain.PeerTypeUser, ID: userID}
	changedPeer := false
	if req.Peer.ID != userID && !req.RecipientBlocked {
		changedPeer, err = s.dialogs.SetChatTheme(ctx, req.Peer.ID, otherPeer, req.Emoticon)
		if err != nil {
			return out, err
		}
	}
	out.Changed = changedSelf || changedPeer
	if !out.Changed {
		return out, nil
	}
	send, err := s.SendPrivateText(ctx, userID, domain.SendPrivateTextRequest{
		SenderUserID:     userID,
		RecipientUserID:  req.Peer.ID,
		RandomID:         chatThemeServiceMessageRandomID(userID, req.Peer.ID, req.Emoticon, req.Date),
		Media:            chatThemeServiceMedia(req.Emoticon),
		Silent:           true,
		Date:             req.Date,
		OriginAuthKeyID:  req.OriginAuthKeyID,
		OriginSessionID:  req.OriginSessionID,
		RecipientBlocked: req.RecipientBlocked,
	})
	if err != nil {
		return out, err
	}
	out.Send = send
	return out, nil
}

// GetPrivateNoForwards returns the canonical content-protection state for one
// ordinary private chat.
func (s *Service) GetPrivateNoForwards(ctx context.Context, userID, peerUserID int64) (domain.PrivateNoForwardsState, error) {
	if s == nil || s.messages == nil || userID == 0 || peerUserID == 0 || userID == peerUserID {
		return domain.PrivateNoForwardsState{}, domain.ErrMessageIDInvalid
	}
	backend, ok := s.messages.(store.PrivateNoForwardsStore)
	if !ok {
		return domain.PrivateNoForwardsState{}, nil
	}
	return backend.GetPrivateNoForwards(ctx, userID, peerUserID)
}

// TogglePrivateNoForwards atomically mutates the pair state and appends the
// corresponding service message when the official state machine requires one.
func (s *Service) TogglePrivateNoForwards(ctx context.Context, userID int64, req domain.TogglePrivateNoForwardsRequest) (domain.TogglePrivateNoForwardsResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.TogglePrivateNoForwardsResult{}, domain.ErrMessageIDInvalid
	}
	if req.ActorUserID == 0 {
		req.ActorUserID = userID
	}
	if req.ActorUserID != userID || req.PeerUserID == 0 || req.PeerUserID == userID ||
		req.RequestMsgID < 0 || req.RequestMsgID > domain.MaxMessageBoxID {
		return domain.TogglePrivateNoForwardsResult{}, domain.ErrMessageIDInvalid
	}
	if err := s.ensureCanSend(ctx, userID); err != nil {
		return domain.TogglePrivateNoForwardsResult{}, err
	}
	backend, ok := s.messages.(store.PrivateNoForwardsStore)
	if !ok {
		return domain.TogglePrivateNoForwardsResult{}, domain.ErrMessageIDInvalid
	}
	return backend.TogglePrivateNoForwards(ctx, req)
}

func chatThemeServiceMedia(emoticon string) *domain.MessageMedia {
	return &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind:              domain.MessageServiceActionSetChatTheme,
			ChatThemeEmoticon: emoticon,
		},
	}
}

func chatThemeServiceMessageRandomID(userID, peerUserID int64, emoticon string, date int) int64 {
	var id int64 = 0x43485448454d45
	id ^= userID << 21
	id ^= peerUserID << 7
	id ^= int64(date) << 33
	for _, r := range emoticon {
		id = (id << 5) - id + int64(r)
	}
	if id == 0 {
		return 0x43485401
	}
	return id
}

// ForwardPrivateMessages 转发当前账号可见的私聊文本消息。
func (s *Service) ForwardPrivateMessages(ctx context.Context, userID int64, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ForwardPrivateMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if err := s.ensureCanSend(ctx, req.OwnerUserID); err != nil {
		return domain.ForwardPrivateMessagesResult{OwnerUserID: req.OwnerUserID}, err
	}
	return s.messages.ForwardPrivateMessages(ctx, req)
}

// GetMessages returns exact owner-visible message boxes in request order.
func (s *Service) GetMessages(ctx context.Context, userID int64, ids []int) (domain.MessageList, error) {
	if s == nil || s.messages == nil || userID == 0 || len(ids) == 0 {
		return domain.MessageList{}, nil
	}
	list, err := s.messages.GetByIDs(ctx, userID, ids)
	if err != nil {
		return domain.MessageList{}, err
	}
	return s.projectMessageUsers(ctx, userID, list)
}

type messageByUIDStore interface {
	GetByUID(ctx context.Context, userID, uid int64) (domain.Message, bool, error)
}

// GetMessageByUID translates a shared private message id into one owner's exact box row.
// It is intentionally an optional capability so lightweight MessageStore test doubles that
// never exercise callback translation do not need a meaningless implementation.
func (s *Service) GetMessageByUID(ctx context.Context, userID, uid int64) (domain.Message, bool, error) {
	if s == nil || userID == 0 || uid == 0 {
		return domain.Message{}, false, nil
	}
	provider, ok := s.messages.(messageByUIDStore)
	if !ok {
		return domain.Message{}, false, nil
	}
	msg, found, err := provider.GetByUID(ctx, userID, uid)
	if err != nil || !found {
		return domain.Message{}, found, err
	}
	list, err := s.projectMessageUsers(ctx, userID, domain.MessageList{Messages: []domain.Message{msg}})
	if err != nil || len(list.Messages) != 1 {
		return domain.Message{}, false, err
	}
	return list.Messages[0], true, nil
}

// GetHistory 返回当前账号某个 peer 的历史消息。
func (s *Service) GetHistory(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	return s.list(ctx, userID, filter)
}

// Search 返回当前账号消息搜索结果。Query 为空时等价于历史列表过滤。
func (s *Service) Search(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	return s.list(ctx, userID, filter)
}

// SearchPrivateMedia 返回某私聊会话中属于给定媒体类别的消息(共享媒体标签页)。
func (s *Service) SearchPrivateMedia(ctx context.Context, userID, peerID int64, req domain.MediaSearchRequest) (domain.MessageList, error) {
	if s == nil || s.messages == nil || userID == 0 || peerID == 0 {
		return domain.MessageList{}, nil
	}
	list, err := s.messages.SearchPrivateMedia(ctx, userID, peerID, req)
	if err != nil {
		return domain.MessageList{}, err
	}
	return s.projectMessageUsers(ctx, userID, list)
}

// CountPrivateMediaCategories 返回某私聊会话按基础媒体类别聚合的精确计数。
func (s *Service) CountPrivateMediaCategories(ctx context.Context, userID, peerID int64) (domain.MediaCategoryCounts, error) {
	if s == nil || s.messages == nil || userID == 0 || peerID == 0 {
		return domain.MediaCategoryCounts{}, nil
	}
	return s.cachedPrivateMediaCounts(ctx, userID, peerID)
}

// ReadHistory 将当前账号某个 peer 的 inbox 标记为已读，并为发送方生成 outbox 已读回执。
func (s *Service) ReadHistory(ctx context.Context, userID int64, req domain.ReadHistoryRequest) (domain.ReadHistoryResult, error) {
	if s == nil || userID == 0 {
		return domain.ReadHistoryResult{Peer: req.Peer, MaxID: req.MaxID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if s.messages != nil {
		return s.messages.ReadHistory(ctx, req)
	}
	if s.dialogs != nil {
		return s.dialogs.MarkRead(ctx, userID, req.Peer, req.MaxID)
	}
	return domain.ReadHistoryResult{OwnerUserID: userID, Peer: req.Peer, MaxID: req.MaxID}, nil
}

// ReadMessageContents checks exact owner-visible private message IDs for content-read sync.
func (s *Service) ReadMessageContents(ctx context.Context, userID int64, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error) {
	res := domain.ReadMessageContentsResult{OwnerUserID: userID}
	if s == nil || s.messages == nil || userID == 0 {
		return res, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID || len(req.IDs) > domain.MaxGetMessageIDs {
		return res, domain.ErrMessageIDInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return res, domain.ErrMessageIDInvalid
		}
	}
	return s.messages.ReadMessageContents(ctx, req)
}

// GetOutboxReadDate 返回当前账号某条 outgoing 私聊消息被对端读到的时间。
func (s *Service) GetOutboxReadDate(ctx context.Context, userID int64, req domain.OutboxReadDateRequest) (int, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return 0, domain.ErrMessageIDInvalid
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.GetOutboxReadDate(ctx, req)
}

// SetMessageReactions replaces the current user's reactions on a visible private message.
func (s *Service) SetMessageReactions(ctx context.Context, userID int64, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	return s.messages.SetMessageReactions(ctx, req)
}

// VoteMessagePoll 给私聊消息上的 poll 投票（options 为空 = 撤票）。
func (s *Service) VoteMessagePoll(ctx context.Context, userID int64, req domain.VotePrivateMessagePollRequest) (domain.PrivateMessagePollResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	return s.messages.VoteMessagePoll(ctx, req)
}

// CloseMessagePoll 关闭私聊消息上的 poll（仅 poll 创建者）。
func (s *Service) CloseMessagePoll(ctx context.Context, userID int64, req domain.ClosePrivateMessagePollRequest) (domain.PrivateMessagePollResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	return s.messages.CloseMessagePoll(ctx, req)
}

// GetMessageReactions returns reaction summaries for visible private messages.
func (s *Service) GetMessageReactions(ctx context.Context, userID int64, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessageReactionsResult{}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.messages.GetMessageReactions(ctx, req)
}

// SavedReactionTags returns the global or one-sub-dialog Saved Messages tag list.
func (s *Service) SavedReactionTags(ctx context.Context, userID int64, savedPeer domain.Peer, limit int) ([]domain.SavedReactionTag, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > domain.MaxSavedReactionTags {
		limit = domain.MaxSavedReactionTags
	}
	return s.messages.ListSavedReactionTags(ctx, domain.SavedReactionTagsRequest{
		UserID:    userID,
		SavedPeer: savedPeer,
		Limit:     limit,
	})
}

// UpdateSavedReactionTag stores or removes the optional global title for one
// tag that is currently assigned to at least one visible Saved Message.
func (s *Service) UpdateSavedReactionTag(ctx context.Context, userID int64, tag domain.SavedReactionTag) error {
	if s == nil || s.messages == nil || userID == 0 || !tag.Reaction.Valid() {
		return domain.ErrReactionInvalid
	}
	tag.UserID = userID
	return s.messages.UpsertSavedReactionTag(ctx, tag)
}

// EditMessage 编辑当前账号发出的私聊文本消息。
func (s *Service) EditMessage(ctx context.Context, userID int64, req domain.EditMessageRequest) (domain.EditMessageResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.EditMessageResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.SetReplyMarkup {
		if err := domain.ValidateReplyMarkup(req.ReplyMarkup); err != nil {
			return domain.EditMessageResult{OwnerUserID: userID}, err
		}
	}
	return s.messages.EditMessage(ctx, req)
}

// PinPrivateMessage 翻转当前账号可见私聊消息的置顶状态；非 pm_oneside
// 时同步翻转对端视角。
func (s *Service) PinPrivateMessage(ctx context.Context, userID int64, req domain.PinPrivateMessageRequest) (domain.PinPrivateMessageResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PinPrivateMessageResult{OwnerUserID: userID}, domain.ErrMessageIDInvalid
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID {
		return domain.PinPrivateMessageResult{OwnerUserID: userID}, domain.ErrMessageIDInvalid
	}
	var serviceReq domain.SendPrivateTextRequest
	var automation businessAutomationContext
	var automationOK bool
	if req.Pinned && !req.PmOneside && req.Peer.ID != userID {
		if err := s.ensureCanSend(ctx, userID); err != nil {
			return domain.PinPrivateMessageResult{OwnerUserID: userID}, err
		}
		serviceReq = domain.SendPrivateTextRequest{
			SenderUserID: userID, RecipientUserID: req.Peer.ID, Date: req.Date, Silent: req.Silent,
			RecipientBlocked: req.RecipientBlocked, OriginAuthKeyID: req.OriginAuthKeyID, OriginSessionID: req.OriginSessionID,
			Media:   &domain.MessageMedia{Kind: domain.MessageMediaKindService, ServiceAction: &domain.MessageServiceAction{Kind: domain.MessageServiceActionPinMessage}},
			ReplyTo: &domain.MessageReply{MessageID: req.MessageID, Peer: req.Peer},
		}
		automation, automationOK = s.prepareBusinessAutomation(ctx, serviceReq)
	}
	res, err := s.messages.PinPrivateMessage(ctx, req)
	// Preserve normal private-send observers, only after the combined commit.
	if err == nil && res.ServiceMessage.SenderMessage.ID != 0 {
		serviceReq.RandomID = res.ServiceMessage.SenderMessage.RandomID
		serviceReq.Date = res.ServiceMessage.SenderMessage.Date
		if automationOK {
			s.runBusinessAutomation(ctx, serviceReq, res.ServiceMessage, automation)
		}
		if s.botResponder != nil && s.botResponder.HandlesBot(req.Peer.ID) {
			s.botResponder.OnPrivateMessage(ctx, req.Peer.ID, res.ServiceMessage.RecipientMessage, domain.ClientSessionMetadata{})
		}
	}
	return res, err
}

// UnpinAllPrivateMessages 清空当前账号与某私聊 peer 的全部置顶。
func (s *Service) UnpinAllPrivateMessages(ctx context.Context, userID int64, req domain.UnpinAllPrivateMessagesRequest) (domain.PinPrivateMessageResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PinPrivateMessageResult{OwnerUserID: userID}, domain.ErrMessageIDInvalid
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID {
		return domain.PinPrivateMessageResult{OwnerUserID: userID}, domain.ErrMessageIDInvalid
	}
	return s.messages.UnpinAllPrivateMessages(ctx, req)
}

// DeleteMessages 删除当前账号视角下的一组消息；revoke 时同步删除对端私聊盒子。
func (s *Service) DeleteMessages(ctx context.Context, userID int64, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.DeleteMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.DeleteMessages(ctx, req)
}

// DeleteHistory 清空当前账号与某个 peer 的历史；revoke 时同步删除对端私聊盒子。
func (s *Service) DeleteHistory(ctx context.Context, userID int64, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.DeleteMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.DeleteHistory(ctx, req)
}

// GetSavedDialogs 返回收藏夹子会话分页（messages.getSavedDialogs）。
func (s *Service) GetSavedDialogs(ctx context.Context, userID int64, filter domain.SavedDialogsFilter) (domain.SavedDialogList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.SavedDialogList{Full: true}, nil
	}
	return s.messages.ListSavedDialogs(ctx, userID, filter)
}

// GetPinnedSavedDialogs 返回全部置顶收藏夹子会话（messages.getPinnedSavedDialogs）。
func (s *Service) GetPinnedSavedDialogs(ctx context.Context, userID int64) (domain.SavedDialogList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.SavedDialogList{Full: true}, nil
	}
	return s.messages.ListPinnedSavedDialogs(ctx, userID)
}

// GetSavedDialogsByPeers 返回指定收藏夹子会话（messages.getSavedDialogsByID）。
func (s *Service) GetSavedDialogsByPeers(ctx context.Context, userID int64, peers []domain.Peer) (domain.SavedDialogList, error) {
	if s == nil || s.messages == nil || userID == 0 || len(peers) == 0 {
		return domain.SavedDialogList{Full: true}, nil
	}
	return s.messages.ListSavedDialogsByPeers(ctx, userID, peers)
}

// ToggleSavedDialogPin 翻转收藏夹子会话置顶状态，返回是否实际变化。
func (s *Service) ToggleSavedDialogPin(ctx context.Context, userID int64, peer domain.Peer, pinned bool) (bool, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return false, nil
	}
	return s.messages.ToggleSavedDialogPin(ctx, userID, peer, pinned)
}

// ReorderPinnedSavedDialogs 全量重排收藏夹置顶顺序。
func (s *Service) ReorderPinnedSavedDialogs(ctx context.Context, userID int64, order []domain.Peer, force bool) error {
	if s == nil || s.messages == nil || userID == 0 {
		return nil
	}
	return s.messages.ReorderPinnedSavedDialogs(ctx, userID, order, force)
}

// DeleteSavedHistory 删除收藏夹一个子会话的消息（单批）。
func (s *Service) DeleteSavedHistory(ctx context.Context, userID int64, req domain.DeleteSavedHistoryRequest) (domain.DeleteSavedHistoryResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.DeleteSavedHistoryResult{}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.DeleteSavedHistory(ctx, req)
}

func (s *Service) ScheduleMessage(ctx context.Context, userID int64, req domain.ScheduleMessageRequest) (domain.ScheduledMessage, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ScheduledMessage{}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return domain.ScheduledMessage{}, nil
	}
	return scheduled.CreateScheduledMessage(ctx, req)
}

func (s *Service) ListScheduledMessages(ctx context.Context, userID int64, filter domain.ScheduledMessageFilter) (domain.ScheduledMessageList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ScheduledMessageList{}, nil
	}
	if filter.OwnerUserID == 0 {
		filter.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return domain.ScheduledMessageList{}, nil
	}
	return scheduled.ListScheduledMessages(ctx, filter)
}

func (s *Service) EditScheduledMessage(ctx context.Context, userID int64, req domain.EditScheduledMessageRequest) (domain.ScheduledMessage, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ScheduledMessage{}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return domain.ScheduledMessage{}, nil
	}
	return scheduled.EditScheduledMessage(ctx, req)
}

func (s *Service) GetScheduledMessages(ctx context.Context, userID int64, filter domain.ScheduledMessageFilter) (domain.ScheduledMessageList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ScheduledMessageList{}, nil
	}
	if filter.OwnerUserID == 0 {
		filter.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return domain.ScheduledMessageList{}, nil
	}
	return scheduled.GetScheduledMessages(ctx, filter)
}

func (s *Service) DeleteScheduledMessages(ctx context.Context, userID int64, filter domain.ScheduledMessageFilter, date int) ([]domain.ScheduledMessage, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return nil, nil
	}
	if filter.OwnerUserID == 0 {
		filter.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return nil, nil
	}
	return scheduled.DeleteScheduledMessages(ctx, filter, date)
}

func (s *Service) ClaimScheduledMessages(ctx context.Context, userID int64, claim domain.ScheduledMessageClaim) ([]domain.ScheduledMessage, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return nil, nil
	}
	if claim.OwnerUserID == 0 {
		claim.OwnerUserID = userID
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return nil, nil
	}
	return scheduled.ClaimScheduledMessages(ctx, claim)
}

func (s *Service) ClaimDueScheduledMessages(ctx context.Context, now, limit, leaseSeconds int) ([]domain.ScheduledMessage, error) {
	if s == nil || s.messages == nil {
		return nil, nil
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return nil, nil
	}
	return scheduled.ClaimDueScheduledMessages(ctx, now, limit, leaseSeconds)
}

func (s *Service) MarkScheduledMessageSent(ctx context.Context, ownerUserID int64, id, sentMessageID, date int) error {
	if s == nil || s.messages == nil {
		return nil
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return nil
	}
	return scheduled.MarkScheduledMessageSent(ctx, ownerUserID, id, sentMessageID, date)
}

func (s *Service) ReleaseScheduledMessage(ctx context.Context, ownerUserID int64, id int, errText string) error {
	if s == nil || s.messages == nil {
		return nil
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return nil
	}
	return scheduled.ReleaseScheduledMessage(ctx, ownerUserID, id, errText)
}

func (s *Service) HasScheduledMessages(ctx context.Context, userID int64, peer domain.Peer) (bool, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return false, nil
	}
	scheduled, ok := s.messages.(store.ScheduledMessageStore)
	if !ok {
		return false, nil
	}
	return scheduled.HasScheduledMessages(ctx, userID, peer)
}

func (s *Service) GetPrivateHistoryTTL(ctx context.Context, userID int64, peer domain.Peer) (int, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return 0, nil
	}
	ttl, ok := s.messages.(store.HistoryTTLStore)
	if !ok {
		return 0, nil
	}
	return ttl.GetPrivateHistoryTTL(ctx, userID, peer)
}

func (s *Service) SetPrivateHistoryTTL(ctx context.Context, userID int64, peer domain.Peer, period int) error {
	if s == nil || s.messages == nil || userID == 0 {
		return nil
	}
	ttl, ok := s.messages.(store.HistoryTTLStore)
	if !ok {
		return nil
	}
	return ttl.SetPrivateHistoryTTL(ctx, userID, peer, period)
}

func (s *Service) DefaultHistoryTTL(ctx context.Context, userID int64) (int, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return 0, nil
	}
	ttl, ok := s.messages.(store.HistoryTTLStore)
	if !ok {
		return 0, nil
	}
	return ttl.DefaultHistoryTTL(ctx, userID)
}

func (s *Service) SetDefaultHistoryTTL(ctx context.Context, userID int64, period int) error {
	if s == nil || s.messages == nil || userID == 0 {
		return nil
	}
	ttl, ok := s.messages.(store.HistoryTTLStore)
	if !ok {
		return nil
	}
	return ttl.SetDefaultHistoryTTL(ctx, userID, period)
}

func (s *Service) ClaimExpiredPrivateMessages(ctx context.Context, now, limit int) ([]domain.DeleteMessagesRequest, error) {
	if s == nil || s.messages == nil {
		return nil, nil
	}
	ttl, ok := s.messages.(store.HistoryTTLStore)
	if !ok {
		return nil, nil
	}
	return ttl.ClaimExpiredPrivateMessages(ctx, now, limit)
}

func (s *Service) list(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.MessageList{}, nil
	}
	list, err := s.messages.ListByUser(ctx, userID, filter)
	if err != nil {
		return domain.MessageList{}, err
	}
	return s.projectMessageUsers(ctx, userID, list)
}

func (s *Service) projectMessageUsers(ctx context.Context, userID int64, list domain.MessageList) (domain.MessageList, error) {
	if s == nil || s.projector == nil {
		return list, nil
	}
	users, err := s.projector.ForViewer(ctx, userID, list.Users)
	if err != nil {
		return domain.MessageList{}, err
	}
	list.Users = users
	return list, nil
}

type privateUnreadReactionsStore interface {
	ListUnreadReactionMessages(ctx context.Context, ownerUserID int64, peer domain.Peer, limit int) ([]domain.Message, error)
	ReadPeerReactions(ctx context.Context, ownerUserID int64, peer domain.Peer) (int, error)
}

// ListUnreadReactionMessages 返回私聊 peer 下带未读 reaction 的消息。
func (s *Service) ListUnreadReactionMessages(ctx context.Context, userID int64, peer domain.Peer, limit int) ([]domain.Message, error) {
	store, ok := s.messages.(privateUnreadReactionsStore)
	if !ok || userID == 0 {
		return nil, nil
	}
	return store.ListUnreadReactionMessages(ctx, userID, peer, limit)
}

// ReadPeerReactions 清理私聊 peer 下的全部未读 reaction。
func (s *Service) ReadPeerReactions(ctx context.Context, userID int64, peer domain.Peer) (int, error) {
	store, ok := s.messages.(privateUnreadReactionsStore)
	if !ok || userID == 0 {
		return 0, nil
	}
	return store.ReadPeerReactions(ctx, userID, peer)
}
