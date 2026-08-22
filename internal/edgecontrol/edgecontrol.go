package edgecontrol

import (
	"context"
	"errors"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
)

var ErrOutboxDeliveryIndeterminate = errors.New("edgecontrol: outbox delivery indeterminate")

var ErrOutboxTargetUnavailable = errors.New("edgecontrol: outbox target instance unavailable")

var ErrLocationLeaseHeld = errors.New("edgecontrol: location lease held by another process")

var ErrLocationLeaseLost = errors.New("edgecontrol: location lease lost")

type OutboxDeliveryStatus uint8

const (
	OutboxDeliveryUnknown OutboxDeliveryStatus = iota
	OutboxDeliveryDelivered
	OutboxDeliveryNoKnownOnlineTargets
	OutboxDeliveryIndeterminate
)

// OutboxDeliveryRef is the durable queue identity attached to an online push so
// service-level Edge confirmation and optional late client ACKs can be fenced.
type OutboxDeliveryRef struct {
	OutboxID     int64
	TargetUserID int64
	Pts          int
	Attempt      int
}

func (r OutboxDeliveryRef) Empty() bool {
	return r.OutboxID == 0 && r.TargetUserID == 0 && r.Pts == 0 && r.Attempt == 0
}

type OutboxPushRequest struct {
	TargetUserID     int64
	DeliveryRef      OutboxDeliveryRef
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	MessageType      proto.MessageType
	UpdateBytes      []byte
	DeliveryTimeout  time.Duration
}

type OutboxPushResult struct {
	Sent   int
	Status OutboxDeliveryStatus
}

type OutboxPushCommand struct {
	CommandID        string
	SourceInstanceID string
	TargetInstanceID string
	TargetUserID     int64
	DeliveryRef      OutboxDeliveryRef
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	MessageType      proto.MessageType
	UpdateBytes      []byte
	DeliveryTimeout  time.Duration
}

type OutboxPushAck struct {
	CommandID        string
	SourceInstanceID string
	TargetInstanceID string
	DeliveryRef      OutboxDeliveryRef
	Sent             int
	Status           OutboxDeliveryStatus
	Error            string
}

type SessionControlKind string

// MaxChannelMembershipSyncPage bounds one Core-to-Edge staging command. The
// complete account membership remains unbounded and is streamed over pages.
const MaxChannelMembershipSyncPage = 1000

// ChannelMembershipSyncDisposition is the Edge-owned singleflight decision
// for one session membership bootstrap. Only the acquired caller may append
// pages and commit. In-progress and prepared callers must not scan PostgreSQL.
type ChannelMembershipSyncDisposition string

const (
	ChannelMembershipSyncAcquired   ChannelMembershipSyncDisposition = "acquired"
	ChannelMembershipSyncInProgress ChannelMembershipSyncDisposition = "in_progress"
	ChannelMembershipSyncPrepared   ChannelMembershipSyncDisposition = "prepared"
)

const (
	SessionControlCloseBusinessAuthKey                 SessionControlKind = "close_business_auth_key"
	SessionControlCloseRawAuthKey                      SessionControlKind = "close_raw_auth_key"
	SessionControlBindAuthKeySession                   SessionControlKind = "bind_auth_key_session"
	SessionControlBindRawAuthKey                       SessionControlKind = "bind_raw_auth_key"
	SessionControlBindUser                             SessionControlKind = "bind_user"
	SessionControlUnbindAuthKey                        SessionControlKind = "unbind_auth_key"
	SessionControlSetReceivesUpdates                   SessionControlKind = "set_receives_updates"
	SessionControlSetClientLayer                       SessionControlKind = "set_client_layer"
	SessionControlSeedRawLayer                         SessionControlKind = "seed_raw_layer"
	SessionControlSeedBusinessLayer                    SessionControlKind = "seed_business_layer"
	SessionControlRefreshRawLayer                      SessionControlKind = "refresh_raw_layer"
	SessionControlClearRawLayer                        SessionControlKind = "clear_raw_layer"
	SessionControlPushSession                          SessionControlKind = "push_session"
	SessionControlPushSessionImmediate                 SessionControlKind = "push_session_immediate"
	SessionControlPushUser                             SessionControlKind = "push_user"
	SessionControlPushUserBatch                        SessionControlKind = "push_user_batch"
	SessionControlPushUserBounded                      SessionControlKind = "push_user_bounded"
	SessionControlPushUserTransient                    SessionControlKind = "push_user_transient"
	SessionControlPushUserAuthKey                      SessionControlKind = "push_user_auth_key"
	SessionControlPushUserAuthKeyTransient             SessionControlKind = "push_user_auth_key_transient"
	SessionControlPushUserExceptBusinessAuthKey        SessionControlKind = "push_user_except_business_auth_key"
	SessionControlPushUserTransientAtLeastLayer        SessionControlKind = "push_user_transient_at_least_layer"
	SessionControlPushUserAuthKeyTransientAtLeastLayer SessionControlKind = "push_user_auth_key_transient_at_least_layer"
	SessionControlTrackChannelInterest                 SessionControlKind = "track_channel_interest"
	SessionControlClearChannelInterest                 SessionControlKind = "clear_channel_interest"
	SessionControlRefreshChannelSubscription           SessionControlKind = "refresh_channel_subscription"
	SessionControlBeginChannelMembershipSync           SessionControlKind = "begin_channel_membership_sync"
	SessionControlAppendChannelMembershipSync          SessionControlKind = "append_channel_membership_sync"
	SessionControlCommitChannelMembershipSync          SessionControlKind = "commit_channel_membership_sync"
	SessionControlAbortChannelMembershipSync           SessionControlKind = "abort_channel_membership_sync"
	SessionControlAddUserChannelMembership             SessionControlKind = "add_user_channel_membership"
	SessionControlRemoveUserChannelMembership          SessionControlKind = "remove_user_channel_membership"
)

type SessionControlUserPush struct {
	TargetUserID    int64
	RawAuthKeyID    [8]byte
	ExceptSessionID int64
	MessageType     proto.MessageType
	UpdateBytes     []byte
	ChannelDelivery ChannelDeliveryWatermark
}

type ChannelDeliveryKind string

const (
	ChannelDeliveryPayload ChannelDeliveryKind = "payload"
	ChannelDeliveryNudge   ChannelDeliveryKind = "nudge"
)

type ChannelDeliveryWatermark struct {
	Kind      ChannelDeliveryKind
	ChannelID int64
	MinPts    int
	MaxPts    int
}

func (w ChannelDeliveryWatermark) Present() bool {
	return w.Kind != "" || w.ChannelID != 0 || w.MinPts != 0 || w.MaxPts != 0
}

func (w ChannelDeliveryWatermark) Valid() bool {
	if w.ChannelID <= 0 || w.MinPts <= 0 || w.MaxPts < w.MinPts {
		return false
	}
	return w.Kind == ChannelDeliveryPayload || w.Kind == ChannelDeliveryNudge
}

type SessionControlCommand struct {
	CommandID         string
	SourceInstanceID  string
	TargetInstanceID  string
	Kind              SessionControlKind
	AuthKeyID         [8]byte
	RawAuthKeyID      [8]byte
	BusinessAuthKeyID [8]byte
	ExceptSessionID   int64
	SessionID         int64
	UserID            int64
	TargetUserID      int64
	ReceivesUpdates   bool
	Layer             int
	Semantic          tlprofile.SemanticID
	MessageType       proto.MessageType
	UpdateBytes       []byte
	UserPushes        []SessionControlUserPush
	DeliveryTimeout   time.Duration
	ChannelID         int64
	ChannelIDs        []int64
	SubscriptionTTL   time.Duration
	MembershipSyncID  int64
}

type SessionControlAck struct {
	CommandID                 string
	SourceInstanceID          string
	TargetInstanceID          string
	Affected                  int
	MembershipSyncID          int64
	MembershipSyncDisposition ChannelMembershipSyncDisposition
	Error                     string
}

// OutboxDeliverer gives Core a delivery confirmation boundary for durable
// outbox rows. Implementations must return Indeterminate rather than pretending
// success when a target may be online on another Edge but no ACK was observed.
type OutboxDeliverer interface {
	PushOutboxUpdate(ctx context.Context, req OutboxPushRequest) (OutboxPushResult, error)
}

type OutboxPushCommandHandler func(context.Context, OutboxPushCommand) OutboxPushAck

type OutboxPushCommandBus interface {
	SendOutboxPush(ctx context.Context, targetInstanceID string, cmd OutboxPushCommand) (OutboxPushAck, error)
	SubscribeOutboxPushes(ctx context.Context, instanceID string, handle OutboxPushCommandHandler) error
}

type SessionControlCommandHandler func(context.Context, SessionControlCommand) SessionControlAck

type SessionControlCommandBus interface {
	SendSessionControl(ctx context.Context, targetInstanceID string, cmd SessionControlCommand) (SessionControlAck, error)
	SubscribeSessionControls(ctx context.Context, instanceID string, handle SessionControlCommandHandler) error
}

type LocationRecord struct {
	InstanceID           string
	UserID               int64
	RawAuthKeyID         [8]byte
	BusinessAuthKeyID    [8]byte
	SessionID            int64
	ReceivesUpdates      bool
	Layer                int
	ActiveChannelIDs     []int64
	ChannelSubscriptions []ChannelSubscriptionLocation
	UpdatedAtUnix        int64
	// LocationRevision is allocated atomically by the shared registry and is
	// the authoritative reconnect ownership order across Edge instances.
	LocationRevision int64
	// UpdatedAtUnixNano is diagnostic time and a fallback for records written
	// before LocationRevision existed; it is not the cross-host clock authority.
	UpdatedAtUnixNano int64
}

// ChannelMembershipRecord is the instance-local online membership projection
// for one user. It is deliberately independent of session LocationRecord so a
// layer/ready/session mutation never rewrites the user's complete channel set,
// and multiple sessions on the same Edge share one Redis membership ref.
type ChannelMembershipRecord struct {
	InstanceID    string
	UserID        int64
	ChannelIDs    []int64
	UpdatedAtUnix int64
}

type ChannelSubscriptionLocation struct {
	ChannelID         int64
	ExpiresAtUnixNano int64
}

type LocationMutation struct {
	Record  LocationRecord
	Deleted bool
}

type ChannelMembershipMutation struct {
	Record  ChannelMembershipRecord
	Deleted bool
}

type LocationRegistry interface {
	ListUser(ctx context.Context, userID int64) ([]LocationRecord, error)
	ListBusinessAuthKey(ctx context.Context, authKeyID [8]byte) ([]LocationRecord, error)
	ListInstance(ctx context.Context, instanceID string) ([]LocationRecord, error)
}

// MutableLocationRegistry is the Edge-owned write boundary. Instance liveness
// is one fenced lease, independent of the number of active sessions. Session
// records and their secondary indexes are updated only when that session
// changes; callers must never renew liveness by rewriting a full snapshot.
type MutableLocationRegistry interface {
	LocationRegistry
	AcquireInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error
	RenewInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error
	ApplyLocationMutations(ctx context.Context, instanceID, leaseID string, mutations []LocationMutation) error
	ApplyChannelMembershipMutations(ctx context.Context, instanceID, leaseID string, mutations []ChannelMembershipMutation) error
	ReleaseInstanceLease(ctx context.Context, instanceID, leaseID string) error
}

type BatchUserLocationRegistry interface {
	ListUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error)
}

type RawAuthKeyLocationRegistry interface {
	ListRawAuthKey(ctx context.Context, authKeyID [8]byte) ([]LocationRecord, error)
}

type ActiveRawAuthKeyRegistry interface {
	ListActiveRawAuthKeyIDs(ctx context.Context) ([][8]byte, error)
}

type ChannelLocationRegistry interface {
	ListChannelInterest(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListChannelMember(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListChannelSubscription(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListOnlineChannelIDsSnapshot(ctx context.Context) ([]int64, error)
}

// Controller is the Core-facing control-plane boundary for active MTProto
// sessions owned by Edge.
//
// MTProto session identity is raw auth_key_id + session_id. Every method that
// targets one logical session must carry both values; session_id alone is not
// globally unique across auth keys.
type Controller interface {
	BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte)
	AuthKeyIDForSession(rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool)
	// BindUserForAuthKey carries the session that observed the authorization,
	// but the resulting user identity is raw-auth-key scoped. A Telegram client
	// can open upload/download sessions under the same raw key before they send
	// a business RPC through CoreExec.
	BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64)
	UserIDResolvedForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (userID int64, resolved bool)
	UnbindAuthKey(authKeyID [8]byte) int
	SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool)
	PushToSessionForAuthKey(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error
	// excludeAuthKeyID/excludeSessionID must both be zero or both be non-zero.
	PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error)
}

type FullController interface {
	Controller
	RawAuthKeyIdentityBinder
	RawAuthKeyMetadataProvider
	ImmediateSessionPusher
	SessionUpdatesStateProvider
	ClientLayerBinder
	AuthKeyLayerBinder
	BusinessAuthKeyLayerBinder
	AuthKeyLayerRefresher
	AuthKeyInheritedLayerClearer
	ActiveSessionLayerEvidenceProvider
	SessionTerminator
	RawSessionTerminator
	BoundedSessionPusher
	TransientSessionPusher
	AuthKeyTargetedSessionPusher
	LayerAwareTransientPusher
	OnlineUserProvider
	ChannelSubscriptionProvider
	ChannelNudgeProvider
	ChannelFanoutRecoverySessionProvider
}

// NewLocal names the Edge-local control adapter used by a dedicated Edge
// process. It intentionally returns the same dynamic implementation so optional
// capabilities remain visible through type assertions at the edge boundary.
func NewLocal(controller Controller) Controller {
	return controller
}

type outboxController struct {
	Controller
	deliverer OutboxDeliverer
}

func NewOutboxController(controller Controller, deliverer OutboxDeliverer) Controller {
	if deliverer == nil {
		return controller
	}
	return &outboxController{Controller: controller, deliverer: deliverer}
}

func (c *outboxController) PushOutboxUpdate(ctx context.Context, req OutboxPushRequest) (OutboxPushResult, error) {
	return c.deliverer.PushOutboxUpdate(ctx, req)
}

// RawAuthKeyIdentityBinder switches all live sessions for a raw temporary key to
// the canonical permanent identity after auth.bindTempAuthKey succeeds.
type RawAuthKeyIdentityBinder interface {
	BindAuthKeyForRawAuthKey(rawAuthKeyID [8]byte, authKeyID [8]byte) int
}

// RawAuthKeyMetadataProvider exposes raw-key protocol expiry observed by Edge.
type RawAuthKeyMetadataProvider interface {
	AuthKeyExpiresAtForSession(rawAuthKeyID [8]byte, sessionID int64) (expiresAt int, found bool)
}

// ImmediateSessionPusher sends login-unblocking updates before the session has
// established its durable updates baseline.
type ImmediateSessionPusher interface {
	PushToSessionForAuthKeyImmediate(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error
}

// SessionUpdatesStateProvider exposes whether a live session is ready to receive updates.
type SessionUpdatesStateProvider interface {
	ReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64) bool
}

// ClientLayerBinder records explicit per-session layer evidence observed at Edge.
type ClientLayerBinder interface {
	SetClientLayerForAuthKey(rawAuthKeyID [8]byte, sessionID int64, layer int)
}

// AuthKeyLayerBinder seeds a mutable inherited layer for live sessions that
// have not yet produced explicit invokeWithLayer evidence.
type AuthKeyLayerBinder interface {
	SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int
}

// BusinessAuthKeyLayerBinder seeds unknown live sessions normalized to a
// permanent/business auth key.
type BusinessAuthKeyLayerBinder interface {
	SeedInheritedLayerForBusinessAuthKey(authKeyID [8]byte, layer int) int
}

// AuthKeyLayerRefresher refreshes inherited layer defaults after identity normalization.
type AuthKeyLayerRefresher interface {
	RefreshInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int
}

// AuthKeyInheritedLayerClearer removes mutable inherited defaults while
// preserving explicit per-session wire evidence.
type AuthKeyInheritedLayerClearer interface {
	ClearInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte) int
}

// ActiveSessionLayerEvidenceProvider exposes explicit live-session layer evidence.
type ActiveSessionLayerEvidenceProvider interface {
	ExplicitLayerEvidenceForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (layer int, msgID int64, ok bool)
}

// SessionTerminator closes sessions associated with a permanent business auth key.
type SessionTerminator interface {
	CloseSessionsForBusinessAuthKey(authKeyID [8]byte) int
}

// BoundedSessionTerminator closes sessions through the distributed Edge
// control fabric and reports an indeterminate remote delivery instead of
// silently treating it as an offline auth key.
type BoundedSessionTerminator interface {
	CloseSessionsForBusinessAuthKeyBounded(ctx context.Context, authKeyID [8]byte) (int, error)
}

// RawSessionTerminator closes sessions associated with a physical/raw auth key.
type RawSessionTerminator interface {
	CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int
}

// BoundedRawSessionTerminator is the context-aware form used when raw temp-key
// aliases must be retired as part of a confirmed authorization revocation.
type BoundedRawSessionTerminator interface {
	CloseSessionsForRawAuthKeyExceptBounded(ctx context.Context, authKeyID [8]byte, exceptSessionID int64) (int, error)
}

// BoundedSessionPusher provides online push with a caller supplied delivery
// deadline. Durable callers must rely on queue ACK/fencing for correctness, not
// on this deadline alone.
type BoundedSessionPusher interface {
	PushToUserExceptAuthKeySessionBounded(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

type ChannelDeliverySessionPusher interface {
	PushChannelUpdateToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, delivery ChannelDeliveryWatermark) (int, error)
}

// TransientSessionPusher sends short-lived updates that must not be queued for
// later durable delivery.
type TransientSessionPusher interface {
	PushToUserTransientExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// AuthKeyTargetedSessionPusher targets a specific business auth key for
// device-level updates such as secret chats.
type AuthKeyTargetedSessionPusher interface {
	PushToUserAuthKey(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass) (int, error)
	PushToUserAuthKeyTransient(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserExceptBusinessAuthKey(ctx context.Context, userID int64, excludeBusinessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// LayerAwareTransientPusher filters transient updates by exact live layer/profile before encoding.
type LayerAwareTransientPusher interface {
	PushToUserTransientAtLeastLayer(ctx context.Context, userID int64, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserAuthKeyTransientAtLeastLayer(ctx context.Context, userID int64, businessAuthKeyID [8]byte, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// SemanticTransientPusher filters transient updates through generated exact-profile
// metadata. It avoids hard-coding the first layer that introduced a constructor.
type SemanticTransientPusher interface {
	PushToUserTransientCompatible(ctx context.Context, userID int64, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserAuthKeyTransientCompatible(ctx context.Context, userID int64, businessAuthKeyID [8]byte, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// OnlineUserProvider exposes a bounded runtime snapshot for transient online fanout.
type OnlineUserProvider interface {
	IsUserOnline(userID int64) bool
	OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64
	TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64)
	ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64)
	OnlineChannelUserIDs(channelID int64, limit int) []int64
	BeginSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID int64) (syncID int64, disposition ChannelMembershipSyncDisposition, err error)
	AppendSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64, channelIDs []int64) error
	CommitSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) (synced bool, err error)
	AbortSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64)
	AddUserChannelMembership(userID, channelID int64)
	RemoveUserChannelMembership(userID, channelID int64)
	OnlineChannelMemberUserIDs(channelID int64, limit int) []int64
}

// ChannelSubscriptionProvider tracks short-lived public-channel subscriptions.
type ChannelSubscriptionProvider interface {
	RefreshChannelSubscription(rawAuthKeyID [8]byte, sessionID, userID, channelID int64, ttl time.Duration)
	OnlineChannelSubscriberUserIDs(channelID int64, limit int) []int64
	OnlineChannelSubscriberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64
}

// ChannelNudgeProvider returns online channel members excluding already delivered users.
type ChannelNudgeProvider interface {
	OnlineChannelMemberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64
}

// ChannelFanoutRecoverySessionProvider snapshots channel IDs with online members.
type ChannelFanoutRecoverySessionProvider interface {
	OnlineChannelIDsSnapshot() []int64
}

type UserLocationBatchProvider interface {
	UserLocationRecordsForUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error)
}

type LocationTargetedUserPush struct {
	TargetUserID     int64
	Locations        []LocationRecord
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	MessageType      proto.MessageType
	Update           tg.UpdatesClass
	ChannelDelivery  ChannelDeliveryWatermark
}

type BatchLocationTargetedSessionPusher interface {
	PushToUserLocationBatches(ctx context.Context, pushes []LocationTargetedUserPush) (int, error)
}
