package store

import (
	"context"

	"telesrv/internal/domain"
)

// BotStore 持久化 bot 账号元数据（bots 表）与内置 bot 对话状态（bot_chat_states 表）。
//
// CreateBotAccount 必须原子地创建 users 行（is_bot=true, bot_info_version=1, phone 空）
// 与 bots 行；username 冲突返回 domain.ErrUsernameOccupied。
type BotStore interface {
	CreateBotAccount(ctx context.Context, user domain.User, profile domain.BotProfile) (domain.User, domain.BotProfile, error)
	GetBot(ctx context.Context, botUserID int64) (domain.BotProfile, bool, error)
	ListBotsByOwner(ctx context.Context, ownerUserID int64) ([]domain.BotProfile, error)
	CountBotsByOwner(ctx context.Context, ownerUserID int64) (int, error)
	// UpdateBotTokenSecret 写入新 token 随机段；旧 token 立即失效（无缓存层）。
	UpdateBotTokenSecret(ctx context.Context, botUserID int64, secret string) error

	// 以下 P2 元数据写入：每个都在单事务内同时 bump 该 bot 对应 users 行的
	// bot_info_version（客户端据此感知变更并重拉 getFullUser），返回 bump 后的新
	// version。UpdateBotInfo 跨 users（first_name/about）与 bots（description）两表，
	// 因此放在同一原子方法里。
	UpdateBotCommands(ctx context.Context, botUserID int64, commands []domain.BotCommand) (newVersion int, err error)
	UpdateBotInfo(ctx context.Context, botUserID int64, upd domain.BotInfoUpdate) (newVersion int, err error)
	UpdateBotMenuButton(ctx context.Context, botUserID int64, button domain.BotMenuButton) (newVersion int, err error)
	SetBotInlinePlaceholder(ctx context.Context, botUserID int64, placeholder string) (newVersion int, err error)
	SetBotInlineGeo(ctx context.Context, botUserID int64, inlineGeo bool) (newVersion int, err error)
	SetBotNochats(ctx context.Context, botUserID int64, nochats bool) (newVersion int, err error)
	SetBotChatHistory(ctx context.Context, botUserID int64, chatHistory bool) (newVersion int, err error)
	CanBotSendMessage(ctx context.Context, botUserID, userID int64) (bool, error)
	AllowBotSendMessage(ctx context.Context, botUserID, userID int64, fromRequest bool) (created bool, err error)

	UpsertBotApp(ctx context.Context, app domain.BotApp) (domain.BotApp, int, error)
	GetBotAppByID(ctx context.Context, appID, accessHash int64) (domain.BotApp, bool, error)
	GetBotAppByShortName(ctx context.Context, botUserID int64, shortName string) (domain.BotApp, bool, error)
	GetMainBotApp(ctx context.Context, botUserID int64) (domain.BotApp, bool, error)
	ListBotApps(ctx context.Context, botUserID int64) ([]domain.BotApp, error)
	GetBotAppSettings(ctx context.Context, botUserID int64) (domain.BotAppSettings, bool, error)
	UpsertBotAppSettings(ctx context.Context, botUserID int64, settings domain.BotAppSettings) (int, error)
	ListBotAppPreviewMedia(ctx context.Context, botUserID, appID int64) ([]domain.BotAppPreviewMedia, error)
	UpsertBotAppPreviewMedia(ctx context.Context, media domain.BotAppPreviewMedia) (domain.BotAppPreviewMedia, int, error)
	DeleteBotAppPreviewMedia(ctx context.Context, botUserID, appID, mediaID int64) (int, error)
	ReorderBotAppPreviewMedia(ctx context.Context, botUserID, appID int64, mediaIDs []int64) (int, error)

	UpsertAttachMenuBot(ctx context.Context, bot domain.BotAttachMenuBot) (int, error)

	// Bot invoices. CreateBotInvoice records the sale right after the invoice
	// message is stored, so the payment form can resolve the price later.
	CreateBotInvoice(ctx context.Context, invoice domain.BotInvoice) (domain.BotInvoice, error)
	// BotInvoiceByMessage resolves the invoice behind an inputInvoiceMessage.
	BotInvoiceByMessage(ctx context.Context, botUserID, chatID int64, messageID int) (domain.BotInvoice, bool, error)
	// SettleBotInvoice marks a paid invoice and binds the charge id the client
	// reported. A replayed call returns the stored invoice with settled=false
	// instead of charging the payer twice.
	SettleBotInvoice(ctx context.Context, botUserID, chatID int64, messageID int, payerUserID int64, chargeID string, date int) (domain.BotInvoice, bool, error)
	// RefundBotInvoiceByCharge resolves an invoice by telegram_payment_charge_id
	// and flags it refunded; an already refunded invoice is a replay-safe no-op.
	RefundBotInvoiceByCharge(ctx context.Context, chargeID string) (domain.BotInvoice, bool, error)
	GetAttachMenuBot(ctx context.Context, botUserID int64) (domain.BotAttachMenuBot, bool, error)
	ListAttachMenuBots(ctx context.Context) ([]domain.BotAttachMenuBot, error)
	GetAttachMenuState(ctx context.Context, userID, botUserID int64) (domain.BotAttachMenuState, bool, error)
	SetAttachMenuState(ctx context.Context, state domain.BotAttachMenuState) (domain.BotAttachMenuState, error)

	SaveRequestedWebViewButton(ctx context.Context, button domain.BotRequestedWebViewButton) error
	GetRequestedWebViewButton(ctx context.Context, botUserID, userID int64, webAppReqID string) (domain.BotRequestedWebViewButton, bool, error)
	DeleteRequestedWebViewButton(ctx context.Context, botUserID, userID int64, webAppReqID string) error

	SetBotEmojiStatusPermission(ctx context.Context, botUserID, userID int64, allowed bool) error
	BotEmojiStatusPermission(ctx context.Context, botUserID, userID int64) (bool, error)

	PutWebViewCustomMethodQuery(ctx context.Context, query domain.BotWebViewCustomMethodQuery) error

	GetBotChatState(ctx context.Context, botUserID, userID int64) (domain.BotChatState, bool, error)
	UpsertBotChatState(ctx context.Context, state domain.BotChatState) error
	DeleteBotChatState(ctx context.Context, botUserID, userID int64) error
}
