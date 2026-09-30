package rpc

import (
	"context"
	"errors"
	"time"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// preCheckoutTimeout 是客户端挂起等待应答的窗口。与 preCheckoutTimeoutSeconds 是同一
// 个 10 秒要求，写成 time.Duration 只为直接交给 time.NewTimer。
//
// 超时即视为 bot 没有批准这笔支付，付款失败，绝不降级为直接扣款——那正是 pre-checkout
// 要防的事。
const preCheckoutTimeout = preCheckoutTimeoutSeconds * time.Second

// preCheckoutRegistry 持有一次挂起的 pre-checkout 询问及其应答通道。
//
// 与 callbackRegistry 的差别在于时效：callback 允许 25s 且超时只影响按钮动画，
// 而 pre-checkout 是付款前的闸门，窗口固定 10s，超时必须让付款失败。因此这里不
// 共享 store —— 询问的生命周期只有一次 RPC 调用，跨节点共享它反而会引入一个
// "等待者已超时、答案迟到"的写入路径。注册表只活在进程内，随 RPC 一起生死。
type preCheckoutRegistry struct {
	mu      chan struct{}
	pending map[int64]*pendingPreCheckout
}

type pendingPreCheckout struct {
	answer    chan domain.BotPreCheckoutAnswer
	done      chan struct{}
	botUserID int64
	userID    int64
}

func newPreCheckoutRegistry() *preCheckoutRegistry {
	return &preCheckoutRegistry{mu: make(chan struct{}, 1), pending: make(map[int64]*pendingPreCheckout)}
}

// register 登记一次挂起的询问，返回 query_id 与接收通道。调用方必须 defer
// deregister，无论是否等到应答，否则表和 goroutine 都会泄漏。
func (p *preCheckoutRegistry) register(botUserID, userID int64) (int64, *pendingPreCheckout) {
	entry := &pendingPreCheckout{
		answer:    make(chan domain.BotPreCheckoutAnswer, 1),
		done:      make(chan struct{}),
		botUserID: botUserID,
		userID:    userID,
	}
	p.mu <- struct{}{}
	queryID := randomNonZeroInt64()
	for _, exists := p.pending[queryID]; exists; _, exists = p.pending[queryID] {
		queryID = randomNonZeroInt64()
	}
	p.pending[queryID] = entry
	<-p.mu
	return queryID, entry
}

func (p *preCheckoutRegistry) deregister(queryID int64) {
	p.mu <- struct{}{}
	entry, ok := p.pending[queryID]
	if ok {
		delete(p.pending, queryID)
		close(entry.done)
	}
	<-p.mu
}

// resolve 只接受属主 bot 的应答：其它 bot 拿到 query_id 也无法代答付款闸门。
func (p *preCheckoutRegistry) resolve(callerBotID, queryID int64, ans domain.BotPreCheckoutAnswer) bool {
	p.mu <- struct{}{}
	entry, ok := p.pending[queryID]
	if ok && entry.botUserID == callerBotID {
		delete(p.pending, queryID)
	} else {
		ok = false
	}
	<-p.mu
	if !ok {
		return false
	}
	// answer 带 1 容量缓冲且非阻塞投递：等待方若已超时离开，缓冲随 entry 回收，
	// resolve 不会卡在发送上。
	select {
	case entry.answer <- ans:
	default:
	}
	close(entry.done)
	return true
}

// pendingSnapshot 返回当前挂起询问的 query_id，供测试断言"有没有被问过"。
func (p *preCheckoutRegistry) pendingSnapshot() []int64 {
	p.mu <- struct{}{}
	defer func() { <-p.mu }()
	out := make([]int64, 0, len(p.pending))
	for queryID := range p.pending {
		out = append(out, queryID)
	}
	return out
}

func (p *preCheckoutRegistry) size() int {
	p.mu <- struct{}{}
	defer func() { <-p.mu }()
	return len(p.pending)
}

// await 阻塞到 bot 应答、10s 超时，或调用方 ctx 取消。三者中先到者胜出；超时与
// 取消都会先 deregister，保证不会有迟到的应答投递到已离开的等待者。
//
// 返回的 answered 为 false 时，调用方必须让付款失败——把"没有批准"降级成"直接
// 扣款"正是 pre-checkout 要防的事。
func (p *preCheckoutRegistry) await(ctx context.Context, queryID int64, entry *pendingPreCheckout) (domain.BotPreCheckoutAnswer, bool) {
	timer := time.NewTimer(preCheckoutTimeout)
	defer timer.Stop()
	defer p.deregister(queryID)

	select {
	case ans := <-entry.answer:
		return ans, true
	case <-timer.C:
		return domain.BotPreCheckoutAnswer{}, false
	case <-entry.done:
		// resolve 已经投递过答案，答案就在缓冲里；这里只是被 close 唤醒。
		select {
		case ans := <-entry.answer:
			return ans, true
		default:
			return domain.BotPreCheckoutAnswer{}, false
		}
	case <-ctx.Done():
		return domain.BotPreCheckoutAnswer{}, false
	}
}

// awaitBotPreCheckout 走完整链路：投递 update（Bot API 队列 + MTProto push），然后挂起
// 最多 10 秒等应答。Bot 不应答即视为拒绝——这正是 Telegram 的约定：10 秒内不答 = 这笔
// 支付不成立，所以超时绝不能降级成直接扣款。
func (r *Router) awaitBotPreCheckout(ctx context.Context, botUserID, payerUserID int64, currency string, totalAmount int64, payload string) error {
	query := domain.BotPreCheckoutQuery{
		BotUserID: botUserID, UserID: payerUserID,
		Currency: currency, TotalAmount: totalAmount, Payload: payload,
	}
	queryID, entry := r.preCheckouts.register(botUserID, payerUserID)
	query.ID = queryID

	// A question that was never queued can never be answered. Carrying on would
	// spend the full 10 second window waiting for a bot that was never asked, and
	// then report PAYMENT_FAILED as if the bot had declined - which hides the real
	// cause. So a queue failure fails the payment here, with the reason logged.
	if r.deps.BotAPIUpdates == nil {
		if r.log != nil {
			r.log.Error("bot invoice pre-checkout not queued",
				zap.Int64("bot_user_id", botUserID), zap.Error(errors.New("bot api update store not wired")))
		}
		r.preCheckouts.deregister(queryID)
		return internalErr()
	}
	queued, _, err := r.deps.BotAPIUpdates.EnqueueBotAPIUpdate(ctx, domain.EnqueueBotAPIUpdateRequest{
		BotUserID:   botUserID,
		Kind:        domain.BotAPIUpdatePreCheckoutQuery,
		Date:        int(r.clock.Now().Unix()),
		PreCheckout: &query,
	})
	if err != nil {
		if r.log != nil {
			r.log.Error("bot invoice pre-checkout not queued",
				zap.Int64("bot_user_id", botUserID),
				zap.Int64("query_id", queryID),
				zap.Error(err))
		}
		r.preCheckouts.deregister(queryID)
		return internalErr()
	}
	r.notifyBotAPIUpdate(botUserID)
	if r.log != nil {
		r.log.Info("bot invoice pre-checkout queued",
			zap.Int64("bot_user_id", botUserID),
			zap.Int64("query_id", queryID),
			zap.Int64("update_id", queued.ID),
			zap.Int64("payer_id", payerUserID),
			zap.Int64("total_amount", totalAmount))
	}

	update := &tg.UpdateBotPrecheckoutQuery{
		QueryID: queryID, UserID: payerUserID, Currency: currency, TotalAmount: totalAmount,
	}
	update.Payload = []byte(payload)
	r.pushUserMessage(ctx, botUserID, "push bot pre-checkout query", &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Date:    int(r.clock.Now().Unix()),
	})

	reply, answered := r.preCheckouts.await(ctx, queryID, entry)
	if !answered {
		// 超时即失败：没有批准就没有付款。
		return preCheckoutFailedErr()
	}
	if reply.OK {
		return nil
	}
	return preCheckoutDeclinedErr(reply.Error)
}

// serviceBotPreCheckout 让内置 service bot 同步回答 pre-checkout。它们没有 MTProto
// 会话也没有 Bot API 消费者，若走"投递 update + 挂起 10 秒"必然每次都超时付款。
//
// 它是可选能力（type-assert）而非 ServiceBotCallbacks 的成员：绝大多数内置 bot 都不卖
// 东西，强迫它们实现一个用不到的方法没有意义。
type serviceBotPreCheckout interface {
	ServiceBotCallbacks
	OnPreCheckoutQuery(ctx context.Context, query domain.BotPreCheckoutQuery) (domain.BotPreCheckoutAnswer, bool, error)
}

// botInvoicePreCheckoutResponder 让内置 service bot 同步回答 pre-checkout，
// 免得它们在没有 MTProto 会话的情况下必然超时。
type botInvoicePreCheckoutResponder interface {
	HandlesBot(botUserID int64) bool
	OnPreCheckoutQuery(ctx context.Context, query domain.BotPreCheckoutQuery) (domain.BotPreCheckoutAnswer, bool, error)
}

// runPreCheckout 在扣款前向 bot 询问并等待应答。Bot 不应答即视为拒绝，这正是
// Telegram 的约定：10 秒内不答 = 这笔支付不成立。
//

// runPreCheckout 在扣款前向 bot 询问并等待应答。
//
// 内置 service bot 没有 MTProto 会话也没有 Bot API 消费者，走"投递 update + 挂起"
// 必然超时，因此先尝试同步应答器（与 callback 的处理一致，见 bots_callback.go）。
// 外部 bot 一律走真实链路：投递 update，然后最多挂起 10 秒。
func (r *Router) runPreCheckout(ctx context.Context, botUserID, payerUserID int64, currency string, totalAmount int64, payload string) error {
	if responder, ok := r.deps.ServiceBotCallbacks.(serviceBotPreCheckout); ok &&
		r.deps.ServiceBotCallbacks.HandlesBot(botUserID) {
		reply, handled, err := responder.OnPreCheckoutQuery(ctx, domain.BotPreCheckoutQuery{
			BotUserID: botUserID, UserID: payerUserID,
			Currency: currency, TotalAmount: totalAmount, Payload: payload,
		})
		if handled {
			if err != nil {
				if r.log != nil {
					r.log.Warn("service bot pre-checkout",
						zap.Int64("bot_user_id", botUserID), zap.Error(err))
				}
				return preCheckoutFailedErr()
			}
			if reply.OK {
				return nil
			}
			return preCheckoutDeclinedErr(reply.Error)
		}
	}
	return r.awaitBotPreCheckout(ctx, botUserID, payerUserID, currency, totalAmount, payload)
}
