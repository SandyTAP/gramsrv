package stargiveaways

import (
	"context"
	"errors"
	"fmt"
	"time"

	"telesrv/internal/domain"
)

// DrawStore is the persistence the draw worker needs.
//
// Deliberately narrow. The worker's job is to find giveaways whose deadline
// passed, choose winners, and record that it did. Anything that would let it
// pay twice or draw twice belongs behind CommitStarsGiveawayDraw's
// WHERE state = 'active' guard, not behind more interface methods.
type DrawStore interface {
	DueStarsGiveaways(ctx context.Context, now, limit int) ([]domain.StarsGiveawayRun, error)
	ListStarsGiveawayEntries(ctx context.Context, giveawayID int64, limit int) ([]domain.StarsGiveawayEntry, error)
	CommitStarsGiveawayDraw(ctx context.Context, draw domain.StarsGiveawayDraw, winners []int64) (bool, error)
	StarsGiveawayPendingPayouts(ctx context.Context, giveawayID int64, limit int) ([]int64, error)
	MarkStarsGiveawayEntryPaid(ctx context.Context, giveawayID, userID int64, now int, failure string) error
	StarsGiveawayByID(ctx context.Context, giveawayID int64) (domain.StarsGiveawayRun, error)
}

// CreditStore grants stars to a winner.
type CreditStore interface {
	Credit(ctx context.Context, userID, amount int64, reason domain.StarsTransactionReason, peer domain.Peer, title, desc string) (domain.StarsBalance, error)
}

// Options configures the worker.
type Options struct {
	// Interval between sweeps. A giveaway that ends is not urgent, but the
	// announcement should not trail the deadline by an hour either.
	Interval time.Duration
	// BatchSize bounds one sweep so a backlog does not hold the loop.
	BatchSize int
	// Now is injectable for tests.
	Now func() time.Time
	// Log receives sweep-level failures. Optional.
	Log func(error)
}

// Service draws ended giveaways and pays their winners.
type Service struct {
	store  DrawStore
	credit CreditStore
	opts   Options
}

// New builds the worker.
func New(store DrawStore, credit CreditStore, opts Options) *Service {
	if opts.Interval <= 0 {
		opts.Interval = time.Minute
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Service{store: store, credit: credit, opts: opts}
}

// Run sweeps until ctx is cancelled.
//
// Payout for a given giveaway is retried on every sweep, so a winner whose
// credit failed once is still paid later without operator involvement.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil && s.opts.Log != nil {
				s.opts.Log(err)
			}
		}
	}
}

// Sweep runs one pass: draw everything due, then finish paying anything already
// drawn.
//
// Ordering matters. A giveaway drawn by a previous run but not yet paid is
// picked up by the payout pass; if the draw pass ran first it would find
// nothing to do and the payout pass would settle it, so the same sweep
// converges instead of costing two minutes per crash.
func (s *Service) Sweep(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	now := s.opts.Now().UTC()

	due, err := s.store.DueStarsGiveaways(ctx, int(now.Unix()), s.opts.BatchSize)
	if err != nil {
		return fmt.Errorf("list due giveaways: %w", err)
	}
	for _, run := range due {
		if run.State != domain.StarsGiveawayActive {
			// Already drawn: the only work left is paying, which the second
			// pass below handles.
			continue
		}
		if err := s.drawOne(ctx, run, now); err != nil && s.opts.Log != nil {
			s.opts.Log(fmt.Errorf("draw giveaway %d: %w", run.ID, err))
		}
	}

	// Payout retries, for both giveaways drawn moments ago and ones whose
	// credit failed on an earlier sweep.
	for _, run := range due {
		if err := s.payPending(ctx, run.ID, now); err != nil && s.opts.Log != nil {
			s.opts.Log(fmt.Errorf("pay giveaway %d: %w", run.ID, err))
		}
	}
	return nil
}

// drawOne chooses and commits the winner set for one ended giveaway.
//
// A giveaway nobody entered is left active. The client already renders an ended
// giveaway with no participants, and completing it would mean recording a zero
// winner list, which the draw CHECK forbids. It stays a cheap indexed miss on
// each sweep until an operator cancels it.
func (s *Service) drawOne(ctx context.Context, run domain.StarsGiveawayRun, now time.Time) error {
	entries, err := s.store.ListStarsGiveawayEntries(ctx, run.ID, 0)
	if err != nil {
		return fmt.Errorf("list entries: %w", err)
	}
	if len(entries) == 0 {
		// Nothing to draw. Not an error: an operator launched a giveaway and
		// nobody joined.
		return nil
	}

	draw, err := domain.DrawStarsGiveawayWinners(run, entries, now)
	if err != nil {
		if errors.Is(err, domain.ErrGiveawayNoEntries) || errors.Is(err, domain.ErrGiveawayWinnersInvalid) {
			return nil
		}
		return err
	}

	committed, err := s.store.CommitStarsGiveawayDraw(ctx, draw, draw.Winners)
	if err != nil {
		return fmt.Errorf("commit draw: %w", err)
	}
	if !committed {
		// Lost the race against a concurrent sweep or a retry that already
		// committed. Whoever won the race owns the payout, which the payment
		// pass below performs.
		return nil
	}
	// Payment is deliberately NOT done here. The sweep has a single payment
	// pass, and paying from the draw as well would credit the winners twice.
	return nil
}

// payPending credits every winner recorded as 'won' but not yet 'paid'.
//
// Each credit is marked individually so one failing winner does not block the
// rest, and the failure is stored on the row rather than swallowed.
func (s *Service) payPending(ctx context.Context, giveawayID int64, now time.Time) error {
	pending, err := s.store.StarsGiveawayPendingPayouts(ctx, giveawayID, 0)
	if err != nil {
		return fmt.Errorf("list pending payouts: %w", err)
	}
	if len(pending) == 0 || s.credit == nil {
		return nil
	}

	run, err := s.giveawayFor(ctx, giveawayID)
	if err != nil {
		return err
	}

	var firstErr error
	for _, userID := range pending {
		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: run.ChannelID}
		_, creditErr := s.credit.Credit(ctx, userID, run.PerUserStars, domain.StarsReasonGiveaway, peer,
			"Giveaway prize", fmt.Sprintf("You won a giveaway with %d stars", run.PerUserStars))
		failure := ""
		if creditErr != nil {
			failure = creditErr.Error()
			if firstErr == nil {
				firstErr = creditErr
			}
		}
		if err := s.store.MarkStarsGiveawayEntryPaid(ctx, giveawayID, userID, int(now.Unix()), failure); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// giveawayFor reloads the giveaway to read the payout amount and channel.
//
// Re-reading rather than passing the value through from the draw is
// deliberate: the per-winner amount is money, and it must come from the
// committed row so a retried sweep cannot pay an amount from a stale in-memory
// giveaway.
func (s *Service) giveawayFor(ctx context.Context, giveawayID int64) (domain.StarsGiveawayRun, error) {
	run, err := s.store.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		return domain.StarsGiveawayRun{}, fmt.Errorf("%w: giveaway %d for payout", domain.ErrGiveawayNotFound, giveawayID)
	}
	if run.PerUserStars <= 0 {
		return domain.StarsGiveawayRun{}, fmt.Errorf("%w: giveaway %d has no payout amount", domain.ErrGiveawayWinnersInvalid, giveawayID)
	}
	return run, nil
}
