package memory

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// 频道/超级群消息 poll 投票与关闭：成员资格与消息可见性沿用 reaction 同款校验，
// poll 级语义委托共享 PollStore。

func (s *ChannelStore) VoteChannelMessagePoll(_ context.Context, req domain.VoteChannelMessagePollRequest) (domain.ChannelMessagePollResult, error) {
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	channel, msg, err := s.pollChannelMessageTarget(req.UserID, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelMessagePollResult{}, err
	}
	if err := s.polls.Vote(msg.Media.Poll.ID, req.UserID, req.Options, req.Date); err != nil {
		return domain.ChannelMessagePollResult{}, err
	}
	return s.channelPollResult(channel, msg, req.UserID, req.Date), nil
}

func (s *ChannelStore) CloseChannelMessagePoll(_ context.Context, req domain.CloseChannelMessagePollRequest) (domain.ChannelMessagePollResult, error) {
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	channel, msg, err := s.pollChannelMessageTarget(req.UserID, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelMessagePollResult{}, err
	}
	if err := s.polls.Close(msg.Media.Poll.ID, req.UserID); err != nil {
		return domain.ChannelMessagePollResult{}, err
	}
	return s.channelPollResult(channel, msg, req.UserID, req.Date), nil
}

func (s *ChannelStore) pollChannelMessageTarget(userID, channelID int64, messageID int) (domain.Channel, domain.ChannelMessage, error) {
	if s == nil || s.polls == nil {
		return domain.Channel{}, domain.ChannelMessage{}, domain.ErrMessageIDInvalid
	}
	if userID == 0 || channelID == 0 || messageID <= 0 || messageID > domain.MaxMessageBoxID {
		return domain.Channel{}, domain.ChannelMessage{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, domain.ChannelMessage{}, err
	}
	msg, ok := s.findMessageLocked(channelID, messageID)
	if !ok || msg.Deleted || msg.Action != nil || msg.ID <= member.AvailableMinID {
		return domain.Channel{}, domain.ChannelMessage{}, domain.ErrMessageIDInvalid
	}
	if msg.Media == nil || msg.Media.Kind != domain.MessageMediaKindPoll || msg.Media.Poll == nil || msg.Media.Poll.ID == 0 {
		return domain.Channel{}, domain.ChannelMessage{}, domain.ErrMessageIDInvalid
	}
	return cloneChannel(channel), cloneChannelMessage(msg), nil
}

// ChannelPollFanoutViews 批量加载一条 poll 消息对一组 viewer 的 per-viewer enrich（与 postgres 同口径，
// 是在线 poll fan-out 的唯一批量投影入口）：成员/AvailableMinID 可见性复刻 channelAndMemberLocked +
// pollChannelMessageTarget；poll enrich 委托 PollStore.EnrichPollForViewers（模板一次）。bot 历史过滤
// 在 app 层叠加。Polls：key 存在=已评估；nil=不可见；非 nil=可见 enrich poll。
func (s *ChannelStore) ChannelPollFanoutViews(_ context.Context, channelID int64, msgID int, viewers []int64, now int) (domain.ChannelPollFanoutViews, error) {
	out := domain.ChannelPollFanoutViews{Polls: map[int64]*domain.MessagePoll{}}
	if s == nil || s.polls == nil || channelID == 0 || msgID <= 0 || len(viewers) == 0 {
		return out, nil
	}
	if now == 0 {
		now = int(time.Now().Unix())
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return out, nil
	}
	msg, ok := s.findMessageLocked(channelID, msgID)
	if !ok || msg.Deleted || msg.Action != nil || msg.Media == nil || msg.Media.Kind != domain.MessageMediaKindPoll || msg.Media.Poll == nil || msg.Media.Poll.ID == 0 {
		return out, nil
	}
	out.Found = true
	out.Message = cloneChannelMessage(msg)
	visible := make([]int64, 0, len(viewers))
	for _, viewer := range viewers {
		if viewer == 0 {
			continue
		}
		member, ok := s.members[channelID][viewer]
		if !ok || member.Status != domain.ChannelMemberActive || member.BannedRights.ViewMessages || (member.AvailableMinID > 0 && msgID <= member.AvailableMinID) {
			out.Polls[viewer] = nil // 已评估但不可见
			continue
		}
		visible = append(visible, viewer)
	}
	enriched := s.polls.EnrichPollForViewers(msg.Media.Poll, visible, now)
	for viewer, poll := range enriched {
		out.Polls[viewer] = poll
	}
	return out, nil
}

// channelPollResult 为投票者视角组装结果；实时 fan-out 与 reaction 同款由 rpc 层按 viewer 重建。
func (s *ChannelStore) channelPollResult(channel domain.Channel, msg domain.ChannelMessage, viewerUserID int64, now int) domain.ChannelMessagePollResult {
	msg.Media = enrichPollMediaForViewer(s.polls, msg.Media, viewerUserID, now)
	return domain.ChannelMessagePollResult{
		PollID:     msg.Media.Poll.ID,
		Channel:    channel,
		Message:    msg,
		Recipients: []int64{viewerUserID, msg.SenderUserID},
	}
}
