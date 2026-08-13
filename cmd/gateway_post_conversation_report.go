package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

const defaultPostConversationReportIdle = 5 * time.Minute

type postConversationReporter struct {
	ctx    context.Context
	mu     sync.Mutex
	timers map[string]*time.Timer
	seq    map[string]int64
}

type postConversationReportConfig struct {
	Enabled            bool
	SourceChannels     []string
	SourceChannelTypes []string
	TargetChannel      string
	TargetChatID       string
	IdleTimeout        time.Duration
	MaxMessages        int
}

type postConversationReportEvent struct {
	AgentKey         string
	Channel          string
	ChannelType      string
	ChatID           string
	PeerKind         string
	SessionKey       string
	InboundContent   string
	Metadata         map[string]string
	TenantID         uuid.UUID
	AgentUUID        uuid.UUID
	AgentOtherConfig []byte
}

func newPostConversationReporter(ctx context.Context) *postConversationReporter {
	return &postConversationReporter{
		ctx:    ctx,
		timers: make(map[string]*time.Timer),
		seq:    make(map[string]int64),
	}
}

func (r *postConversationReporter) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, timer := range r.timers {
		timer.Stop()
		delete(r.timers, key)
	}
	clear(r.seq)
}

func (r *postConversationReporter) Schedule(ctx context.Context, deps *ConsumerDeps, ev postConversationReportEvent) {
	if r == nil || deps == nil || deps.MsgBus == nil || deps.SessStore == nil {
		return
	}
	cfg, ok := parsePostConversationReportConfig(ev.AgentOtherConfig)
	if !ok || !postConversationReportMatches(cfg, ev.Channel, ev.ChannelType) {
		return
	}
	r.scheduleParsed(ctx, deps, cfg, ev)
}

func (r *postConversationReporter) scheduleParsed(ctx context.Context, deps *ConsumerDeps, cfg postConversationReportConfig, ev postConversationReportEvent) {
	key := postConversationReportKey(ev)

	r.mu.Lock()
	r.seq[key]++
	seq := r.seq[key]
	if old := r.timers[key]; old != nil {
		old.Stop()
	}
	r.timers[key] = time.AfterFunc(cfg.IdleTimeout, func() {
		r.fire(ctx, deps, cfg, ev, key, seq)
	})
	r.mu.Unlock()
}

func (r *postConversationReporter) fire(ctx context.Context, deps *ConsumerDeps, cfg postConversationReportConfig, ev postConversationReportEvent, key string, seq int64) {
	select {
	case <-r.ctx.Done():
		return
	default:
	}

	r.mu.Lock()
	current := r.seq[key]
	if current != seq {
		r.mu.Unlock()
		return
	}
	delete(r.timers, key)
	delete(r.seq, key)
	r.mu.Unlock()

	if deps.Agents != nil && deps.Agents.IsSessionBusy(ev.SessionKey) {
		if cfg.IdleTimeout > 30*time.Second {
			cfg.IdleTimeout = 30 * time.Second
		}
		r.scheduleParsed(ctx, deps, cfg, ev)
		return
	}

	reportCtx := context.Background()
	if ev.TenantID != uuid.Nil {
		reportCtx = store.WithTenantID(reportCtx, ev.TenantID)
	} else {
		reportCtx = store.WithTenantID(reportCtx, store.MasterTenantID)
	}
	content := buildPostConversationReport(deps.SessStore.GetHistory(reportCtx, ev.SessionKey), ev, cfg)
	if strings.TrimSpace(content) == "" {
		return
	}
	deps.MsgBus.PublishOutbound(bus.OutboundMessage{
		Channel:          cfg.TargetChannel,
		ChatID:           cfg.TargetChatID,
		Content:          content,
		Metadata:         map[string]string{"source": "post_conversation_report", "source_channel": ev.Channel, "source_chat_id": ev.ChatID},
		TenantID:         ev.TenantID,
		AgentID:          ev.AgentUUID,
		AgentOtherConfig: ev.AgentOtherConfig,
	})
	slog.Info("post_conversation_report.sent",
		"source_channel", ev.Channel,
		"source_chat_id", ev.ChatID,
		"target_channel", cfg.TargetChannel,
		"target_chat_id", cfg.TargetChatID,
		"session", ev.SessionKey,
	)
}

func parsePostConversationReportConfig(raw []byte) (postConversationReportConfig, bool) {
	cfg := postConversationReportConfig{IdleTimeout: defaultPostConversationReportIdle, MaxMessages: 12}
	if len(raw) == 0 || string(raw) == "null" {
		return cfg, false
	}
	var bag map[string]json.RawMessage
	if err := json.Unmarshal(raw, &bag); err != nil {
		return cfg, false
	}
	node, ok := bag["post_conversation_report"]
	if !ok {
		return cfg, false
	}
	var wire struct {
		Enabled            bool     `json:"enabled"`
		SourceChannels     []string `json:"source_channels"`
		SourceChannelTypes []string `json:"source_channel_types"`
		TargetChannel      string   `json:"target_channel"`
		TargetChatID       string   `json:"target_chat_id"`
		IdleTimeoutSeconds int      `json:"idle_timeout_seconds"`
		MaxMessages        int      `json:"max_messages"`
	}
	if err := json.Unmarshal(node, &wire); err != nil || !wire.Enabled {
		return cfg, false
	}
	cfg.Enabled = wire.Enabled
	cfg.SourceChannels = wire.SourceChannels
	cfg.SourceChannelTypes = wire.SourceChannelTypes
	cfg.TargetChannel = strings.TrimSpace(wire.TargetChannel)
	cfg.TargetChatID = strings.TrimSpace(wire.TargetChatID)
	if wire.IdleTimeoutSeconds > 0 {
		cfg.IdleTimeout = time.Duration(wire.IdleTimeoutSeconds) * time.Second
	}
	if wire.MaxMessages > 0 {
		cfg.MaxMessages = wire.MaxMessages
	}
	if cfg.TargetChannel == "" || cfg.TargetChatID == "" {
		return cfg, false
	}
	return cfg, true
}

func postConversationReportMatches(cfg postConversationReportConfig, channel, channelType string) bool {
	if !cfg.Enabled {
		return false
	}
	if len(cfg.SourceChannels) == 0 && len(cfg.SourceChannelTypes) == 0 {
		return true
	}
	if slices.Contains(cfg.SourceChannels, channel) {
		return true
	}
	return channelType != "" && slices.Contains(cfg.SourceChannelTypes, channelType)
}

func postConversationReportKey(ev postConversationReportEvent) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s", ev.TenantID, ev.AgentUUID, ev.Channel, ev.ChatID, ev.PeerKind, ev.SessionKey)
}

func buildPostConversationReport(history []providers.Message, ev postConversationReportEvent, cfg postConversationReportConfig) string {
	chatName := strings.TrimSpace(ev.Metadata[tools.MetaChatTitle])
	if chatName == "" {
		chatName = strings.TrimSpace(ev.Metadata["display_name"])
	}
	if chatName == "" {
		chatName = ev.ChatID
	}
	lastCustomer := strings.TrimSpace(ev.InboundContent)
	if lastCustomer == "" {
		lastCustomer = lastMessageByRole(history, "user")
	}
	lastAssistant := lastMessageByRole(history, "assistant")
	transcript := compactConversationExcerpt(history, cfg.MaxMessages)

	var b strings.Builder
	b.WriteString("Tóm tắt Zalo sau 5 phút không có tin mới\n")
	b.WriteString("Khách/chat: ")
	b.WriteString(truncateForReport(chatName, 120))
	if ev.ChatID != "" && ev.ChatID != chatName {
		b.WriteString(" (")
		b.WriteString(truncateForReport(ev.ChatID, 80))
		b.WriteString(")")
	}
	b.WriteString("\nTin khách gần nhất: \"")
	b.WriteString(truncateForReport(lastCustomer, 700))
	b.WriteString("\"")
	if transcript != "" {
		b.WriteString("\nNội dung trao đổi gần đây:\n")
		b.WriteString(transcript)
	}
	if lastAssistant != "" {
		b.WriteString("\nSochi đã phản hồi: ")
		b.WriteString(truncateForReport(lastAssistant, 700))
	}
	b.WriteString("\nCần xử lý: đội vận hành/Cường xem và tiếp tục hỗ trợ nếu cần.")
	return b.String()
}

func lastMessageByRole(history []providers.Message, role string) string {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == role {
			if content := strings.TrimSpace(history[i].Content); content != "" {
				return content
			}
		}
	}
	return ""
}

func compactConversationExcerpt(history []providers.Message, maxMessages int) string {
	if maxMessages <= 0 {
		maxMessages = 12
	}
	var filtered []providers.Message
	for _, msg := range history {
		if msg.Role != "user" && msg.Role != "assistant" {
			continue
		}
		if strings.TrimSpace(msg.Content) == "" {
			continue
		}
		filtered = append(filtered, msg)
	}
	if len(filtered) > maxMessages {
		filtered = filtered[len(filtered)-maxMessages:]
	}
	var lines []string
	for _, msg := range filtered {
		prefix := "Khách"
		if msg.Role == "assistant" {
			prefix = "Sochi"
		}
		lines = append(lines, prefix+": "+truncateForReport(msg.Content, 320))
	}
	return strings.Join(lines, "\n")
}

func truncateForReport(s string, max int) string {
	s = strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
	if max <= 0 || len([]rune(s)) <= max {
		return s
	}
	runes := []rune(s)
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}
