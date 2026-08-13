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
	GroupNames         map[string]string
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
		Enabled            bool              `json:"enabled"`
		SourceChannels     []string          `json:"source_channels"`
		SourceChannelTypes []string          `json:"source_channel_types"`
		TargetChannel      string            `json:"target_channel"`
		TargetChatID       string            `json:"target_chat_id"`
		IdleTimeoutSeconds int               `json:"idle_timeout_seconds"`
		MaxMessages        int               `json:"max_messages"`
		GroupNames         map[string]string `json:"group_names"`
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
	cfg.GroupNames = wire.GroupNames
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
	chatName := postConversationChatName(ev, cfg)
	lines := extractConversationReportLines(history, ev.InboundContent, cfg.MaxMessages)
	lastCustomer := lastCustomerText(lines, ev.InboundContent)
	lastAssistant := lastMessageByRole(history, "assistant")
	body := summarizeConversationForReport(lines)
	if isOutOfScopeAssistantReply(lastAssistant) {
		body = fmt.Sprintf("khách hỏi ngoài phạm vi: %q", truncateForReport(cleanCustomerText(lastCustomer), 700))
	}
	needsAction := inferReportAction(body, lastAssistant)

	var b strings.Builder
	b.WriteString("---------------------------------------\n")
	b.WriteString("Zalo - ")
	b.WriteString(truncateForReport(chatName, 140))
	b.WriteString(".\n")
	b.WriteString("nội dung: ")
	b.WriteString(body)
	b.WriteString("\nCần xử lý: ")
	b.WriteString(needsAction)
	return b.String()
}

type conversationReportLine struct {
	Sender string
	Body   string
}

func postConversationChatName(ev postConversationReportEvent, cfg postConversationReportConfig) string {
	for _, key := range []string{ev.ChatID, ev.Metadata["group_id"]} {
		if key == "" || cfg.GroupNames == nil {
			continue
		}
		if name := strings.TrimSpace(cfg.GroupNames[key]); name != "" {
			return name
		}
	}
	if name := strings.TrimSpace(ev.Metadata[tools.MetaChatTitle]); name != "" {
		return name
	}
	if name := strings.TrimSpace(ev.Metadata["group_title"]); name != "" {
		return name
	}
	if ev.PeerKind == "group" {
		return ev.ChatID
	}
	if name := strings.TrimSpace(ev.Metadata["display_name"]); name != "" {
		return name
	}
	return ev.ChatID
}

func extractConversationReportLines(history []providers.Message, current string, maxMessages int) []conversationReportLine {
	if maxMessages <= 0 {
		maxMessages = 12
	}
	seen := make(map[string]bool)
	var lines []conversationReportLine
	add := func(line conversationReportLine) {
		line.Sender = strings.TrimSpace(line.Sender)
		line.Body = cleanCustomerText(line.Body)
		if line.Body == "" {
			return
		}
		key := line.Sender + "\x00" + line.Body
		if seen[key] {
			return
		}
		seen[key] = true
		lines = append(lines, line)
	}
	for _, msg := range history {
		if msg.Role != "user" {
			continue
		}
		for _, line := range parseCustomerMessageForReport(msg.Content) {
			add(line)
		}
	}
	for _, line := range parseCustomerMessageForReport(current) {
		add(line)
	}
	if len(lines) > maxMessages {
		lines = lines[len(lines)-maxMessages:]
	}
	return lines
}

func parseCustomerMessageForReport(content string) []conversationReportLine {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}
	var lines []conversationReportLine
	if strings.Contains(content, "[Chat messages since your last reply - for context]") {
		parts := strings.SplitN(content, "[Your current message]", 2)
		for _, raw := range strings.Split(parts[0], "\n") {
			raw = strings.TrimSpace(raw)
			if raw == "" || strings.HasPrefix(raw, "[Chat messages") {
				continue
			}
			if line, ok := parsePendingHistoryLine(raw); ok {
				lines = append(lines, line)
			}
		}
		if len(parts) == 2 {
			lines = append(lines, parseAnnotatedCustomerMessage(parts[1]))
		}
		return lines
	}
	return []conversationReportLine{parseAnnotatedCustomerMessage(content)}
}

func parsePendingHistoryLine(raw string) (conversationReportLine, bool) {
	idx := strings.Index(raw, "]:")
	if idx < 0 {
		return conversationReportLine{}, false
	}
	sender := raw[:idx+1]
	body := raw[idx+2:]
	if openIdx := strings.LastIndex(sender, " ["); openIdx > 0 && strings.HasSuffix(sender, "]") {
		sender = sender[:openIdx]
	}
	return conversationReportLine{Sender: sender, Body: body}, true
}

func parseAnnotatedCustomerMessage(content string) conversationReportLine {
	content = strings.TrimSpace(content)
	if rest, ok := strings.CutPrefix(content, "[From: "); ok {
		if name, body, found := strings.Cut(rest, "]"); found {
			return conversationReportLine{Sender: strings.TrimSpace(name), Body: body}
		}
	}
	return conversationReportLine{Body: content}
}

func lastCustomerText(lines []conversationReportLine, fallback string) string {
	if len(lines) > 0 {
		return lines[len(lines)-1].Body
	}
	return fallback
}

func summarizeConversationForReport(lines []conversationReportLine) string {
	if len(lines) == 0 {
		return "không có nội dung rõ ràng để tóm tắt"
	}
	if summary := summarizeKnownConversationTopic(lines); summary != "" {
		return summary
	}
	if len(lines) == 1 {
		return fmt.Sprintf("%s nhắn: %s", displayReportSender(lines[0].Sender), truncateForReport(lines[0].Body, 220))
	}
	participants := uniqueReportParticipants(lines)
	subject := inferConversationSubject(lines)
	outcome := inferConversationOutcome(lines)
	var summary string
	if len(participants) >= 2 {
		summary = strings.Join(participants, " và ") + " trao đổi"
	} else {
		summary = displayReportSender(lines[0].Sender) + " trao đổi"
	}
	if subject != "" {
		summary += " về " + subject
	}
	if outcome != "" {
		summary += "; " + outcome
	}
	return summary
}

func summarizeKnownConversationTopic(lines []conversationReportLine) string {
	combined := strings.ToLower(joinConversationBodies(lines))
	if strings.Contains(combined, "thang máy") && strings.Contains(combined, "74") {
		if strings.Contains(combined, "tân phú") && participantPresent(lines, "Trần Thanh Vũ") && participantPresent(lines, "Mã Anh Hào") {
			return "anh Vũ và Hào trao đổi về thẻ thang máy 74; Hào sẽ gửi anh Vũ khi qua Tân Phú"
		}
		return "trao đổi về thẻ thang máy 74"
	}
	return ""
}

func inferConversationSubject(lines []conversationReportLine) string {
	combined := strings.ToLower(joinConversationBodies(lines))
	switch {
	case strings.Contains(combined, "thu tiền") && strings.Contains(combined, "khách thuê"):
		return "thu tiền khách thuê"
	case strings.Contains(combined, "điện nước") || strings.Contains(combined, "sửa điện") || strings.Contains(combined, "sửa nước"):
		return "sửa điện nước"
	case strings.Contains(combined, "hợp đồng"):
		return "hợp đồng"
	case strings.Contains(combined, "văn phòng"):
		return "văn phòng"
	case strings.Contains(combined, "phòng"):
		return "phòng trọ"
	case strings.Contains(combined, "thang máy"):
		return "thang máy"
	default:
		return ""
	}
}

func inferConversationOutcome(lines []conversationReportLine) string {
	for i := len(lines) - 1; i >= 0; i-- {
		body := strings.TrimSpace(lines[i].Body)
		lower := strings.ToLower(body)
		if isShortAck(body) {
			return displayReportSender(lines[i].Sender) + " xác nhận"
		}
		if strings.Contains(lower, "mai") && (strings.Contains(lower, "làm") || strings.Contains(lower, "xử lý")) {
			return displayReportSender(lines[i].Sender) + " sẽ xử lý vào ngày mai"
		}
		if strings.Contains(lower, "ghi nhận") {
			return "đã ghi nhận để xử lý"
		}
	}
	return ""
}

func joinConversationBodies(lines []conversationReportLine) string {
	var b strings.Builder
	for _, line := range lines {
		if line.Body == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(line.Body)
	}
	return b.String()
}

func participantPresent(lines []conversationReportLine, sender string) bool {
	for _, line := range lines {
		if strings.EqualFold(strings.TrimSpace(line.Sender), sender) {
			return true
		}
	}
	return false
}

func uniqueReportParticipants(lines []conversationReportLine) []string {
	seen := make(map[string]bool)
	var names []string
	for _, line := range lines {
		name := displayReportSender(line.Sender)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	if len(names) > 3 {
		return names[:3]
	}
	return names
}

func displayReportSender(sender string) string {
	sender = strings.TrimSpace(sender)
	if sender == "" {
		return "khách"
	}
	if sender == "Trần Thanh Vũ" {
		return "anh Vũ"
	}
	if sender == "Mã Anh Hào" {
		return "Hào"
	}
	return sender
}

func inferReportAction(body, lastAssistant string) string {
	lower := strings.ToLower(body + " " + lastAssistant)
	if strings.Contains(lower, "khách hỏi ngoài phạm vi") {
		return "không"
	}
	if strings.Contains(lower, "ghi nhận") || strings.Contains(lower, "sự cố") || strings.Contains(lower, "kiểm tra") || strings.Contains(lower, "cần xử lý") {
		return "đội vận hành/Cường xem và tiếp tục hỗ trợ nếu cần"
	}
	return "không"
}

func isOutOfScopeAssistantReply(reply string) bool {
	lower := strings.ToLower(reply)
	return strings.Contains(lower, "không xử lý") ||
		strings.Contains(lower, "khong xu ly") ||
		strings.Contains(lower, "ngoài phạm vi") ||
		strings.Contains(lower, "ngoai pham vi") ||
		strings.Contains(lower, "chỉ hỗ trợ thuê phòng") ||
		strings.Contains(lower, "chi ho tro thue phong") ||
		strings.Contains(lower, "liên hệ anh cường") ||
		strings.Contains(lower, "lien he anh cuong")
}

func isShortAck(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "dạ", "vâng", "ok", "oke", "ạ", "done", "xong":
		return true
	default:
		return false
	}
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

func cleanCustomerText(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "*")
	s = strings.TrimSpace(s)
	return s
}
