package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

type postConversationReportSessionStore struct {
	store.SessionStore
	history []providers.Message
}

func (s *postConversationReportSessionStore) GetHistory(context.Context, string) []providers.Message {
	return append([]providers.Message(nil), s.history...)
}

func TestParsePostConversationReportConfig(t *testing.T) {
	raw := json.RawMessage(`{
		"post_conversation_report": {
			"enabled": true,
			"source_channels": ["zalo-personal"],
			"source_channel_types": ["zalo_personal"],
			"target_channel": "bao-cao",
			"target_chat_id": "-5479458662",
			"idle_timeout_seconds": 300,
			"max_messages": 8
		}
	}`)

	cfg, ok := parsePostConversationReportConfig(raw)
	if !ok {
		t.Fatal("expected config to parse")
	}
	if cfg.TargetChannel != "bao-cao" || cfg.TargetChatID != "-5479458662" {
		t.Fatalf("target = %q/%q, want bao-cao/-5479458662", cfg.TargetChannel, cfg.TargetChatID)
	}
	if cfg.IdleTimeout != 5*time.Minute {
		t.Fatalf("idle timeout = %v, want 5m", cfg.IdleTimeout)
	}
	if !postConversationReportMatches(cfg, "zalo-personal", "zalo_personal") {
		t.Fatal("expected config to match Zalo source")
	}
	if postConversationReportMatches(cfg, "telegram-main", "telegram") {
		t.Fatal("expected config not to match Telegram source")
	}
}

func TestPostConversationReporterResetsTimerAndSendsLatestQuote(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := newPostConversationReporter(ctx)
	defer reporter.Stop()

	msgBus := bus.New()
	deps := &ConsumerDeps{
		MsgBus: msgBus,
		SessStore: &postConversationReportSessionStore{history: []providers.Message{
			{Role: "user", Content: "Gia vang hom nay bao nhieu?"},
			{Role: "assistant", Content: "Phan nay minh khong xu ly duoc. Ban lien he anh Cuong giup minh."},
		}},
	}
	cfg := postConversationReportConfig{
		Enabled:        true,
		SourceChannels: []string{"zalo-personal"},
		TargetChannel:  "bao-cao",
		TargetChatID:   "-5479458662",
		IdleTimeout:    20 * time.Millisecond,
		MaxMessages:    4,
	}
	ev := postConversationReportEvent{
		Channel:        "zalo-personal",
		ChannelType:    "zalo_personal",
		ChatID:         "zalo-chat-1",
		PeerKind:       "direct",
		SessionKey:     "session-1",
		InboundContent: "first message",
		TenantID:       uuid.New(),
		AgentUUID:      uuid.New(),
	}
	reporter.scheduleParsed(ctx, deps, cfg, ev)
	ev.InboundContent = "Gia vang hom nay bao nhieu?"
	reporter.scheduleParsed(ctx, deps, cfg, ev)

	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()
	out, ok := msgBus.SubscribeOutbound(waitCtx)
	if !ok {
		t.Fatal("outbound bus closed before report")
	}
	if out.Channel != "bao-cao" || out.ChatID != "-5479458662" {
		t.Fatalf("outbound target = %q/%q, want bao-cao/-5479458662", out.Channel, out.ChatID)
	}
	if !strings.Contains(out.Content, `Tin khách gần nhất: "Gia vang hom nay bao nhieu?"`) {
		t.Fatalf("report did not quote latest customer message: %s", out.Content)
	}
	if strings.Contains(out.Content, "first message") {
		t.Fatalf("stale timer content leaked into report: %s", out.Content)
	}
}
