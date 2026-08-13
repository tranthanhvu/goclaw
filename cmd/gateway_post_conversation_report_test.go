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
			"max_messages": 8,
			"group_names": {"8897171220293491311": "ATH - Sửa điện nước"}
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
	if cfg.GroupNames["8897171220293491311"] != "ATH - Sửa điện nước" {
		t.Fatalf("group name not parsed: %#v", cfg.GroupNames)
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
	if !strings.Contains(out.Content, `nội dung: khách hỏi ngoài phạm vi: "Gia vang hom nay bao nhieu?"`) {
		t.Fatalf("report did not quote latest customer message: %s", out.Content)
	}
	if strings.Contains(out.Content, "first message") {
		t.Fatalf("stale timer content leaked into report: %s", out.Content)
	}
}

func TestBuildPostConversationReportSummarizesGroupWithoutTranscript(t *testing.T) {
	history := []providers.Message{
		{Role: "user", Content: `[Chat messages since your last reply - for context]
  Ngân Lê [12:01]: Mãi chưa thu tiền khách thuê đc
  Hoàng Phương [12:02]: @Ngân Lê bữa e k thấy tn sr chị nha , mai làm liền á
  Trần Thanh Vũ [12:03]: @Mã Anh Hào thẻ thang máy 74 e sao ra chưa Hào?
  Mã Anh Hào [12:04]: @Trần Thanh Vũ em kím chổ sao chưa ra anh ! Anh vũ biết chổ nào sao không anh?
  Trần Thanh Vũ [12:05]: Chừng nào qua làm Tân Phú thì đưa lại a nghen, a đưa bạn lắp thang máy 74 làm lại

[Your current message]
[From: Mã Anh Hào]
Dạ`},
		{Role: "assistant", Content: "NO_REPLY"},
	}
	report := buildPostConversationReport(history, postConversationReportEvent{
		ChatID:         "8897171220293491311",
		PeerKind:       "group",
		InboundContent: "[From: Mã Anh Hào]\nDạ",
	}, postConversationReportConfig{
		MaxMessages: 12,
		GroupNames:  map[string]string{"8897171220293491311": "ATH - Sửa điện nước"},
	})

	for _, want := range []string{
		"---------------------------------------",
		"Zalo - ATH - Sửa điện nước.",
		"nội dung: anh Vũ và Hào trao đổi về thẻ thang máy 74; Hào sẽ gửi anh Vũ khi qua Tân Phú",
		"Cần xử lý: không",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
	for _, forbidden := range []string{"Khách:", "Sochi:", "NO_REPLY", "Nội dung trao đổi gần đây", "Mãi chưa thu tiền", "em kím chổ sao chưa ra"} {
		if strings.Contains(report, forbidden) {
			t.Fatalf("report leaked transcript marker %q:\n%s", forbidden, report)
		}
	}
}
