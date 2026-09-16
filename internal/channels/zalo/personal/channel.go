package personal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/typing"
	"github.com/nextlevelbuilder/goclaw/internal/channels/zalo/personal/protocol"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// Channel connects to Zalo Personal Chat via the internal protocol port (from zcago, MIT).
// WARNING: Zalo Personal is an unofficial, reverse-engineered integration. Account may be locked/banned.
type Channel struct {
	*channels.BaseChannel
	config      config.ZaloPersonalConfig
	typingCtrls sync.Map // threadID → *typing.Controller

	mu       sync.RWMutex // protects sess and listener
	sess     *protocol.Session
	listener *protocol.Listener

	// Pre-loaded credentials (from DB or from file/QR as fallback).
	preloadedCreds   *protocol.Credentials
	connectorIntake  athconnector.GroupIntake         // ATH restricted onboarding intake; nil in the generic runtime
	connectorHarness *athconnector.Harness            // loader-injected enablement; armed at Start once the account is verified
	connectorCatalog *athconnector.GroupCatalogWorker // joined-group catalog refresh; stopped with the channel

	stopCh   chan struct{}
	stopOnce sync.Once
}

// SetGroupIntake arms the ATH restricted onboarding intake. Must be called by
// the isolated connector service assembly before Start; the generic runtime
// never sets it.
func (c *Channel) SetGroupIntake(intake athconnector.GroupIntake) { c.connectorIntake = intake }

// SetConnectorHarness installs the loader-provided connector enablement. The
// restricted intake and catalog worker stay inert until Start verifies the
// provider account.
func (c *Channel) SetConnectorHarness(h *athconnector.Harness) { c.connectorHarness = h }

// New creates a new Zalo Personal channel from config.
func New(cfg config.ZaloPersonalConfig, msgBus *bus.MessageBus, pairingSvc store.PairingStore, pendingStore store.PendingMessageStore) (*Channel, error) {
	base := channels.NewBaseChannel(channels.TypeZaloPersonal, msgBus, cfg.AllowFrom)

	if cfg.DMPolicy == "" {
		cfg.DMPolicy = "allowlist"
	}
	if cfg.GroupPolicy == "" {
		cfg.GroupPolicy = "allowlist"
	}
	base.ValidatePolicy(cfg.DMPolicy, cfg.GroupPolicy)

	historyLimit := cfg.HistoryLimit
	if historyLimit == 0 {
		historyLimit = channels.DefaultGroupHistoryLimit
	}

	requireMention := true
	if cfg.RequireMention != nil {
		requireMention = *cfg.RequireMention
	}

	ch := &Channel{
		BaseChannel: base,
		config:      cfg,
		stopCh:      make(chan struct{}),
	}
	ch.SetPairingService(pairingSvc)
	ch.SetGroupHistory(channels.MakeHistory(channels.TypeZaloPersonal, pendingStore, base.TenantID()))
	ch.SetHistoryLimit(historyLimit)
	ch.SetRequireMention(requireMention)
	return ch, nil
}

// BlockReplyEnabled returns the per-channel block_reply override (nil = inherit gateway default).
func (c *Channel) BlockReplyEnabled() *bool { return c.config.BlockReply }

// ChatBehaviorConfig returns the per-channel chat_behavior override.
func (c *Channel) ChatBehaviorConfig() *config.ChatBehaviorConfig { return c.config.ChatBehavior }

// session returns the current session snapshot (thread-safe).
func (c *Channel) session() *protocol.Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sess
}

// getListener returns the current listener snapshot (thread-safe).
func (c *Channel) getListener() *protocol.Listener {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.listener
}

// Start authenticates and begins listening for Zalo messages.
func (c *Channel) Start(ctx context.Context) error {
	if gh := c.GroupHistory(); gh != nil {
		gh.StartFlusher()
	}
	slog.Warn("security.unofficial_api",
		"channel", "zalo_personal",
		"msg", "Zalo Personal is unofficial and reverse-engineered. Account may be locked/banned. Use at own risk.",
	)

	sess, err := c.authenticate(ctx)
	if err != nil {
		return fmt.Errorf("zalo_personal auth: %w", err)
	}

	ln, err := protocol.NewListener(sess)
	if err != nil {
		return fmt.Errorf("zalo_personal listener: %w", err)
	}
	if err := ln.Start(ctx); err != nil {
		return fmt.Errorf("zalo_personal listener start: %w", err)
	}

	c.mu.Lock()
	c.sess = sess
	c.listener = ln
	c.mu.Unlock()

	// Arm the ATH restricted intake and the group catalog worker now that the
	// provider account identity is verified. Any failure leaves them unarmed:
	// denied groups keep falling through generic policy instead of trusting
	// unproven provenance, and refresh jobs wait for the next start.
	if c.connectorHarness != nil {
		if intake, err := c.connectorHarness.ArmGroupIntake(ctx, c.TenantID(), c.ChannelInstanceID(), channels.TypeZaloPersonal, sess.UID); err != nil {
			slog.Warn("zalo_personal connector intake not armed (fail closed)", "uid", sess.UID, "error", err)
		} else {
			c.SetGroupIntake(intake)
			slog.Info("zalo_personal connector intake armed", "uid", sess.UID, "instance", c.ChannelInstanceID())
		}
		workerID := "zalo-personal:" + c.ChannelInstanceID().String()
		if worker, err := c.connectorHarness.ArmCatalogWorker(ctx, c.TenantID(), c.ChannelInstanceID(), channels.TypeZaloPersonal, sess.UID, workerID, zaloCatalogSource{channel: c}, 15*time.Second); err != nil {
			slog.Warn("zalo_personal connector catalog worker not armed (fail closed)", "uid", sess.UID, "error", err)
		} else {
			c.connectorCatalog = worker
			worker.Start()
			slog.Info("zalo_personal connector catalog worker armed", "uid", sess.UID, "instance", c.ChannelInstanceID())
		}
	}

	slog.Info("zalo_personal connected", "uid", sess.UID)

	c.SetRunning(true)
	go c.listenLoop(ctx)

	slog.Info("zalo_personal listener loop started")
	return nil
}

// SetPendingCompaction configures LLM-based auto-compaction for pending messages.
func (c *Channel) SetPendingCompaction(cfg *channels.CompactionConfig) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetCompactionConfig(cfg)
	}
}

// SetPendingHistoryTenantID propagates tenant_id to the pending history for DB operations.
func (c *Channel) SetPendingHistoryTenantID(id uuid.UUID) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetTenantID(id)
	}
}

// Stop gracefully shuts down the Zalo Personal channel.
func (c *Channel) Stop(_ context.Context) error {
	if gh := c.GroupHistory(); gh != nil {
		gh.StopFlusher()
	}
	slog.Info("stopping zalo_personal channel")
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.typingCtrls.Range(func(key, val any) bool {
		if ctrl, ok := val.(*typing.Controller); ok {
			ctrl.Stop()
		}
		c.typingCtrls.Delete(key)
		return true
	})
	if ln := c.getListener(); ln != nil {
		ln.Stop()
	}
	if c.connectorCatalog != nil {
		c.connectorCatalog.Stop()
	}
	c.SetRunning(false)
	return nil
}
