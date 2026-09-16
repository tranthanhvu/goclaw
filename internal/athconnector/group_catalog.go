package athconnector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Catalog transport limits mirroring the gateway contract.
const (
	CatalogMaxEntriesPerPage = 500
	CatalogMaxPages          = 100
	CatalogMaxTotalEntries   = 10000
	CatalogDisplayNameLimit  = 300
	CatalogLeaseSeconds      = 30
)

// catalogFailureCodePattern mirrors the gateway's failure_code contract.
const catalogFailureCodeAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789_.-"

// CatalogGroup is one joined provider group observed through the isolated
// connector's authenticated session — never chat history or pairing state.
type CatalogGroup struct {
	ConversationID string
	DisplayName    string
	Available      bool
}

// GroupCatalogSource fetches the full joined-group list for the authenticated
// account. Implemented by the channel adapter over protocol.FetchGroups.
type GroupCatalogSource interface {
	FetchJoinedGroups(ctx context.Context) ([]CatalogGroup, error)
}

// CatalogEntryMetadata is the bounded, account-scoped metadata each catalog
// entry carries. The gateway rejects any other metadata key.
type CatalogEntryMetadata struct {
	ChannelInstanceID   uuid.UUID
	Provider            string
	ProviderAccountHint string
	ObservedAt          time.Time
}

// CatalogEntry is one published catalog row.
type CatalogEntry struct {
	ConversationID string
	DisplayName    string
	Metadata       CatalogEntryMetadata
	IsAvailable    bool
}

// jsonbString renders s exactly as Postgres jsonb::text renders string values:
// JSON escaping without HTML escaping (which Go's default marshal applies to
// <, >, &). This keeps digests byte-compatible with the gateway.
func jsonbString(s string) (string, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(s); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// CanonicalJSONB renders one entry in Postgres jsonb key order (alphabetical:
// conversation_id, display_name, is_available, metadata; metadata keys:
// channel_instance_id, observed_at, provider, provider_account_hint) with
// jsonb separators (", " and ": "), so sha256 over the bytes equals the
// gateway's digest over _entries::text.
func (e CatalogEntry) CanonicalJSONB() ([]byte, error) {
	conversation, err := jsonbString(e.ConversationID)
	if err != nil {
		return nil, err
	}
	display, err := jsonbString(e.DisplayName)
	if err != nil {
		return nil, err
	}
	var hint string
	if e.Metadata.ProviderAccountHint != "" {
		hint, err = jsonbString(e.Metadata.ProviderAccountHint)
		if err != nil {
			return nil, err
		}
	}
	observedAt := e.Metadata.ObservedAt.UTC().Format(time.RFC3339)
	var metadata strings.Builder
	metadata.WriteString(`{"channel_instance_id": "`)
	metadata.WriteString(e.Metadata.ChannelInstanceID.String())
	metadata.WriteString(`", "observed_at": "`)
	metadata.WriteString(observedAt)
	metadata.WriteString(`", "provider": "`)
	metadata.WriteString(e.Metadata.Provider)
	metadata.WriteString(`"`)
	if hint != "" {
		metadata.WriteString(`, "provider_account_hint": `)
		metadata.WriteString(hint)
	}
	metadata.WriteString("}")
	return []byte(fmt.Sprintf(`{"conversation_id": %s, "display_name": %s, "is_available": %t, "metadata": %s}`,
		conversation, display, e.IsAvailable, metadata.String())), nil
}

// CanonicalEntriesJSONB renders the array form; sha256 over the result is the
// page digest (one page) or the catalog digest (all entries).
func CanonicalEntriesJSONB(entries []CatalogEntry) ([]byte, error) {
	if len(entries) == 0 {
		return []byte("[]"), nil
	}
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		rendered, err := entry.CanonicalJSONB()
		if err != nil {
			return nil, err
		}
		parts = append(parts, string(rendered))
	}
	return []byte("[" + strings.Join(parts, ", ") + "]"), nil
}

// Digest returns the raw SHA-256 over the canonical jsonb array text.
func Digest(entries []CatalogEntry) ([]byte, error) {
	canonical, err := CanonicalEntriesJSONB(entries)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

// EncodeDigest renders a raw digest in the gateway's 43-char base64url form.
func EncodeDigest(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

// CatalogJob is a claimed refresh job with its lease.
type CatalogJob struct {
	JobID          uuid.UUID
	Generation     int
	LeaseToken     uuid.UUID
	LeaseExpiresAt time.Time
}

// CatalogFinalizeResult reports the committed snapshot.
type CatalogFinalizeResult struct {
	SnapshotID uuid.UUID
	Generation int
	EntryCount int
}

func (c *ControlClient) accountScope(binding AccountBinding) AssertionScope {
	return AssertionScope{
		ConnectorID:  c.cfg.ConnectorID,
		AccountID:    binding.AccountID,
		AccountEpoch: binding.AccountEpoch,
	}
}

// ClaimCatalogJob claims one pending refresh job for the account. A nil job
// means no work is queued.
func (c *ControlClient) ClaimCatalogJob(ctx context.Context, binding AccountBinding, workerID string) (*CatalogJob, error) {
	body := map[string]any{
		"account_id":    binding.AccountID.String(),
		"account_epoch": binding.AccountEpoch,
		"operation":     "claim",
		"worker_id":     workerID,
		"lease_seconds": CatalogLeaseSeconds,
	}
	var out *struct {
		JobID          uuid.UUID `json:"job_id"`
		Generation     int       `json:"generation"`
		LeaseToken     uuid.UUID `json:"lease_token"`
		LeaseExpiresAt string    `json:"lease_expires_at"`
	}
	if err := c.Call(ctx, PathCatalogRefreshJob, "catalog_refresh.mutate", c.accountScope(binding), body, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	leaseExpiry, err := time.Parse(time.RFC3339, out.LeaseExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("athconnector: catalog lease expiry is invalid: %w", err)
	}
	return &CatalogJob{JobID: out.JobID, Generation: out.Generation, LeaseToken: out.LeaseToken, LeaseExpiresAt: leaseExpiry}, nil
}

// RenewCatalogJob extends the lease; the job identity stays unchanged.
func (c *ControlClient) RenewCatalogJob(ctx context.Context, binding AccountBinding, job CatalogJob) (time.Time, error) {
	body := map[string]any{
		"account_id":    binding.AccountID.String(),
		"account_epoch": binding.AccountEpoch,
		"operation":     "renew",
		"job_id":        job.JobID.String(),
		"lease_token":   job.LeaseToken.String(),
		"lease_seconds": CatalogLeaseSeconds,
	}
	var out struct {
		LeaseExpiresAt string `json:"lease_expires_at"`
	}
	if err := c.Call(ctx, PathCatalogRefreshJob, "catalog_refresh.mutate", c.accountScope(binding), body, &out); err != nil {
		return time.Time{}, err
	}
	expiry, err := time.Parse(time.RFC3339, out.LeaseExpiresAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("athconnector: catalog lease expiry is invalid: %w", err)
	}
	return expiry, nil
}

// FailCatalogJob reports a failed run. A partial or provider-side failure must
// never be published as an empty success.
func (c *ControlClient) FailCatalogJob(ctx context.Context, binding AccountBinding, job CatalogJob, failureCode string) error {
	if err := validateCatalogFailureCode(failureCode); err != nil {
		return err
	}
	body := map[string]any{
		"account_id":    binding.AccountID.String(),
		"account_epoch": binding.AccountEpoch,
		"operation":     "fail",
		"job_id":        job.JobID.String(),
		"lease_token":   job.LeaseToken.String(),
		"failure_code":  failureCode,
	}
	return c.Call(ctx, PathCatalogRefreshJob, "catalog_refresh.mutate", c.accountScope(binding), body, nil)
}

func validateCatalogFailureCode(code string) error {
	if len(code) < 1 || len(code) > 100 {
		return errors.New("athconnector: catalog failure code must be 1-100 characters")
	}
	for _, r := range code {
		if !strings.ContainsRune(catalogFailureCodeAlphabet, r) {
			return fmt.Errorf("athconnector: catalog failure code %q contains unsupported characters", code)
		}
	}
	return nil
}

// UploadCatalogPage publishes one bounded page. The entries bytes are the
// canonical jsonb array text: the gateway digests them verbatim.
func (c *ControlClient) UploadCatalogPage(ctx context.Context, binding AccountBinding, job CatalogJob, pageIndex int, entries []CatalogEntry) (replayed bool, entryCount int, err error) {
	if pageIndex < 0 || pageIndex >= CatalogMaxPages {
		return false, 0, errors.New("athconnector: catalog page index out of range")
	}
	if len(entries) == 0 || len(entries) > CatalogMaxEntriesPerPage {
		return false, 0, errors.New("athconnector: catalog page must carry 1-500 entries")
	}
	canonical, err := CanonicalEntriesJSONB(entries)
	if err != nil {
		return false, 0, err
	}
	digest := sha256.Sum256(canonical)
	body := map[string]any{
		"account_id":    binding.AccountID.String(),
		"account_epoch": binding.AccountEpoch,
		"operation":     "upload_page",
		"job_id":        job.JobID.String(),
		"lease_token":   job.LeaseToken.String(),
		"page_index":    pageIndex,
		"entries":       json.RawMessage(canonical),
		"page_digest":   EncodeDigest(digest[:]),
	}
	var out struct {
		PageIndex  int  `json:"page_index"`
		EntryCount int  `json:"entry_count"`
		Replayed   bool `json:"replayed"`
	}
	if err := c.Call(ctx, PathGroupCatalog, "group_catalog.mutate", c.accountScope(binding), body, &out); err != nil {
		return false, 0, err
	}
	return out.Replayed, out.EntryCount, nil
}

// FinalizeCatalog commits the complete snapshot after every page is accepted.
func (c *ControlClient) FinalizeCatalog(ctx context.Context, binding AccountBinding, job CatalogJob, entries []CatalogEntry) (*CatalogFinalizeResult, error) {
	pageCount := (len(entries) + CatalogMaxEntriesPerPage - 1) / CatalogMaxEntriesPerPage
	if pageCount < 1 || pageCount > CatalogMaxPages {
		return nil, fmt.Errorf("athconnector: catalog page count %d out of range", pageCount)
	}
	rawDigest, err := Digest(entries)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"account_id":          binding.AccountID.String(),
		"account_epoch":       binding.AccountEpoch,
		"operation":           "finalize",
		"job_id":              job.JobID.String(),
		"lease_token":         job.LeaseToken.String(),
		"expected_page_count": pageCount,
		"catalog_digest":      EncodeDigest(rawDigest),
	}
	var out struct {
		SnapshotID uuid.UUID `json:"snapshot_id"`
		Generation int       `json:"generation"`
		EntryCount int       `json:"entry_count"`
	}
	if err := c.Call(ctx, PathGroupCatalog, "group_catalog.mutate", c.accountScope(binding), body, &out); err != nil {
		return nil, err
	}
	return &CatalogFinalizeResult{SnapshotID: out.SnapshotID, Generation: out.Generation, EntryCount: out.EntryCount}, nil
}

// GroupCatalogWorker polls refresh jobs for one account binding and publishes
// bounded complete-generation snapshots from the isolated connector service.
type GroupCatalogWorker struct {
	client     *ControlClient
	binding    AccountBinding
	source     GroupCatalogSource
	instanceID uuid.UUID
	provider   string
	workerID   string
	poll       time.Duration
	now        func() time.Time

	stop     chan struct{}
	stopOnce sync.Once
}

// NewGroupCatalogWorker validates the wiring; poll is the idle interval
// between claim attempts.
func NewGroupCatalogWorker(client *ControlClient, binding AccountBinding, source GroupCatalogSource, instanceID uuid.UUID, provider, workerID string, poll time.Duration) (*GroupCatalogWorker, error) {
	if client == nil || source == nil {
		return nil, errors.New("athconnector: catalog worker requires a control client and a group source")
	}
	if err := binding.validate(); err != nil {
		return nil, err
	}
	if instanceID == uuid.Nil || provider == "" || workerID == "" {
		return nil, errors.New("athconnector: catalog worker requires instance, provider and worker identity")
	}
	if poll <= 0 {
		poll = 15 * time.Second
	}
	return &GroupCatalogWorker{
		client: client, binding: binding, source: source,
		instanceID: instanceID, provider: provider, workerID: workerID,
		poll: poll, now: time.Now, stop: make(chan struct{}),
	}, nil
}

// Start runs the poll loop until Stop.
func (w *GroupCatalogWorker) Start() {
	go func() {
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			if err := w.RunOnce(context.Background()); err != nil {
				slog.Warn("athconnector catalog run failed; backing off", "error", err)
			}
			select {
			case <-w.stop:
				return
			case <-time.After(w.poll):
			}
		}
	}()
}

// Stop ends the poll loop. In-flight runs finish; leases expire server-side.
func (w *GroupCatalogWorker) Stop() { w.stopOnce.Do(func() { close(w.stop) }) }

// RunOnce claims one job and either publishes the full snapshot or fails the
// job with a bounded code. Idle claims return nil.
func (w *GroupCatalogWorker) RunOnce(ctx context.Context) error {
	job, err := w.client.ClaimCatalogJob(ctx, w.binding, w.workerID)
	if err != nil || job == nil {
		return err
	}
	groups, err := w.source.FetchJoinedGroups(ctx)
	if err != nil {
		w.failQuietly(ctx, *job, "provider_fetch_failed", err)
		return fmt.Errorf("athconnector: catalog source failed: %w", err)
	}
	entries, err := w.entriesFor(groups)
	if err != nil {
		w.failQuietly(ctx, *job, "entries_invalid", err)
		return err
	}
	if err := w.publish(ctx, *job, entries); err != nil {
		return err
	}
	slog.Info("athconnector catalog snapshot published",
		"job_id", job.JobID, "generation", job.Generation, "entries", len(entries))
	return nil
}

func (w *GroupCatalogWorker) entriesFor(groups []CatalogGroup) ([]CatalogEntry, error) {
	if len(groups) > CatalogMaxTotalEntries {
		return nil, fmt.Errorf("athconnector: joined groups %d exceed the catalog limit", len(groups))
	}
	observed := w.now().UTC()
	entries := make([]CatalogEntry, 0, len(groups))
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		conversationID := strings.TrimSpace(group.ConversationID)
		if conversationID == "" {
			return nil, errors.New("athconnector: catalog group is missing its conversation id")
		}
		if _, duplicate := seen[conversationID]; duplicate {
			return nil, fmt.Errorf("athconnector: duplicate conversation id %q in joined groups", conversationID)
		}
		seen[conversationID] = struct{}{}
		display := strings.TrimSpace(group.DisplayName)
		if display == "" {
			display = conversationID
		}
		if len(display) > CatalogDisplayNameLimit {
			display = display[:CatalogDisplayNameLimit]
		}
		entries = append(entries, CatalogEntry{
			ConversationID: conversationID,
			DisplayName:    display,
			Metadata: CatalogEntryMetadata{
				ChannelInstanceID:   w.instanceID,
				Provider:            w.provider,
				ProviderAccountHint: w.binding.ProviderAccountID,
				ObservedAt:          observed,
			},
			IsAvailable: group.Available,
		})
	}
	return entries, nil
}

// publish uploads every bounded page then finalizes the snapshot. The lease is
// renewed before each page when less than half of it remains, so a slow
// provider or large catalog cannot invalidate mid-run uploads. Any rejection
// fails the job instead of fabricating success.
func (w *GroupCatalogWorker) publish(ctx context.Context, job CatalogJob, entries []CatalogEntry) error {
	for offset := 0; offset < len(entries); offset += CatalogMaxEntriesPerPage {
		pageIndex := offset / CatalogMaxEntriesPerPage
		if w.now().After(job.LeaseExpiresAt.Add(-CatalogLeaseSeconds * time.Second / 2)) {
			expiry, err := w.client.RenewCatalogJob(ctx, w.binding, job)
			if err != nil {
				w.failQuietly(ctx, job, "lease_renew_failed", err)
				return fmt.Errorf("athconnector: catalog lease renew failed: %w", err)
			}
			job.LeaseExpiresAt = expiry
		}
		end := offset + CatalogMaxEntriesPerPage
		if end > len(entries) {
			end = len(entries)
		}
		if _, _, err := w.client.UploadCatalogPage(ctx, w.binding, job, pageIndex, entries[offset:end]); err != nil {
			w.failQuietly(ctx, job, "upload_rejected", err)
			return fmt.Errorf("athconnector: catalog page upload failed: %w", err)
		}
	}
	if _, err := w.client.FinalizeCatalog(ctx, w.binding, job, entries); err != nil {
		w.failQuietly(ctx, job, "finalize_rejected", err)
		return fmt.Errorf("athconnector: catalog finalize failed: %w", err)
	}
	return nil
}

// failQuietly reports the failure code; the report itself must never mask the
// original error path.
func (w *GroupCatalogWorker) failQuietly(ctx context.Context, job CatalogJob, code string, cause error) {
	if err := w.client.FailCatalogJob(ctx, w.binding, job, code); err != nil {
		slog.Warn("athconnector catalog fail-report rejected", "job_id", job.JobID, "code", code, "error", err, "cause", cause)
	}
}
