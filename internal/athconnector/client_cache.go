package athconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// minTokenRemaining bounds rotation: a fresh issuance is only attempted when
// the current token has less than this left, so an active scope holds at most
// two unexpired credentials (current + rotation overlap).
const minTokenRemaining = 30 * time.Second

// Credential is one issued data credential. The plaintext secret lives only
// in process memory inside the cache; it is never logged, exported, or
// attached to any model-visible output.
type Credential struct {
	CredentialID uuid.UUID
	Secret       string
	ExpiresAt    time.Time
}

func (c Credential) expired(now time.Time) bool {
	return !now.Add(minTokenRemaining).Before(c.ExpiresAt)
}

type cacheEntry struct {
	credential Credential
	ready      chan struct{}
	err        error
}

// ScopeClientCache issues and caches data credentials per scope key with
// single-flight: concurrent runs under one scope share one issuance, a lost
// issuance response is never replayed as success (it rotates under the cap),
// and a scope key change drops its entries entirely.
type ScopeClientCache struct {
	client *ControlClient
	now    func() time.Time

	mu      sync.Mutex
	entries map[string]*cacheEntry
}

// NewScopeClientCache binds the cache to one control client.
func NewScopeClientCache(client *ControlClient) (*ScopeClientCache, error) {
	if client == nil {
		return nil, errors.New("athconnector: scope cache requires a control client")
	}
	return &ScopeClientCache{client: client, now: time.Now, entries: map[string]*cacheEntry{}}, nil
}

// Credential returns a live credential for the scope, issuing or rotating as
// needed. The first caller performs the issuance; concurrent callers wait and
// share the result. An issuance failure fails every waiter without caching.
func (s *ScopeClientCache) Credential(ctx context.Context, scopeKey string, binding AccountBinding, origin *Origin, scope *RunScope) (Credential, error) {
	if scope == nil || scope.Status != ScopeApproved || scope.ContextID == uuid.Nil {
		return Credential{}, errors.New("athconnector: data credentials exist only for approved scopes")
	}
	s.mu.Lock()
	entry, ok := s.entries[scopeKey]
	if ok && entry.ready != nil {
		// Another issuance is in flight: wait for it outside the lock.
		s.mu.Unlock()
		select {
		case <-entry.ready:
		case <-ctx.Done():
			return Credential{}, ctx.Err()
		}
		s.mu.Lock()
	}
	if entry != nil && entry.err == nil && !entry.credential.expired(s.now()) {
		cred := entry.credential
		s.mu.Unlock()
		return cred, nil
	}
	// Issue (or rotate) with single-flight.
	fresh := &cacheEntry{ready: make(chan struct{})}
	s.entries[scopeKey] = fresh
	s.mu.Unlock()

	cred, err := s.issue(ctx, binding, origin, scope)
	fresh.credential, fresh.err = cred, err
	close(fresh.ready)

	s.mu.Lock()
	delete(s.entries, scopeKey)
	if err == nil {
		s.entries[scopeKey] = &cacheEntry{credential: cred}
	}
	s.mu.Unlock()
	if err != nil {
		return Credential{}, err
	}
	return cred, nil
}

func (s *ScopeClientCache) issue(ctx context.Context, binding AccountBinding, origin *Origin, scope *RunScope) (Credential, error) {
	body := conversationScopeBody(binding, origin)
	body["context_id"] = scope.ContextID.String()
	issuanceID := uuid.New()
	body["issuance_id"] = issuanceID.String()
	assertion := AssertionScope{
		ConnectorID:    s.client.Config().ConnectorID,
		AccountID:      binding.AccountID,
		AccountEpoch:   binding.AccountEpoch,
		PeerKind:       origin.ConversationKind(),
		ConversationID: origin.ConversationID(),
	}
	var out struct {
		CredentialID        uuid.UUID `json:"credential_id"`
		PlaintextCredential string    `json:"plaintext_credential"`
		ExpiresAt           string    `json:"expires_at"`
	}
	if err := s.client.Call(ctx, PathToken, "credential.issue", assertion, body, &out); err != nil {
		return Credential{}, fmt.Errorf("athconnector: credential issuance failed: %w", err)
	}
	if out.CredentialID == uuid.Nil || !strings.HasPrefix(out.PlaintextCredential, "agw1.") {
		return Credential{}, errors.New("athconnector: credential issuance returned an unusable credential")
	}
	expiresAt, err := time.Parse(time.RFC3339, out.ExpiresAt)
	if err != nil {
		return Credential{}, fmt.Errorf("athconnector: credential expiry is invalid: %w", err)
	}
	return Credential{CredentialID: out.CredentialID, Secret: out.PlaintextCredential, ExpiresAt: expiresAt}, nil
}

// Drop discards the cached credential for a scope key (revocation, remap, or
// generation change). The next caller issues fresh.
func (s *ScopeClientCache) Drop(scopeKey string) {
	s.mu.Lock()
	delete(s.entries, scopeKey)
	s.mu.Unlock()
}
