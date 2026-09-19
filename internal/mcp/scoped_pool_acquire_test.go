package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// scopeHeaderServer is a minimal streamable-HTTP MCP server recording the
// Authorization header of every request it serves. It answers initialize and
// tools/list with an empty tool set — enough for the pool to build a client —
// and records whether two pool keys ever shared a connection by capturing the
// distinct bearer tokens seen per connection (session id).
type scopeHeaderServer struct {
	mu        sync.Mutex
	tokens    []string
	sessionID string
}

func (s *scopeHeaderServer) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		s.mu.Lock()
		s.tokens = append(s.tokens, auth)
		session := s.sessionID
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		response := map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": map[string]any{
				"sessionId":       session,
				"serverInfo":      map[string]any{"name": "scoped-test", "version": "1.0"},
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{},
			},
		}
		if strings.Contains(r.URL.Path, "tools/list") || strings.HasSuffix(r.URL.Path, "list") {
			response["result"] = map[string]any{"tools": []any{}}
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

// TestScopedPoolAcquireUsesDistinctCredentialKeys proves the connector bridge
// pattern: two different credential identities acquire under distinct pool
// keys, so a rotation never reuses the previous immutable client, and each
// connection carries exactly its own bearer token.
func TestScopedPoolAcquireUsesDistinctCredentialKeys(t *testing.T) {
	server := &scopeHeaderServer{sessionID: uuid.New().String()}
	httpSrv := httptest.NewServer(server.handler(t))
	defer httpSrv.Close()

	pool := NewPool(PoolConfig{MaxSize: 5, MaxIdle: 5, IdleTTL: 5 * time.Second})
	t.Cleanup(pool.Stop)

	tenant := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	first, err := pool.AcquireUser(context.Background(), tenant, "ath-connector",
		"athscope:"+strings.Repeat("1", 32), "streamable-http", "", nil, nil,
		httpSrv.URL, map[string]string{"Authorization": "Bearer agw1.first"}, 5)
	if err != nil {
		t.Fatalf("first scoped acquire failed: %v", err)
	}
	pool.ReleaseUser(UserPoolKey(tenant, "ath-connector", "athscope:"+strings.Repeat("1", 32)))

	second, err := pool.AcquireUser(context.Background(), tenant, "ath-connector",
		"athscope:"+strings.Repeat("2", 32), "streamable-http", "", nil, nil,
		httpSrv.URL, map[string]string{"Authorization": "Bearer agw1.second"}, 5)
	if err != nil {
		t.Fatalf("rotated scoped acquire failed: %v", err)
	}
	pool.ReleaseUser(UserPoolKey(tenant, "ath-connector", "athscope:"+strings.Repeat("2", 32)))

	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.tokens) == 0 {
		t.Fatal("scoped connections must reach the MCP server")
	}
	sawFirst, sawSecond := false, false
	for _, token := range server.tokens {
		if strings.Contains(token, "agw1.first") {
			sawFirst = true
		}
		if strings.Contains(token, "agw1.second") {
			sawSecond = true
		}
	}
	if !sawFirst || !sawSecond {
		t.Fatalf("rotation must send its own token, saw first=%v second=%v tokens=%v", sawFirst, sawSecond, server.tokens)
	}
	// The pool must keep both keys independent: the first entry stays pooled
	// under its own key and the second never mutated it.
	if first == second {
		t.Fatal("distinct credential identities must never share a pool entry")
	}
}

// TestScopedPoolAcquireReusesSameCredentialConnection proves same-credential
// single-flight reuse: acquiring the same scope key twice returns the same
// pooled client without a second handshake, so concurrent runs under one scope
// share one immutable connection.
func TestScopedPoolAcquireReusesSameCredentialConnection(t *testing.T) {
	server := &scopeHeaderServer{sessionID: uuid.New().String()}
	httpSrv := httptest.NewServer(server.handler(t))
	defer httpSrv.Close()

	pool := NewPool(PoolConfig{MaxSize: 5, MaxIdle: 5, IdleTTL: 5 * time.Second})
	t.Cleanup(pool.Stop)

	tenant := uuid.MustParse("00000000-0000-0000-0000-0000000000bb")
	scopeKey := "athscope:" + strings.Repeat("3", 32)
	headers := map[string]string{"Authorization": "Bearer agw1.shared"}

	first, err := pool.AcquireUser(context.Background(), tenant, "ath-connector",
		scopeKey, "streamable-http", "", nil, nil, httpSrv.URL, headers, 5)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	again, err := pool.AcquireUser(context.Background(), tenant, "ath-connector",
		scopeKey, "streamable-http", "", nil, nil, httpSrv.URL, headers, 5)
	if err != nil {
		t.Fatalf("same-scope reacquire failed: %v", err)
	}
	if first != again {
		t.Fatal("same credential identity must reuse the pooled connection")
	}
	pool.ReleaseUser(UserPoolKey(tenant, "ath-connector", scopeKey))
	pool.ReleaseUser(UserPoolKey(tenant, "ath-connector", scopeKey))
}
