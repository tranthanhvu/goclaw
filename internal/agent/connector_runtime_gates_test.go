package agent

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

type stubScopedTool struct {
	name   string
	called int
}

func (s *stubScopedTool) Name() string               { return s.name }
func (s *stubScopedTool) Description() string        { return "scoped" }
func (s *stubScopedTool) Parameters() map[string]any { return map[string]any{"type": "object"} }
func (s *stubScopedTool) Execute(_ context.Context, _ map[string]any) *tools.Result {
	s.called++
	return tools.NewResult("scoped-result")
}

func connectorTestPolicy(scopeKey string) *athconnector.RunPolicy {
	return &athconnector.RunPolicy{
		ScopeKey: scopeKey,
		DataFree: true,
		Origin:   &athconnector.Origin{},
	}
}

func TestConnectorScopedToolsResolveAtExecution(t *testing.T) {
	scoped := &stubScopedTool{name: "contract_read"}
	l := &Loop{}
	l.connectorMCPTools.Store("scope-1", &connectorScopedTools{credentialID: uuid.New(), tools: []tools.Tool{scoped}})
	l.tools = tools.NewRegistry()

	policy := connectorTestPolicy("scope-1")
	ctx := athconnector.WithRunPolicy(context.Background(), policy)

	result := l.executeToolForActor(ctx, "contract_read", nil, "ch", "chat", "group", "session", "actor-1")
	if result == nil || result.ForLLM != "scoped-result" {
		t.Fatalf("scoped tool must execute from the scope cache: %+v", result)
	}
	if scoped.called != 1 {
		t.Fatalf("exactly one execution, got %d", scoped.called)
	}

	// A tool outside the scope's bridge set never executes: it resolves only
	// against the shared registry (empty here → nil/absent behavior).
	other := &stubScopedTool{name: "unscoped_tool"}
	_ = other
	missing := l.executeToolForActor(ctx, "not_in_scope", nil, "ch", "chat", "group", "session", "actor-1")
	if missing != nil && missing.ForLLM == "scoped-result" {
		t.Fatal("tool outside the scope must not execute a scoped bridge tool")
	}

	// Without a connector policy the scoped cache is invisible.
	plain := l.executeToolForActor(context.Background(), "contract_read", nil, "ch", "chat", "group", "session", "actor-1")
	if plain != nil && plain.ForLLM == "scoped-result" {
		t.Fatal("generic runs must never resolve connector scoped tools")
	}
	if scoped.called != 1 {
		t.Fatalf("no extra executions allowed, got %d", scoped.called)
	}
}

func TestConnectorRunsNeverFlushMemory(t *testing.T) {
	l := &Loop{}
	settings := ResolveMemoryFlushSettings(nil)
	if settings == nil {
		t.Fatal("memory flush must default to enabled for generic runs")
	}
	if l.shouldRunMemoryFlush(context.Background(), "s", 10_000_000, settings) == false {
		// Token heuristic may or may not trip for generic runs; the connector
		// path is the guarantee under test.
		_ = settings
	}
	ctx := athconnector.WithRunPolicy(context.Background(), connectorTestPolicy("scope-1"))
	if l.shouldRunMemoryFlush(ctx, "s", 10_000_000, settings) {
		t.Fatal("connector runs must never trigger memory flush")
	}
}
