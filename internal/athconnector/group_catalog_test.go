package athconnector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeCatalogSource struct {
	groups []CatalogGroup
	err    error
}

func (f *fakeCatalogSource) FetchJoinedGroups(context.Context) ([]CatalogGroup, error) {
	return f.groups, f.err
}

type catalogGatewayFake struct {
	mu sync.Mutex
	// claim responses
	claimJob     map[string]any // first claim; subsequent claims return null
	claimedTwice bool
	// recorded operations in order
	operations []map[string]any
	rawBodies  [][]byte
	// failure injection
	failUpload bool
	now        time.Time
}

func (f *catalogGatewayFake) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		raw := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(raw)
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("body decode: %v", err)
		}
		var rawFields struct {
			Entries json.RawMessage `json:"entries"`
		}
		_ = json.Unmarshal(raw, &rawFields)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.rawBodies = append(f.rawBodies, raw)
		path := r.URL.Path
		data := map[string]any{"request_id": "req-cat"}
		switch path {
		case PathCatalogRefreshJob:
			f.operations = append(f.operations, body)
			switch body["operation"] {
			case "claim":
				if f.claimedTwice || f.claimJob == nil {
					data["data"] = nil
				} else {
					f.claimedTwice = true
					job := map[string]any{}
					for k, v := range f.claimJob {
						job[k] = v
					}
					job["job_id"] = uuid.New().String()
					data["data"] = job
				}
			case "renew":
				data["data"] = map[string]any{
					"job_id":           body["job_id"],
					"lease_expires_at": f.now.Add(CatalogLeaseSeconds * time.Second).Format(time.RFC3339),
				}
			case "fail":
				data["data"] = map[string]any{"job_id": body["job_id"], "status": "failed"}
			}
		case PathGroupCatalog:
			f.operations = append(f.operations, body)
			if f.failUpload && body["operation"] == "upload_page" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"request_id":"req-cat","error":{"code":"lease_stale","message":"stale"}}`))
				return
			}
			if body["operation"] == "upload_page" {
				// Server-side digest check mirrors the gateway: sha256 over the
				// jsonb text of the parsed entries (sorted keys, jsonb
				// separators), not the compacted wire bytes.
				var wireEntries []struct {
					ConversationID string `json:"conversation_id"`
					DisplayName    string `json:"display_name"`
					IsAvailable    bool   `json:"is_available"`
					Metadata       struct {
						ChannelInstanceID   uuid.UUID `json:"channel_instance_id"`
						ObservedAt          time.Time `json:"observed_at"`
						Provider            string    `json:"provider"`
						ProviderAccountHint string    `json:"provider_account_hint"`
					} `json:"metadata"`
				}
				if err := json.Unmarshal(rawFields.Entries, &wireEntries); err != nil {
					t.Errorf("entries decode: %v", err)
				}
				roundTrip := make([]CatalogEntry, 0, len(wireEntries))
				for _, item := range wireEntries {
					roundTrip = append(roundTrip, CatalogEntry{
						ConversationID: item.ConversationID,
						DisplayName:    item.DisplayName,
						IsAvailable:    item.IsAvailable,
						Metadata: CatalogEntryMetadata{
							ChannelInstanceID:   item.Metadata.ChannelInstanceID,
							Provider:            item.Metadata.Provider,
							ProviderAccountHint: item.Metadata.ProviderAccountHint,
							ObservedAt:          item.Metadata.ObservedAt,
						},
					})
				}
				canonical, err := CanonicalEntriesJSONB(roundTrip)
				if err != nil {
					t.Errorf("canonical render: %v", err)
				}
				sum := sha256.Sum256(canonical)
				if base64.RawURLEncoding.EncodeToString(sum[:]) != body["page_digest"] {
					t.Errorf("page digest mismatch for page %v", body["page_index"])
				}
				data["data"] = map[string]any{
					"job_id": body["job_id"], "page_index": body["page_index"],
					"entry_count": float64(len(body["entries"].([]any))), "replayed": false,
				}
			} else {
				data["data"] = map[string]any{
					"job_id": body["job_id"], "snapshot_id": uuid.New().String(),
					"generation": 4.0, "entry_count": 1.0,
				}
			}
		default:
			t.Errorf("unexpected path %s", path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	}
}

func TestCatalogEntryCanonicalJSONBMatchesPostgresText(t *testing.T) {
	observed := time.Date(2026, 9, 16, 1, 2, 3, 0, time.UTC)
	instance := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	entry := CatalogEntry{
		ConversationID: "group-101",
		DisplayName:    "Nhóm A — tòa B",
		Metadata: CatalogEntryMetadata{
			ChannelInstanceID:   instance,
			Provider:            "zalo_personal",
			ProviderAccountHint: "account-1",
			ObservedAt:          observed,
		},
		IsAvailable: true,
	}
	canonical, err := entry.CanonicalJSONB()
	if err != nil {
		t.Fatal(err)
	}
	// Pinned byte-for-byte to Postgres jsonb::text: keys length-first then
	// bytewise, jsonb separators (", " and ": "), raw UTF-8, RFC3339 UTC.
	want := `{"metadata": {"provider": "zalo_personal", "observed_at": "2026-09-16T01:02:03Z", "channel_instance_id": "` +
		instance.String() + `", "provider_account_hint": "account-1"}, "display_name": "Nhóm A — tòa B", "is_available": true, "conversation_id": "group-101"}`
	if string(canonical) != want {
		t.Fatalf("canonical jsonb text mismatch:\n got: %s\nwant: %s", canonical, want)
	}
	// Without the optional hint the metadata key set shrinks but the order holds.
	entry.Metadata.ProviderAccountHint = ""
	canonical, err = entry.CanonicalJSONB()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(canonical), `"channel_instance_id": "`+instance.String()+`"}, "display_name"`) {
		t.Fatalf("hintless metadata tail wrong: %s", canonical)
	}
	// Literal escape-shaped text and raw U+2028/U+2029 must survive the
	// escaper as valid JSON with PG-identical bytes.
	if literal, err := jsonbString("back\\u2028text"); err != nil || literal != "\"back\\\\u2028text\"" {
		t.Fatalf("literal escape-shaped text corrupted: %q err=%v", literal, err)
	}
	if raw, err := jsonbString("a b c"); err != nil || raw != "\"a b c\"" {
		t.Fatalf("raw line separators must stay raw: %q err=%v", raw, err)
	}
	entries, err := CanonicalEntriesJSONB([]CatalogEntry{entry, entry})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(entries), `}, {`) {
		t.Fatalf("array separator must be jsonb style: %s", entries)
	}
	if string(entries) != "["+string(canonical)+", "+string(canonical)+"]" {
		t.Fatalf("array render wrong: %s", entries)
	}
}

func newCatalogWorker(t *testing.T, fake *catalogGatewayFake, source GroupCatalogSource) *GroupCatalogWorker {
	t.Helper()
	signerPub, signerKey, err := ed25519.GenerateKey(rand.Reader)
	_ = signerPub
	if err != nil {
		t.Fatal(err)
	}
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	client, err := NewControlClient(connectorTestConfig("http://127.0.0.1:1"), signer)
	if err != nil {
		t.Fatal(err)
	}
	// Point the client at the fake gateway.
	worker, err := NewGroupCatalogWorker(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 2}, source, uuid.New(), "zalo_personal", "worker-test", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestCatalogWorkerPublishesCompleteSnapshot(t *testing.T) {
	instanceID := uuid.New()
	fake := &catalogGatewayFake{now: time.Now()}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	// Build the worker through a client bound to the fake server.
	_, signerKey, _ := ed25519.GenerateKey(rand.Reader)
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	cfg := connectorTestConfig(server.URL)
	client, err := NewControlClient(cfg, signer)
	if err != nil {
		t.Fatal(err)
	}
	source := &fakeCatalogSource{groups: []CatalogGroup{
		{ConversationID: "group-1", DisplayName: "Nhóm trùng tên", Available: true},
		{ConversationID: "group-2", DisplayName: "Nhóm trùng tên", Available: true},
		{ConversationID: "group-3", DisplayName: "", Available: false},
	}}
	worker, err := NewGroupCatalogWorker(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 2}, source, instanceID, "zalo_personal", "worker-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	leaseToken := uuid.New()
	fake.claimJob = map[string]any{
		"account_id":       "x",
		"account_epoch":    2.0,
		"generation":       4.0,
		"lease_token":      leaseToken.String(),
		"lease_expires_at": time.Now().Add(CatalogLeaseSeconds * time.Second).Format(time.RFC3339),
	}

	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, op := range fake.operations {
		ops = append(ops, fmt.Sprint(op["operation"]))
	}
	joined := strings.Join(ops, ",")
	if joined != "claim,upload_page,finalize" {
		t.Fatalf("unexpected operation sequence: %s", joined)
	}
	// Duplicate display names survive; empty display names fall back to the
	// conversation id; availability is preserved.
	upload := fake.operations[1]
	entries := upload["entries"].([]any)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	third := entries[2].(map[string]any)
	if third["display_name"] != "group-3" || third["is_available"] != false {
		t.Fatalf("fallback/availability wrong: %#v", third)
	}
	metadata := entries[0].(map[string]any)["metadata"].(map[string]any)
	if metadata["channel_instance_id"] != instanceID.String() || metadata["provider"] != "zalo_personal" {
		t.Fatalf("metadata not account-scoped: %#v", metadata)
	}
	// Idle second run: claim returns null and nothing else happens.
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.operations) != 4 {
		t.Fatalf("idle claim must only add the claim attempt, got %d operations", len(fake.operations))
	}
}

func TestCatalogWorkerFailsJobOnProviderOutage(t *testing.T) {
	fake := &catalogGatewayFake{now: time.Now()}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	_, signerKey, _ := ed25519.GenerateKey(rand.Reader)
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	source := &fakeCatalogSource{err: context.DeadlineExceeded}
	worker, err := NewGroupCatalogWorker(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}, source, uuid.New(), "zalo_personal", "worker-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fake.claimJob = map[string]any{
		"generation":       3.0,
		"lease_token":      uuid.New().String(),
		"lease_expires_at": time.Now().Add(CatalogLeaseSeconds * time.Second).Format(time.RFC3339),
	}
	if err := worker.RunOnce(context.Background()); err == nil {
		t.Fatal("provider outage must surface an error")
	}
	if len(fake.operations) != 2 || fake.operations[1]["operation"] != "fail" || fake.operations[1]["failure_code"] != "provider_fetch_failed" {
		t.Fatalf("provider failure must fail the job, got %+v", fake.operations)
	}
}

func TestCatalogWorkerFailsJobWhenUploadRejected(t *testing.T) {
	fake := &catalogGatewayFake{now: time.Now(), failUpload: true}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	_, signerKey, _ := ed25519.GenerateKey(rand.Reader)
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	source := &fakeCatalogSource{groups: []CatalogGroup{{ConversationID: "group-1", DisplayName: "A", Available: true}}}
	worker, err := NewGroupCatalogWorker(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}, source, uuid.New(), "zalo_personal", "worker-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fake.claimJob = map[string]any{
		"generation":       3.0,
		"lease_token":      uuid.New().String(),
		"lease_expires_at": time.Now().Add(CatalogLeaseSeconds * time.Second).Format(time.RFC3339),
	}
	if err := worker.RunOnce(context.Background()); err == nil {
		t.Fatal("upload rejection must surface an error")
	}
	var last map[string]any
	for _, op := range fake.operations {
		last = op
	}
	if last["operation"] != "fail" || last["failure_code"] != "upload_rejected" {
		t.Fatalf("rejection must fail the job, got %+v", last)
	}
}

func TestCatalogWorkerRenewsExpiringLease(t *testing.T) {
	fake := &catalogGatewayFake{now: time.Now()}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	_, signerKey, _ := ed25519.GenerateKey(rand.Reader)
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	source := &fakeCatalogSource{groups: []CatalogGroup{{ConversationID: "group-1", DisplayName: "A", Available: true}}}
	worker, err := NewGroupCatalogWorker(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}, source, uuid.New(), "zalo_personal", "worker-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Lease already past its half-life: publish must renew before uploading.
	fake.claimJob = map[string]any{
		"generation":       2.0,
		"lease_token":      uuid.New().String(),
		"lease_expires_at": time.Now().Add(CatalogLeaseSeconds * time.Second / 4).Format(time.RFC3339),
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, op := range fake.operations {
		ops = append(ops, fmt.Sprint(op["operation"]))
	}
	if strings.Join(ops, ",") != "claim,renew,upload_page,finalize" {
		t.Fatalf("lease renewal missing: %s", strings.Join(ops, ","))
	}
}

func TestCatalogWorkerStopEndsPollLoop(t *testing.T) {
	worker := newCatalogWorker(t, &catalogGatewayFake{}, &fakeCatalogSource{})
	worker.Start()
	worker.Stop()
	// Second Stop must not panic (sync.Once).
	worker.Stop()
}
