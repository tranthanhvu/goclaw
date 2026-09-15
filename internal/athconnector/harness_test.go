package athconnector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

type fakeAccountRegistry struct {
	records map[string]AccountRecord
	err     error
}

func (f *fakeAccountRegistry) EnsureAccount(_ context.Context, _, _ uuid.UUID, provider, providerAccountID string) (AccountRecord, error) {
	if f.err != nil {
		return AccountRecord{}, f.err
	}
	if f.records == nil {
		f.records = map[string]AccountRecord{}
	}
	if existing, ok := f.records[provider]; ok && existing.ProviderAccountID == providerAccountID {
		return existing, nil
	}
	next := AccountRecord{ID: uuid.New(), ProviderAccountID: providerAccountID, AccountEpoch: 1}
	if existing, ok := f.records[provider]; ok {
		next.ID = existing.ID
		next.AccountEpoch = existing.AccountEpoch + 1
	}
	f.records[provider] = next
	return next, nil
}

func writeTestKey(t *testing.T) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "connector.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func harnessTestConfig(keyFile string) Config {
	return Config{
		Enabled: true, RuntimeRole: RuntimeRoleConnector, GatewayURL: "https://ath.example.test",
		ConnectorID: uuid.New(), Environment: "production", Issuer: "goclaw-ath",
		KeyID: "key-1", PrivateKeyFile: keyFile,
	}
}

func TestHarnessArmsGroupIntakeFromVerifiedLogin(t *testing.T) {
	registry := &fakeAccountRegistry{}
	harness, err := NewHarness(harnessTestConfig(writeTestKey(t)), PurposeTenantContract, ApprovalHints{}, registry)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, instanceID := uuid.New(), uuid.New()

	intake, err := harness.ArmGroupIntake(context.Background(), tenantID, instanceID, "zalo_personal", "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if !intake.Enabled() || intake.ProviderAccountID() != "account-1" || intake.AccountEpoch() != 1 {
		t.Fatalf("intake not armed from verified login: %+v", intake)
	}

	// Re-login under another account bumps the epoch; the new intake binding
	// reflects it, so origins minted for the old account fail submission.
	rearmed, err := harness.ArmGroupIntake(context.Background(), tenantID, instanceID, "zalo_personal", "account-2")
	if err != nil {
		t.Fatal(err)
	}
	if rearmed.ProviderAccountID() != "account-2" || rearmed.AccountEpoch() != 2 {
		t.Fatalf("re-login must rebind at the bumped epoch: %+v", rearmed)
	}
}

func TestHarnessFailsClosedOnUnprotectedKeyOrRegistryOutage(t *testing.T) {
	keyFile := writeTestKey(t)
	if err := os.Chmod(keyFile, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewHarness(harnessTestConfig(keyFile), PurposeTenantContract, ApprovalHints{}, &fakeAccountRegistry{}); err != nil {
		t.Fatalf("harness construction validates lazily on arm, got: %v", err)
	}
	harness, err := NewHarness(harnessTestConfig(writeTestKey(t)), PurposeTenantContract, ApprovalHints{}, &fakeAccountRegistry{})
	if err != nil {
		t.Fatal(err)
	}
	protectedHarness, err := NewHarness(harnessTestConfig(keyFile), PurposeTenantContract, ApprovalHints{}, &fakeAccountRegistry{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = harness, protectedHarness
	badKeyHarness := protectedHarness
	if _, err := badKeyHarness.ArmGroupIntake(context.Background(), uuid.New(), uuid.New(), "zalo_personal", "account-1"); err == nil {
		t.Fatal("group/world-readable key file must fail arm")
	}
	outage := &fakeAccountRegistry{err: context.DeadlineExceeded}
	outageHarness, err := NewHarness(harnessTestConfig(writeTestKey(t)), PurposeTenantContract, ApprovalHints{}, outage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outageHarness.ArmGroupIntake(context.Background(), uuid.New(), uuid.New(), "zalo_personal", "account-1"); err == nil {
		t.Fatal("registry outage must fail arm")
	}
	if _, err := NewHarness(harnessTestConfig(writeTestKey(t)), OnboardingPurpose("root"), ApprovalHints{}, &fakeAccountRegistry{}); err == nil {
		t.Fatal("unknown purpose must fail construction")
	}
}
