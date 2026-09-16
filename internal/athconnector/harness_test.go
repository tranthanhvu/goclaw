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
	err   error
	last  AccountBinding
	calls int
}

func (f *fakeAccountRegistry) AlignAccount(_ context.Context, _, _ uuid.UUID, _ string, configured AccountBinding) (AccountRecord, error) {
	f.calls++
	f.last = configured
	if f.err != nil {
		return AccountRecord{}, f.err
	}
	return AccountRecord{ID: configured.AccountID, ProviderAccountID: configured.ProviderAccountID, AccountEpoch: configured.AccountEpoch}, nil
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
	athAccount := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 3}
	harness, err := NewHarness(harnessTestConfig(writeTestKey(t)), athAccount, PurposeTenantContract, ApprovalHints{}, registry)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, instanceID := uuid.New(), uuid.New()

	intake, err := harness.ArmGroupIntake(context.Background(), tenantID, instanceID, "zalo_personal", "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if !intake.Enabled() || intake.ProviderAccountID() != "account-1" || intake.AccountEpoch() != athAccount.AccountEpoch {
		t.Fatalf("intake not armed with the ATH-issued binding: %+v", intake)
	}
	if registry.last != athAccount {
		t.Fatalf("registry must record the configured binding: %+v", registry.last)
	}

	// A provider login under a different account fails closed: ATH owns the
	// identity, so the operator must update the registration after re-login.
	if _, err := harness.ArmGroupIntake(context.Background(), tenantID, instanceID, "zalo_personal", "account-2"); err == nil {
		t.Fatal("unregistered provider login must fail closed")
	}
}

func TestHarnessFailsClosedOnUnprotectedKeyOrRegistryOutage(t *testing.T) {
	goodBinding := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}

	keyFile := writeTestKey(t)
	if err := os.Chmod(keyFile, 0o644); err != nil {
		t.Fatal(err)
	}
	badKeyHarness, err := NewHarness(harnessTestConfig(keyFile), goodBinding, PurposeTenantContract, ApprovalHints{}, &fakeAccountRegistry{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := badKeyHarness.ArmGroupIntake(context.Background(), uuid.New(), uuid.New(), "zalo_personal", "account-1"); err == nil {
		t.Fatal("group/world-readable key file must fail arm")
	}

	outage := &fakeAccountRegistry{err: context.DeadlineExceeded}
	outageHarness, err := NewHarness(harnessTestConfig(writeTestKey(t)), goodBinding, PurposeTenantContract, ApprovalHints{}, outage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outageHarness.ArmGroupIntake(context.Background(), uuid.New(), uuid.New(), "zalo_personal", "account-1"); err == nil {
		t.Fatal("registry outage must fail arm")
	}
	if _, err := NewHarness(harnessTestConfig(writeTestKey(t)), goodBinding, OnboardingPurpose("root"), ApprovalHints{}, &fakeAccountRegistry{}); err == nil {
		t.Fatal("unknown purpose must fail construction")
	}
	unbound := goodBinding
	unbound.AccountID = uuid.Nil
	if _, err := NewHarness(harnessTestConfig(writeTestKey(t)), unbound, PurposeTenantContract, ApprovalHints{}, &fakeAccountRegistry{}); err == nil {
		t.Fatal("unbound account must fail construction")
	}
}
