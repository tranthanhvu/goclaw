package athconnector

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestConfigValidateRequiresIsolatedRuntimeAndPinnedIdentity(t *testing.T) {
	valid := Config{
		Enabled: true, RuntimeRole: RuntimeRoleConnector, GatewayURL: "https://ath.example.test",
		ConnectorID: uuid.New(), Environment: "production", Issuer: "goclaw-ath",
		KeyID: "key-1", PrivateKeyFile: "/run/secrets/ath-ed25519",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	bad := valid
	bad.RuntimeRole = "gateway"
	if err := bad.Validate(); err == nil {
		t.Fatal("generic gateway runtime must not enable ATH connector")
	}
	bad = valid
	bad.GatewayURL = "https://ath.example.test/redirectable/path"
	if err := bad.Validate(); err == nil {
		t.Fatal("gateway URL must be an exact origin")
	}
	bad = valid
	bad.GatewayURL = "http://ath.example.test"
	if err := bad.Validate(); err == nil {
		t.Fatal("non-local HTTP destination must fail closed")
	}
	bad = valid
	bad.ConnectorID = uuid.Nil
	if err := bad.Validate(); err == nil {
		t.Fatal("connector identity is mandatory")
	}
}

func TestLoadPrivateKeyRequiresProtectedRegularFile(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "connector.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPrivateKey(path)
	if err != nil {
		t.Fatalf("protected key rejected: %v", err)
	}
	if !privateKey.Equal(loaded) {
		t.Fatal("loaded key differs")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivateKey(path); err == nil {
		t.Fatal("group/world-readable signer key must be rejected")
	}
}
