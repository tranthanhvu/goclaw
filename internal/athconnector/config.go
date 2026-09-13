package athconnector

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"

	"github.com/google/uuid"
)

// RuntimeRole names the process shape this configuration may enable.
// RuntimeRoleConnector is the isolated connector-only service; the generic
// gateway/agent runtime must never hold signing capability.
type RuntimeRole string

const (
	RuntimeRoleConnector RuntimeRole = "connector"
	RuntimeRoleGateway   RuntimeRole = "gateway"
)

// Config is the enablement contract for the ATH connector. Validation fails
// closed unless the runtime is the isolated connector role, the destination is
// an exact HTTPS origin (loopback HTTP allowed for local development), and the
// signer identity is fully pinned.
type Config struct {
	Enabled        bool
	RuntimeRole    RuntimeRole
	GatewayURL     string
	ConnectorID    uuid.UUID
	Environment    string
	Issuer         string
	KeyID          string
	PrivateKeyFile string
}

// Validate returns nil when the configuration is safe to enable. A disabled
// configuration is always valid: absence of the connector must not break the
// generic runtime.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.RuntimeRole != RuntimeRoleConnector {
		return fmt.Errorf("athconnector: runtime role %q must not enable the ATH connector", c.RuntimeRole)
	}
	destination, err := url.Parse(c.GatewayURL)
	if err != nil {
		return fmt.Errorf("athconnector: gateway url is invalid: %w", err)
	}
	if err := validateExactOrigin(destination); err != nil {
		return err
	}
	if c.ConnectorID == uuid.Nil {
		return errors.New("athconnector: connector identity is mandatory")
	}
	if c.Environment == "" || c.Issuer == "" || c.KeyID == "" || c.PrivateKeyFile == "" {
		return errors.New("athconnector: environment, issuer, key id and private key file are required")
	}
	return nil
}

// validateExactOrigin enforces scheme, host, and the absence of any path,
// query, fragment, or userinfo. Redirects are never followed by the transport,
// so a destination carrying extra components would widen the signed surface.
func validateExactOrigin(destination *url.URL) error {
	if destination.User != nil {
		return errors.New("athconnector: gateway url must not embed credentials")
	}
	if destination.Path != "" && destination.Path != "/" {
		return fmt.Errorf("athconnector: gateway url must be an exact origin, got path %q", destination.Path)
	}
	if destination.RawQuery != "" || destination.Fragment != "" || destination.RawFragment != "" {
		return errors.New("athconnector: gateway url must not carry query or fragment")
	}
	host := destination.Hostname()
	switch destination.Scheme {
	case "https":
		if host == "" {
			return errors.New("athconnector: gateway host is required")
		}
		return nil
	case "http":
		if !isLoopbackHost(host) {
			return errors.New("athconnector: plain HTTP destination is only allowed for loopback development")
		}
		return nil
	default:
		return fmt.Errorf("athconnector: unsupported gateway scheme %q", destination.Scheme)
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// LoadPrivateKey reads the Ed25519 signer key from a protected regular file.
// Group- or world-readable key material is rejected: the signing capability is
// exclusive to the isolated connector service's secret mount.
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("athconnector: signer key file unavailable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("athconnector: signer key %q is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("athconnector: signer key %q must not be group or world readable", path)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("athconnector: signer key unreadable: %w", err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("athconnector: signer key must be a PEM PRIVATE KEY block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("athconnector: signer key parse failed: %w", err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("athconnector: signer key must be Ed25519")
	}
	return key, nil
}
