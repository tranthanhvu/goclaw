package athconnector

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// SettingsModeATHConnector is the MCP settings mode marking a server entry as
// the ATH connector registration for one channel instance.
const SettingsModeATHConnector = "ath-connector"

// Registration is the validated connector configuration persisted inside an
// MCP server entry's settings. PrivateKeyFile is write-only: it names the
// isolated connector service's protected secret mount and is stripped from
// every API response, list, and export.
type Registration struct {
	Mode              string            `json:"mode"`
	GatewayURL        string            `json:"gateway_url"`
	ConnectorID       uuid.UUID         `json:"connector_id"`
	Environment       string            `json:"environment"`
	Issuer            string            `json:"issuer"`
	KeyID             string            `json:"key_id"`
	PrivateKeyFile    string            `json:"private_key_file,omitempty"`
	ChannelInstanceID uuid.UUID         `json:"channel_instance_id"`
	Purpose           OnboardingPurpose `json:"purpose"`
}

// ParseRegistration extracts and validates an ATH connector registration from
// MCP settings JSON. Every field must come from the operator payload; unknown
// modes return (nil, nil) so non-connector servers pass through untouched.
func ParseRegistration(settings json.RawMessage) (*Registration, error) {
	if len(settings) == 0 {
		return nil, nil
	}
	var probe struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(settings, &probe); err != nil {
		return nil, fmt.Errorf("athconnector: settings are not valid JSON: %w", err)
	}
	if probe.Mode == "" {
		return nil, nil
	}
	if probe.Mode != SettingsModeATHConnector {
		return nil, fmt.Errorf("athconnector: unsupported settings mode %q", probe.Mode)
	}
	var reg Registration
	if err := json.Unmarshal(settings, &reg); err != nil {
		return nil, fmt.Errorf("athconnector: connector registration is malformed: %w", err)
	}
	if reg.ChannelInstanceID == uuid.Nil {
		return nil, errors.New("athconnector: connector registration requires the owned channel instance id")
	}
	switch reg.Purpose {
	case PurposeTenantContract, PurposeSalesInventory, PurposeManagementAccess:
	default:
		return nil, fmt.Errorf("athconnector: unsupported onboarding purpose %q", reg.Purpose)
	}
	// Reuse the enablement contract so registration-time validation and the
	// runtime check cannot drift apart.
	cfg := Config{
		Enabled:        true,
		RuntimeRole:    RuntimeRoleConnector,
		GatewayURL:     reg.GatewayURL,
		ConnectorID:    reg.ConnectorID,
		Environment:    reg.Environment,
		Issuer:         reg.Issuer,
		KeyID:          reg.KeyID,
		PrivateKeyFile: reg.PrivateKeyFile,
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &reg, nil
}

// RedactSettings returns settings with the signer secret reference removed.
// Non-connector settings pass through unchanged. A redaction failure fails
// closed by returning empty settings: callers must never serve the raw value.
func RedactSettings(settings json.RawMessage) json.RawMessage {
	if len(settings) == 0 {
		return settings
	}
	reg, err := ParseRegistration(settings)
	if err != nil || reg == nil {
		// Not a connector registration (or invalid): serve as-is only when it
		// is not marked as one. Invalid connector-shaped settings are already
		// rejected at write time; reads must not leak the key reference from
		// legacy or hand-edited rows.
		var probe struct {
			Mode string `json:"mode"`
		}
		if json.Unmarshal(settings, &probe) == nil && probe.Mode == SettingsModeATHConnector {
			redacted := map[string]any{}
			if err := json.Unmarshal(settings, &redacted); err == nil {
				delete(redacted, "private_key_file")
				if out, err := json.Marshal(redacted); err == nil {
					return out
				}
			}
			return json.RawMessage(`{}`)
		}
		return settings
	}
	redacted := *reg
	redacted.PrivateKeyFile = ""
	out, err := json.Marshal(redacted)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return out
}

// Config converts the registration into the runtime enablement config.
func (r *Registration) Config() Config {
	return Config{
		Enabled:        true,
		RuntimeRole:    RuntimeRoleConnector,
		GatewayURL:     r.GatewayURL,
		ConnectorID:    r.ConnectorID,
		Environment:    r.Environment,
		Issuer:         r.Issuer,
		KeyID:          r.KeyID,
		PrivateKeyFile: r.PrivateKeyFile,
	}
}

// IsConnectorSettings reports whether raw MCP settings carry the connector mode.
func IsConnectorSettings(settings json.RawMessage) bool {
	if len(settings) == 0 {
		return false
	}
	var probe struct {
		Mode string `json:"mode"`
	}
	return json.Unmarshal(settings, &probe) == nil && probe.Mode == SettingsModeATHConnector
}
