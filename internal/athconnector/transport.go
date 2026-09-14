package athconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Control paths on the ATH gateway, mirroring the accepted control-plane
// contract. Paths are fixed constants: the signer binds assertions to the exact
// method/path/action, so clients must never assemble these dynamically.
const (
	PathContext           = "/v1/connector/context"
	PathApprovalRequests  = "/v1/connector/approval-requests"
	PathToken             = "/v1/connector/token"
	PathRevalidate        = "/v1/connector/revalidate"
	PathCatalogRefreshJob = "/v1/connector/catalog-refresh-jobs"
	PathGroupCatalog      = "/v1/connector/group-catalog"
)

// maxControlResponseBody bounds response reading; control payloads are small
// and a larger response indicates a misrouted destination.
const maxControlResponseBody = 1 << 20

// controlTimeout bounds one control-plane call. Intake runs on the message
// path, so the deadline must fail closed quickly when the gateway is down.
const controlTimeout = 15 * time.Second

// ControlError describes a non-success control-plane response.
type ControlError struct {
	StatusCode int
	ErrorCode  string
	Message    string
	RequestID  string
}

func (e *ControlError) Error() string {
	if e.ErrorCode != "" {
		return fmt.Sprintf("athconnector: control call failed: %s: %s (status %d, request %s)", e.ErrorCode, e.Message, e.StatusCode, e.RequestID)
	}
	return fmt.Sprintf("athconnector: control call failed with status %d (request %s)", e.StatusCode, e.RequestID)
}

// ControlClient signs and sends control-plane requests from the isolated
// connector service to the ATH gateway. The destination is pinned to the exact
// validated origin; redirects are refused and TLS is required outside loopback
// development (enforced by Config.Validate).
type ControlClient struct {
	cfg    Config
	signer *Signer
	http   *http.Client
}

// NewControlClient validates the configuration and binds a signer. The client
// must only be constructed inside the connector-only service: the generic
// runtime has no access to the signer or its key file.
func NewControlClient(cfg Config, signer *Signer) (*ControlClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if signer == nil {
		return nil, errors.New("athconnector: signer is required for control calls")
	}
	return &ControlClient{
		cfg:    cfg,
		signer: signer,
		http: &http.Client{
			Timeout: controlTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("athconnector: control destination must not redirect")
			},
		},
	}, nil
}

// Config exposes the validated configuration for service wiring.
func (c *ControlClient) Config() Config { return c.cfg }

// Call signs and POSTs one control request. payload is marshaled to the JSON
// body; out (when non-nil) receives the parsed `data` field of the success
// envelope. Non-2xx responses return *ControlError.
func (c *ControlClient) Call(ctx context.Context, path, action string, scope AssertionScope, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("athconnector: control payload encoding failed: %w", err)
	}
	assertion, err := c.signer.Sign(http.MethodPost, path, action, body, scope)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.cfg.GatewayURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("athconnector: control request build failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+assertion)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("athconnector: control call failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxControlResponseBody))
	if err != nil {
		return fmt.Errorf("athconnector: control response read failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			RequestID string `json:"request_id"`
			Error     struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(respBody, &failure)
		return &ControlError{
			StatusCode: resp.StatusCode,
			ErrorCode:  failure.Error.Code,
			Message:    failure.Error.Message,
			RequestID:  failure.RequestID,
		}
	}
	if out == nil {
		return nil
	}
	var envelope struct {
		RequestID string          `json:"request_id"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return fmt.Errorf("athconnector: control response envelope is invalid: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("athconnector: control response data is invalid: %w", err)
	}
	return nil
}
