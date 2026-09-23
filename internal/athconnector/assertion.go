package athconnector

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AssertionLifetime bounds how long a signed assertion stays acceptable. The
// window is deliberately short: assertions are minted per request by the
// isolated connector service and must not be reusable after replay windows.
const AssertionLifetime = 60 * time.Second

// jtiLen is the hex-encoded length of a 16-byte random nonce (32 chars).
const jtiLen = 32

// AssertionScope carries the trusted context a signature binds itself to:
// connector identity, authenticated account and epoch, and the conversation
// the request is about. Peer fields may be empty for account-scoped control
// actions such as catalog refresh receipts.
type AssertionScope struct {
	ConnectorID    uuid.UUID
	AccountID      uuid.UUID
	AccountEpoch   int
	PeerKind       string
	ConversationID string
}

type assertionHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

type assertionClaims struct {
	Issuer         string `json:"iss"`
	Audience       string `json:"aud"`
	Subject        string `json:"sub"`
	Environment    string `json:"env"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	Action         string `json:"action"`
	BodyDigest     string `json:"body_sha256"`
	AccountID      string `json:"account_id"`
	AccountEpoch   int    `json:"account_epoch"`
	PeerKind       string `json:"peer_kind,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	IssuedAt       int64  `json:"iat"`
	NotBefore      int64  `json:"nbf"`
	ExpiresAt      int64  `json:"exp"`
	JTI            string `json:"jti"`
}

// Signer mints short-lived EdDSA assertions for the ATH control plane. It runs
// only inside the isolated connector-only service; the private key is loaded
// from a protected secret file and never leaves the signer.
type Signer struct {
	issuer      string
	environment string
	keyID       string
	key         ed25519.PrivateKey
	now         func() time.Time
}

// NewSigner builds a signer bound to one issuer, environment, and key. It
// returns nil when the binding is incomplete; Sign on a nil signer fails
// closed, so an invalid construction can never mint assertions.
func NewSigner(issuer, environment, keyID string, key ed25519.PrivateKey, now func() time.Time) *Signer {
	if issuer == "" || environment == "" || keyID == "" {
		return nil
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil
	}
	if now == nil {
		return nil
	}
	return &Signer{issuer: issuer, environment: environment, keyID: keyID, key: key, now: now}
}

// Sign produces a compact JWS over the exact HTTP route, action, body digest,
// and trusted scope. The audience pins the control plane for the configured
// environment so an assertion cannot be replayed against another environment.
func (s *Signer) Sign(method, path, action string, body []byte, scope AssertionScope) (string, error) {
	if s == nil {
		return "", errors.New("athconnector: signer is not initialized")
	}
	if method == "" || path == "" || action == "" {
		return "", errors.New("athconnector: method, path and action are required")
	}
	if scope.ConnectorID == uuid.Nil || scope.AccountID == uuid.Nil {
		return "", errors.New("athconnector: connector and account identity are required")
	}
	if scope.AccountEpoch < 1 {
		return "", fmt.Errorf("athconnector: account epoch must be positive, got %d", scope.AccountEpoch)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("athconnector: nonce generation failed: %w", err)
	}
	digest := sha256.Sum256(body)
	issued := s.now().UTC()
	header := assertionHeader{Algorithm: "EdDSA", Type: "ath-connector+jwt", KeyID: s.keyID}
	claims := assertionClaims{
		Issuer:         s.issuer,
		Audience:       "ath-connector-control:" + s.environment,
		Subject:        scope.ConnectorID.String(),
		Environment:    s.environment,
		Method:         method,
		Path:           path,
		Action:         action,
		BodyDigest:     base64.RawURLEncoding.EncodeToString(digest[:]),
		AccountID:      scope.AccountID.String(),
		AccountEpoch:   scope.AccountEpoch,
		PeerKind:       scope.PeerKind,
		ConversationID: scope.ConversationID,
		IssuedAt:       issued.Unix(),
		NotBefore:      issued.Unix(),
		ExpiresAt:      issued.Add(AssertionLifetime).Unix(),
		JTI:            hex.EncodeToString(nonce),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("athconnector: header encoding failed: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("athconnector: claims encoding failed: %w", err)
	}
	if len(claims.JTI) != jtiLen {
		return "", errors.New("athconnector: nonce is outside contract")
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	signature := ed25519.Sign(s.key, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
