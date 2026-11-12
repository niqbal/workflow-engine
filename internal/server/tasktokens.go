package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
)

func encodeTaskToken[T any](key []byte, token T) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	sig := mac.Sum(nil)

	raw := make([]byte, 0, len(sig)+len(payload))
	raw = append(raw, sig...)
	raw = append(raw, payload...)
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeTaskToken[T any](key []byte, tok string) (T, error) {
	var out T
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return out, invalid("malformed task token")
	}
	if len(raw) <= sha256.Size {
		return out, invalid("malformed task token")
	}
	sig := raw[:sha256.Size]
	payload := raw[sha256.Size:]

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return out, invalid("malformed task token")
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, invalid("malformed task token")
	}
	return out, nil
}
