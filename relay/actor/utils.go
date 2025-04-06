package actor

import (
	"crypto/rand"
	"encoding/base64"
)

// GenerateRandomBytes16 creates a random 16-byte identifier, base64 encoded
func GenerateRandomBytes16() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(buf)
}
