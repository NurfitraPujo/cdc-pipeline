package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
)

// GetEncryptionKey returns a valid AES key (16, 24, or 32 bytes).
// It accepts keys in three forms:
// 1. Raw bytes of exactly 16, 24, or 32 bytes (use os.Getenv with string conversion)
// 2. Base64-encoded strings that decode to 16, 24, or 32 bytes (preferred for safety)
// Human-readable passphrases are NOT accepted; they must be hashed via PBKDF2 first.
// This prevents weak keys from being used for encryption.
func GetEncryptionKey() ([]byte, error) {
	keyStr := os.Getenv("ENCRYPTION_KEY")
	if keyStr == "" {
		return nil, errors.New("ENCRYPTION_KEY environment variable is not set")
	}

	// Try base64 decode first; if it succeeds and produces valid length, use it
	decoded, err := base64.StdEncoding.DecodeString(keyStr)
	if err == nil && len(decoded) == 16 || len(decoded) == 24 || len(decoded) == 32 {
		return decoded, nil
	}

	// If raw string is exactly 16, 24, or 32 bytes, use it directly
	if len([]byte(keyStr)) == 16 || len([]byte(keyStr)) == 24 || len([]byte(keyStr)) == 32 {
		return []byte(keyStr), nil
	}

	// Human-readable passphrases are not allowed without PBKDF2.
	// Reject them explicitly to prevent weak key vulnerabilities.
	return nil, errors.New("ENCRYPTION_KEY must be 16, 24, or 32 raw bytes or base64-encoded; human-readable passphrases are not permitted")
}

func Encrypt(plaintext string, key []byte) (string, error) {
	// 1. Guard against empty keys (Avoid silent plain-text leaks)
	if len(key) == 0 {
		return "", errors.New("encryption key cannot be empty")
	} else if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return "", errors.New("encryption key must be 16, 24, or 32 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Seal appends the ciphertext to the nonce prefix
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

func Decrypt(ciphertextStr string, key []byte) (string, error) {
	if len(key) == 0 {
		return "", errors.New("decryption key cannot be empty")
	}

	data, err := base64.StdEncoding.DecodeString(ciphertextStr)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}
