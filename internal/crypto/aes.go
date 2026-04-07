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

func GetEncryptionKey() []byte {
	key := os.Getenv("ENCRYPTION_KEY")
	if key == "" {
		// In a real prod env, we'd fail. For this exercise, we'll return nil
		// and the caller should handle it.
		return nil
	}
	// Key must be 16, 24, or 32 bytes for AES
	return []byte(key)
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
