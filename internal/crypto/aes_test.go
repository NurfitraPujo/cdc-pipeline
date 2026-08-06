package crypto

import (
	"testing"
)

func TestGetEncryptionKey(t *testing.T) {
	// Test case 1: ENCRYPTION_KEY is not set
	t.Setenv("ENCRYPTION_KEY", "")
	key := GetEncryptionKey()
	if key != nil {
		t.Errorf("Expected nil key when ENCRYPTION_KEY is not set, got %v", key)
	}

	// Test case 2: ENCRYPTION_KEY is set
	testKey := "1234567890123456"
	t.Setenv("ENCRYPTION_KEY", testKey)
	key = GetEncryptionKey()
	if string(key) != testKey {
		t.Errorf("Expected key %s, got %s", testKey, string(key))
	}
}

func TestEncrypt(t *testing.T) {
	plaintext := "Hello, World!"

	tests := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{"Valid 16-byte key", []byte("1234567890123456"), false},
		{"Valid 24-byte key", []byte("123456789012345678901234"), false},
		{"Valid 32-byte key", []byte("12345678901234567890123456789012"), false},
		{"Empty key", []byte(""), true},
		{"Invalid key length", []byte("invalid"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ciphertext, err := Encrypt(plaintext, tt.key)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if ciphertext != "" {
					t.Errorf("Expected empty ciphertext on error, got %s", ciphertext)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if ciphertext == "" {
					t.Error("Expected non-empty ciphertext")
				}
			}
		})
	}
}

func TestDecrypt(t *testing.T) {
	key := []byte("1234567890123456")
	plaintext := "Hello, World!"
	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("Failed to encrypt for setup: %v", err)
	}

	tests := []struct {
		name       string
		ciphertext string
		key        []byte
		wantErr    bool
	}{
		{"Successful decryption", ciphertext, key, false},
		{"Empty key", ciphertext, []byte(""), true},
		{"Invalid base64", "invalid-base64", key, true},
		{"Ciphertext too short", "abcd", key, true},
		{"Incorrect key", ciphertext, []byte("wrong-key-123456"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decrypted, err := Decrypt(tt.ciphertext, tt.key)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if decrypted != "" {
					t.Errorf("Expected empty decrypted string on error, got %s", decrypted)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if decrypted != plaintext {
					t.Errorf("Expected decrypted %s, got %s", plaintext, decrypted)
				}
			}
		})
	}
}

func TestEncryptionRoundTrip(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	originalPlaintext := "Sensitive message to be encrypted"

	ciphertext, err := Encrypt(originalPlaintext, key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if ciphertext == "" {
		t.Fatal("Ciphertext is empty")
	}

	decryptedPlaintext, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if decryptedPlaintext != originalPlaintext {
		t.Errorf("Expected %s, got %s", originalPlaintext, decryptedPlaintext)
	}
}
