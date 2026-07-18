package api

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/bcrypt"
)

func TestEnsureDevAuth_NoOpInProduction(t *testing.T) {
	t.Setenv("ENV", "production")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kv := mocks.NewMockKeyValue(ctrl)
	// Expect no calls: production must not touch the KV.
	if err := EnsureDevAuth(kv); err != nil {
		t.Fatalf("EnsureDevAuth in production should be a no-op, got error: %v", err)
	}
}

func TestEnsureDevAuth_SkipsWhenUserExists(t *testing.T) {
	t.Setenv("ENV", "development")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kv := mocks.NewMockKeyValue(ctrl)
	existing := []byte(`{"username":"existing","password":"hash"}`)
	kv.EXPECT().Get(protocol.KeyAuthConfig).Return(mockEntry{value: existing}, nil)

	if err := EnsureDevAuth(kv); err != nil {
		t.Fatalf("EnsureDevAuth should be a no-op when user exists, got error: %v", err)
	}
}

func TestEnsureDevAuth_SeedsWhenMissing(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("DEV_ADMIN_USERNAME", "tester")
	t.Setenv("DEV_ADMIN_PASSWORD", "testerpass")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kv := mocks.NewMockKeyValue(ctrl)
	kv.EXPECT().Get(protocol.KeyAuthConfig).Return(nil, nats.ErrKeyNotFound)
	kv.EXPECT().Put(protocol.KeyAuthConfig, gomock.Any()).DoAndReturn(func(_ string, value []byte) (uint64, error) {
		var got protocol.UserConfig
		if err := json.Unmarshal(value, &got); err != nil {
			t.Fatalf("put value is not valid JSON: %v", err)
		}
		if got.Username != "tester" {
			t.Errorf("expected username %q, got %q", "tester", got.Username)
		}
		if err := bcrypt.CompareHashAndPassword([]byte(got.Password), []byte("testerpass")); err != nil {
			t.Errorf("stored hash does not match DEV_ADMIN_PASSWORD: %v", err)
		}
		return 1, nil
	})

	if err := EnsureDevAuth(kv); err != nil {
		t.Fatalf("EnsureDevAuth failed: %v", err)
	}
}

func TestEnsureDevAuth_DefaultCredentials(t *testing.T) {
	t.Setenv("ENV", "development")
	os.Unsetenv("DEV_ADMIN_USERNAME")
	os.Unsetenv("DEV_ADMIN_PASSWORD")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kv := mocks.NewMockKeyValue(ctrl)
	kv.EXPECT().Get(protocol.KeyAuthConfig).Return(nil, nats.ErrKeyNotFound)
	kv.EXPECT().Put(protocol.KeyAuthConfig, gomock.Any()).DoAndReturn(func(_ string, value []byte) (uint64, error) {
		var got protocol.UserConfig
		if err := json.Unmarshal(value, &got); err != nil {
			t.Fatalf("put value is not valid JSON: %v", err)
		}
		if got.Username != "admin" {
			t.Errorf("expected default username 'admin', got %q", got.Username)
		}
		if err := bcrypt.CompareHashAndPassword([]byte(got.Password), []byte("admin")); err != nil {
			t.Errorf("default hash does not match 'admin': %v", err)
		}
		return 1, nil
	})

	if err := EnsureDevAuth(kv); err != nil {
		t.Fatalf("EnsureDevAuth failed: %v", err)
	}
}
