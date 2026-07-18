package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

func getJWTSecret() ([]byte, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("JWT_SECRET environment variable is not set")
	}
	return []byte(secret), nil
}

// Login handles user authentication.
// @Summary      Authenticate user
// @Description  Get a JWT token for authorized requests
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        credentials  body      object  true  "Username and Password"
// @Success      200  {object}  map[string]string "token"
// @Failure      401  {object}  map[string]string "unauthorized"
// @Router       /login [post]
func (h *Handler) Login(c *gin.Context) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := c.ShouldBindJSON(&creds); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	entry, err := h.kv.Get(protocol.KeyAuthConfig)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch auth config"})
		return
	}

	var authCfg protocol.UserConfig
	if err := json.Unmarshal(entry.Value(), &authCfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to parse auth config"})
		return
	}

	// Compare with bcrypt
	err = bcrypt.CompareHashAndPassword([]byte(authCfg.Password), []byte(creds.Password))
	if creds.Username == authCfg.Username && err == nil {
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"username": creds.Username,
			"exp":      time.Now().Add(time.Hour * 24).Unix(),
		})

		secret, err := getJWTSecret()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "JWT_SECRET not configured"})
			return
		}

		tokenString, err := token.SignedString(secret)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sign token"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"token": tokenString})
		return
	}

	c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
}

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := c.GetHeader("Authorization")
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}

		// Strip "Bearer " prefix
		if len(tokenString) > 7 && tokenString[:7] == "Bearer " {
			tokenString = tokenString[7:]
		}

		secret, err := getJWTSecret()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "authentication not configured"})
			return
		}

		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return secret, nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		c.Next()
	}
}

// EnsureDevAuth seeds a default admin user into NATS KV when one is not already
// present. It only runs when ENV is "development" or "dev". Intended to be called
// once at API startup so local development and E2E tests can authenticate immediately.
//
// The default credentials can be overridden via env vars:
//   - DEV_ADMIN_USERNAME  (default: "admin")
//   - DEV_ADMIN_PASSWORD  (default: "admin")
func EnsureDevAuth(kv nats.KeyValue) error {
	env := os.Getenv("ENV")
	if env != "development" && env != "dev" {
		return nil
	}

	if _, err := kv.Get(protocol.KeyAuthConfig); err == nil {
		return nil
	}

	username := os.Getenv("DEV_ADMIN_USERNAME")
	if username == "" {
		username = "admin"
	}
	password := os.Getenv("DEV_ADMIN_PASSWORD")
	if password == "" {
		password = "admin"
	}

	log.Warn().Str("username", username).Msg("seeding default dev credentials")

	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash dev password: %w", err)
	}

	cfg := protocol.UserConfig{Username: username, Password: string(hashed)}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate dev user: %w", err)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal dev user: %w", err)
	}

	if _, err := kv.Put(protocol.KeyAuthConfig, data); err != nil {
		return fmt.Errorf("put dev user: %w", err)
	}

	return nil
}
