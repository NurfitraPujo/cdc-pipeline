### Vulnerability Findings

1. **Vulnerability:** Hardcoded JWT Secret
   **Vulnerability Type:** Security
   **Severity:** High
   **Source Location:** `internal/api/auth.go`
   **Line Content:** `return []byte("cdc-secret-key-change-me-in-production")`
   **Description:** A default, hardcoded JWT secret is used if the `JWT_SECRET` environment variable is not set. This secret is easily guessable or discoverable, allowing attackers to forge valid JWT tokens and bypass authentication.
   **Recommendation:** Remove the hardcoded default secret and enforce the requirement for a strong, externally managed secret via environment variables. The application should fail to start if the secret is missing.

2. **Vulnerability:** Plaintext Password Storage/Comparison
   **Vulnerability Type:** Security
   **Severity:** High
   **Source Location:** `internal/api/auth.go`
   **Line Content:** `if creds.Username == authCfg.Username && creds.Password == authCfg.Password {`
   **Description:** The application compares user-provided passwords directly against values stored in the configuration (NATS KV). This strongly implies that passwords are stored in plaintext. If the storage is compromised, all user credentials are exposed.
   **Recommendation:** Store only salted and hashed versions of passwords (e.g., using Argon2 or bcrypt). Use a secure comparison function to verify passwords during login.

3. **Vulnerability:** Insecure Direct Object Reference (IDOR) / Missing Function-Level Access Control
   **Vulnerability Type:** Security
   **Severity:** High
   **Source Location:** `internal/api/handler.go`
   **Line Content:** (Multiple handlers, e.g., `UpdatePipeline`, `DeletePipeline`)
   **Description:** The API endpoints for managing pipelines, sources, and sinks do not verify if the authenticated user has permission to access or modify the specific resource identified by the `id` parameter. Any user with a valid JWT can perform any action on any resource.
   **Recommendation:** Implement an ownership model or a fine-grained access control list (ACL). Each resource should be associated with an owner (user or group), and the API should verify this association before performing sensitive operations.

4. **Vulnerability:** Plaintext Sensitive Information in Configuration
   **Vulnerability Type:** Privacy/Security
   **Severity:** High
   **Source Location:** `internal/protocol/config.go`
   **Line Content:** `PassEncrypted string `msg:"pass" yaml:"pass" json:"pass"``, `DSN string `msg:"dsn" yaml:"dsn" json:"dsn"``
   **Description:** Configuration for sources and sinks, including database passwords and DSNs (which often contain credentials), are stored in plaintext in NATS KV. While `PassEncrypted` is named as if it's encrypted, the current implementation in `CreateSource`/`UpdateSource` handles it as a standard string from JSON input without any encryption logic visible in the handlers.
   **Recommendation:** Encrypt sensitive configuration values before storing them. Use a key management system (KMS) or a vault to manage encryption keys.

5. **Vulnerability:** SQL Injection in Sink (DDL and DML)
   **Vulnerability Type:** Security
   **Severity:** Critical
   **Source Location:** `internal/sink/databend/sink.go`
   **Line Content:** `query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", schema.Table, strings.Join(colDefs, ", "))`, `query := fmt.Sprintf("REPLACE INTO \"%s\" (%s) ON (%s) VALUES ", table, colList, pkList)`
   **Description:** Table names, column names, and primary key names are directly formatted into SQL queries using `fmt.Sprintf`. While these values are wrapped in double quotes, they are not properly escaped. An attacker who can influence the schema of the source database (or a malicious source) can inject arbitrary SQL into the Databend sink. The developer marked these as `#nosec G201` assuming the source schema is trusted, which is a dangerous assumption in a distributed system.
   **Recommendation:** Use a proper SQL builder or implement strict sanitization/validation for table and column names. Specifically, ensure that names do not contain double quotes or other control characters.
