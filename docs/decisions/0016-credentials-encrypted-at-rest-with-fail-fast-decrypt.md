---
status: accepted
date: 2026-08-01
decision-makers: cdc-pipeline maintainers
consulted: internal/crypto/aes.go, internal/protocol/config.go:313-343 (T2-3)
---

# Credentials are AES-GCM encrypted in KV, and decryption failure is fatal

## Context and Problem Statement

Source passwords and sink DSNs live in NATS KV. KV is readable by anything with cluster access, so
storing them in plaintext makes a NATS compromise a credential compromise
(`rfc/RFC-001-Architecture-and-Design.md:275-278`).

## Decision Outcome

**AES-GCM** with a master key from `ENCRYPTION_KEY`, random nonce per encryption prefixed to the
ciphertext (`internal/crypto/aes.go:38-67`). Encryption happens at the API write boundary, before
the KV put (`internal/api/handler.go:809,873,1258,1329`).

**Human-readable passphrases are rejected outright** — the key must base64-decode to 16/24/32
bytes, or be raw bytes of those lengths. "This prevents weak keys from being used for encryption"
(`aes.go:12-17`). A missing env var is an error, never a default.

**Decryption fails fast (T2-3).** `SourceConfig.Decrypt` / `SinkConfig.Decrypt` return the error
wrapped with the config ID; there is no fallback to treating the stored value as plaintext
(`config.go:313-343`). Callers abort worker construction (`factory.go:64,96,193`).

### Consequences

* Good: a NATS compromise does not yield usable credentials.
* Good: a wrong, rotated or missing key fails loudly at startup instead of dialling Postgres with
  ciphertext-as-password and producing a confusing auth error far from the cause.
* Good: the invariant is enforced in tests — the e2e harness must encrypt under the test key "so the
  engine's fail-fast Decrypt (T2-3) does not reject them" (`internal/test/e2e/env.go:42-45`).
* Bad: key rotation has no story here. Re-encrypting every stored config is a manual operation.
* Bad: naming wart — the field is `PassEncrypted` but holds **plaintext** after `Decrypt()` mutates
  it in place (`config.go:325`).
* Bad: `docker-compose.yaml:125` defaults `ENCRYPTION_KEY` to a literal 32-byte string. It passes
  validation, so it is convenient locally and **must never reach a shared environment**.

## More Information

The rejected alternative is the silent fallback this replaced: treating a decryption failure as
"the value must already be plaintext". That both hides misconfiguration and lets an unencrypted
credential persist unnoticed (`docs/todos/holistic_review_remediation.md:363`).
