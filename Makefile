APP_NAME = cdc-pipeline
CCE_NAMESPACE_STAGING = cdc-pipeline-staging
CCE_NAMESPACE_PRODUCTION = cdc-pipeline-production

.PHONY: build-all build-api build-worker build-cdc-pipeline clean release lint-helm cce-seal-string generate install-tools run-api e2e e2e-install e2e-up e2e-down e2e-list setup-hooks

build-all: build-api build-worker

build-cdc-pipeline: build-all

build-api:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o bin/api ./cmd/api/main.go

build-worker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o bin/worker ./cmd/pipeline/main.go

clean:
	rm -rf bin/

run-api:
	go run ./cmd/api/main.go

# Install the OpenAPI 3 code-generation toolchain (Go side).
install-tools:
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest

# Regenerate internal/api/generated.go from docs/openapi.yaml.
generate:
	oapi-codegen --config=oapi-codegen.yaml docs/openapi.yaml

# =============================================================================
# E2E (Playwright) targets
# =============================================================================
# E2E tests live in e2e/ at the repo root. They assume the control plane API
# is reachable on http://localhost:8080 and the Vite dev server is reachable
# on http://localhost:3000. Playwright's webServer auto-boots Vite if it is
# not already running; the API + NATS + Postgres must be started separately
# (see e2e-up, or run your existing dev stack).

e2e-install:
	cd e2e && pnpm install && pnpm exec playwright install chromium

e2e-up:
	@echo "Starting NATS + Postgres via podman for E2E..."
	podman run -d --name cdc-e2e-nats --rm -p 4222:4222 docker.io/library/nats:2.10-alpine -js || true
	podman run -d --name cdc-e2e-pg --rm -p 5432:5432 \
		-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=cdc_e2e \
		docker.io/library/postgres:16-alpine || true

e2e-down:
	podman stop cdc-e2e-nats cdc-e2e-pg 2>/dev/null || true

e2e-list:
	cd e2e && pnpm exec playwright test --list

e2e:
	cd e2e && pnpm exec playwright test

# =============================================================================
# Deployment & Release Targets
# =============================================================================

release:
	@echo "Creating new release tag from main branch"
	@if [ -z "$(TAG)" ]; then \
		echo "Please provide tag version using TAG=v*. Example: make release TAG=v1.0.0"; \
		exit 1; \
	fi
	@if echo "$(TAG)" | grep -q "[ ;:]"; then \
		echo "Tag cannot contain spaces, semicolons, or colons"; \
		exit 1; \
	fi
	@if ! echo "$(TAG)" | grep -q "^v"; then \
		echo "Tag must start with 'v'"; \
		exit 1; \
	fi
	@if git ls-remote --tags origin | grep -q "refs/tags/$(TAG)$$"; then \
		echo "Tag $(TAG) already exists on origin"; \
		exit 1; \
	fi
	@read -p "Create and push tag $(TAG) to origin? This will trigger Docker build image. [y/N] " confirm && \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		git checkout main && \
		git pull origin main && \
		git tag $(TAG) && \
		git push origin $(TAG) && \
		echo "Successfully created and pushed tag $(TAG)"; \
	else \
		echo "Operation cancelled"; \
		exit 1; \
	fi

# --- Helm ---
lint-helm:
	@echo "Linting helm chart for $(APP_NAME)"
	@if [ -z "$(env)" ]; then \
		echo "Please specify environment: make lint-helm env=staging|production"; \
		exit 1; \
	fi
	@echo "Generating helm template for $(env) environment"
	helm template deploy/helm-chart/ -f deploy/helm-chart/values.$(env).yml --name-template $(APP_NAME)-$(env) > /tmp/$(APP_NAME)-$(env).yml
	@echo "Helm template generated at /tmp/$(APP_NAME)-$(env).yml"

# --- CCE Sealed Secrets ---
cce-seal-string:
	@if [ "$(env)" != "staging" ] && [ "$(env)" != "production" ]; then \
		echo "Usage: make cce-seal-string env=<staging|production>"; \
		exit 1; \
	fi
	@which jq > /dev/null || (echo "jq is required but not installed. Install it with: brew install jq" && exit 1)
	@which fzf > /dev/null || (echo "fzf is required but not installed. Install it with: brew install fzf" && exit 1)
	@SECRET_NAME=$$(curl -s https://kubeseal-encryptor-daya-cce-production.daya.ai/namespaces/$(CCE_NAMESPACE_$(shell echo $(env) | tr '[:lower:]' '[:upper:]'))/secrets | jq -r '.secrets[]' | grep -v "paas.elb" | fzf); \
	printf "Enter the key (example: DB_PASSWORD): "; \
	read -r KEY; \
	printf "Enter the value (example: ksjw@!%%sd): "; \
	read -r VALUE; \
	echo ""; \
	RESPONSE=$$(curl -s -X POST https://kubeseal-encryptor-daya-cce-production.daya.ai/encrypt \
		-H "Content-Type: application/json" \
		-d "{ \"key\": \"$$KEY\", \"value\": \"$$VALUE\", \"namespace\": \"$(CCE_NAMESPACE_$(shell echo $(env) | tr '[:lower:]' '[:upper:]'))\", \"secretName\": \"$$SECRET_NAME\" }"); \
		echo "Encryption successful"; \
		echo ""; \
		echo "$$RESPONSE" | jq -r .encryptedValue

# =============================================================================
# Git Hooks
# =============================================================================
# Wires up the local CI safety nets under .git-hooks/. After running this,
# pre-commit and pre-push will fire automatically before every commit/push.
setup-hooks:
	@echo "Configuring git to use .git-hooks directory..."
	git config core.hooksPath .git-hooks
	chmod +x .git-hooks/pre-commit .git-hooks/pre-push
	@echo "Hooks installed successfully!"
