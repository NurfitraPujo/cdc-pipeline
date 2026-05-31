APP_NAME = cdc-pipeline
CCE_NAMESPACE_STAGING = cdc-pipeline-staging
CCE_NAMESPACE_PRODUCTION = cdc-pipeline-production

.PHONY: build-all build-api build-worker build-cdc-pipeline clean release lint-helm cce-seal-string

build-all: build-api build-worker

build-cdc-pipeline: build-all

build-api:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o bin/api ./cmd/api/main.go

build-worker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o bin/worker ./cmd/pipeline/main.go

clean:
	rm -rf bin/

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
