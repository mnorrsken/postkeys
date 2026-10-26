.PHONY: build test vuln bench docker-up docker-down test-up test-down clean docker-build deploy

# Load local dev settings if present
-include .dev.env
export

# Default values (can be overridden in .dev.env)
DEV_REPO ?= ghcr.io/mnorrsken
DEV_NAMESPACE ?= postkeys-dev

# Build
build:
	go build -o bin/postkeys ./cmd/server

# Build Docker image for development
docker-build:
	docker build -t $(DEV_REPO)/postkeys:dev .

# Push Docker image
docker-push: docker-build
	docker push $(DEV_REPO)/postkeys:dev

# Create namespace if missing and deploy example to Kubernetes
deploy:
	@kubectl get namespace $(DEV_NAMESPACE) > /dev/null 2>&1 || kubectl create namespace $(DEV_NAMESPACE)
	@echo "Deploying to namespace: $(DEV_NAMESPACE)"
	kubectl apply -k dev/simple -n $(DEV_NAMESPACE)
	helm upgrade --install postkeys ./charts/postkeys --namespace $(DEV_NAMESPACE) -f dev/simple/values.yaml --set image.repository=$(DEV_REPO)/postkeys --set image.tag=dev --set image.pullPolicy=Always

# Delete deployment
undeploy:
	@echo "Deleting deployment from namespace: $(DEV_NAMESPACE)"
	helm uninstall postkeys -n $(DEV_NAMESPACE) --ignore-not-found
	kubectl delete -k dev/simple -n $(DEV_NAMESPACE) --ignore-not-found

# Start test PostgreSQL container
test-up:
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@until docker compose -f docker-compose.test.yml exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "PostgreSQL is ready!"
	@echo "Waiting for Redis to be ready..."
	@until docker compose -f docker-compose.test.yml exec -T redis redis-cli ping > /dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "Redis is ready!"

# Stop test PostgreSQL container
test-down:
	docker compose -f docker-compose.test.yml down -v

# Run integration tests against PostgreSQL
test: test-up
	go test -v -tags=postgres ./tests/...
	go test -v ./internal/...

# Check dependencies and stdlib for known vulnerabilities
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

# Run benchmarks against PostgreSQL
bench: test-up
	go test -bench=. -benchmem -tags=postgres ./tests/...

# Run benchmarks against Redis 7
bench-redis: test-up
	go test -bench=. -benchmem -tags=redis ./tests/...

# Run benchmarks comparing PostgreSQL vs Redis
bench-compare: test-up
	@echo "=== PostgreSQL Benchmarks ==="
	go test -bench=. -benchmem -tags=postgres ./tests/... 2>&1 | tee /tmp/bench-pg.txt
	@echo ""
	@echo "=== Redis 7 Benchmarks ==="
	go test -bench=. -benchmem -tags=redis ./tests/... 2>&1 | tee /tmp/bench-redis.txt

# Start production containers
docker-up:
	docker compose up -d

# Stop production containers
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf bin/
	docker compose -f docker-compose.test.yml down -v 2>/dev/null || true
