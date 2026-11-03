# CareFlow Atlas Makefile
# Root build, lifecycle, and test interface

SHELL := /usr/bin/env bash
PYTHON := uv run python
PYTEST := uv run pytest
COMPOSE_FILE := infrastructure/compose/docker-compose.yml
export PYTHONPATH := packages/contracts/src:.
export JAVA_HOME ?= /Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
export AWS_REGION := us-east-1
export AWS_DEFAULT_REGION := us-east-1
export AWS_ACCESS_KEY_ID := minioadmin
export AWS_SECRET_ACCESS_KEY := miniopassword

.PHONY: all help bootstrap up down fixtures smoke test-unit test-contracts test-integration test-security test-recovery demo clean

all: help

help:
	@echo "CareFlow Atlas - Governed Healthcare ETL Platform"
	@echo ""
	@echo "Available targets:"
	@echo "  make bootstrap         Set up Python 3.11 virtual environment & dependencies"
	@echo "  make up                Launch PostgreSQL 16 & MinIO containers and init buckets"
	@echo "  make down              Stop local infrastructure without deleting data volumes"
	@echo "  make smoke             Run PySpark Iceberg write/merge/snapshot smoke test"
	@echo "  make test-contracts    Run contract and invariant test suite"
	@echo "  make test-unit         Run unit and contract test suites"
	@echo "  make fixtures          (Phase 1+) Generate synthetic healthcare fixtures"
	@echo "  make test-integration   (Phase 1+) Run integration test suite"
	@echo "  make test-security     (Phase 4+) Run security and isolation test suite"
	@echo "  make test-recovery     (Phase 5+) Run crash recovery and replay test suite"
	@echo "  make demo              (Phase 6+) Run 10-minute automated demo"
	@echo "  make clean             Remove temporary build and test caches"

bootstrap:
	@echo "==> Bootstrapping CareFlow Atlas environment..."
	@which uv > /dev/null || (echo "ERROR: 'uv' package manager not found. Install uv to proceed." && exit 1)
	uv sync
	@echo "==> Environment bootstrapped successfully with Python: $$($(PYTHON) --version)"

up:
	@echo "==> Starting local infrastructure (PostgreSQL 16, MinIO)..."
	docker compose -f $(COMPOSE_FILE) up -d --pull never
	@echo "==> Waiting for services to become healthy..."
	@docker compose -f $(COMPOSE_FILE) ps
	@echo "==> Initializing MinIO buckets..."
	$(PYTHON) infrastructure/compose/init_buckets.py
	@echo "==> Infrastructure ready."

down:
	@echo "==> Stopping local infrastructure (non-destructive)..."
	docker compose -f $(COMPOSE_FILE) down
	@echo "==> Infrastructure stopped."

smoke:
	@echo "==> Running PySpark + Apache Iceberg write/merge/snapshot smoke test..."
	$(PYTHON) pipelines/spark/smoke_test.py

test-contracts:
	@echo "==> Running contract invariant tests..."
	$(PYTEST) tests/contracts/ -v

test-unit:
	@echo "==> Running unit test suites..."
	$(PYTEST) tests/contracts/ tests/unit/ -v

fixtures:
	@echo "[Phase 1] Synthea fixture generation is scheduled for Phase 1."

test-integration:
	@echo "[Phase 1+] Integration tests scheduled for Phase 1."

test-security:
	@echo "[Phase 4+] Security tests scheduled for Phase 4."

test-recovery:
	@echo "[Phase 5+] Recovery tests scheduled for Phase 5."

demo:
	@echo "[Phase 6+] End-to-end demo script scheduled for Phase 6."

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__ tests/**/__pycache__ packages/**/__pycache__
