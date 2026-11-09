SHELL := /bin/bash
GO ?= $(if $(wildcard $(HOME)/.local/share/agentcloud/go/bin/go),$(HOME)/.local/share/agentcloud/go/bin/go,go)
PYTHON ?= python3

.PHONY: help bootstrap doctor upstream verify-upstream build test vet replay check p0 p0-substrate p0-fencing p0-dynamo evidence-audit verify-delivery demo serve smoke bench native-build native-verify native-bench live-bench live-suite disk-check

help:
	@echo 'bootstrap       安装并校验 Go 1.27.3 用户空间工具链'
	@echo 'doctor          生成本机环境与上游版本报告'
	@echo 'upstream        检出锁定的 Substrate 与 Dynamo 源码'
	@echo 'build           构建HTTP协调器和回放CLI'
	@echo 'demo            一键运行HTTP持久恢复演示'
	@echo 'check           运行本地测试、静态检查与状态回放'
	@echo 'native-verify   验证真实Dynamo Frontend与官方Mocker'
	@echo 'native-bench    运行四画像原生配对测量'
	@echo 'live-suite      运行真实Substrate双Agent与A～E混合负载'
	@echo 'live-bench      运行真实Substrate与Dynamo消融实验'
	@echo 'disk-check      检查快照与编译所需磁盘余量'
	@echo 'p0              运行两组真实上游测试并汇总证据'
	@echo 'evidence-audit  校验证据引用与日志完整性'

bootstrap:
	$(PYTHON) scripts/bootstrap.py

doctor:
	$(PYTHON) scripts/preflight.py

upstream:
	$(PYTHON) scripts/upstream.py --fetch

verify-upstream:
	$(PYTHON) scripts/upstream.py

build:
	@mkdir -p bin
	$(GO) build -o bin/agentcloud ./cmd/agentcloud

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...
	$(PYTHON) -m compileall -q scripts experiments/p0

replay:
	$(GO) run ./cmd/agentcloud replay --input fixtures/signals/demo.jsonl --output artifacts/replay-demo.json

check:
	$(PYTHON) scripts/check.py --go '$(GO)'

p0: doctor verify-upstream
	$(PYTHON) scripts/run_p0.py

p0-substrate:
	$(PYTHON) experiments/p0/substrate_probe.py --go '$(GO)'

p0-fencing:
	$(PYTHON) experiments/p0/substrate_probe.py --go '$(GO)' --stage whitebox

p0-dynamo:
	$(PYTHON) experiments/p0/dynamo_probe.py

evidence-audit:
	$(PYTHON) scripts/evidence_audit.py

verify-delivery:
	$(PYTHON) scripts/repro_sources.py

serve: build
	$(GO) run ./cmd/agentcloud serve

smoke:
	$(PYTHON) scripts/smoke_service.py

bench:
	$(PYTHON) -m bench.run

native-build:
	$(PYTHON) experiments/dynamo/build_cpu.py

native-verify:
	$(PYTHON) experiments/dynamo/native_mocker.py verify

demo:
	$(PYTHON) scripts/demo.py

native-bench:
	$(PYTHON) -m bench.native

live-bench:
	$(PYTHON) scripts/disk_guard.py
	$(PYTHON) scripts/run_live_bench.py

live-suite:
	$(PYTHON) scripts/live_suite.py

disk-check:
	$(PYTHON) scripts/disk_guard.py
