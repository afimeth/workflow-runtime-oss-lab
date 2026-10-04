# Workflow Runtime OSS Lab

Execute a dependency graph with bounded concurrency without confusing safe retries with crash recovery.

[![CI](https://github.com/afimeth/workflow-runtime-oss-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/afimeth/workflow-runtime-oss-lab/actions)

## Run, test and demo

```sh
go run . workflow.json demo-journal.json
go test -v ./...
go test -bench BenchmarkDAG -benchtime=5x -run "^$" ./...
python scripts/verify.py
```

Install Python 3.11+ for the evidence harness. Install Go 1.23+. External paid services are not required.

## Headless click-to-run contract

`go run . workflow.json demo-journal.json` is the runtime entrypoint. Treat it as the headless equivalent of a click-to-run action: provide a workflow object, execute it, and return journal/evidence state.

No UI is required or shipped. A web, IDE, desktop, or other surface can wrap the same runtime later. The runtime contract remains the graph, execution semantics, journal, and receipts rather than a specific interface.

## Implemented

Go DAG validation, deterministic admission order, adapter interface, concurrency limit, atomic journal replacement, conservative interrupted RUNNING -> HOLD recovery, explicit Retryable errors, dependency HOLD propagation, context cancellation, spec-hash binding CLI.

## Evidence and status

- **IMPLEMENTED:** runnable code and failure tests in this repository.
- **MEASURED:** [baseline](evidence/baseline.json) identifies the measured source commit. Each CI matrix job uploads a fresh `receipt.json` for its exact `GITHUB_SHA`, with actual test totals and toolchain. A receipt commit does not rewrite the measured source SHA.
- **DESIGNED:** Kubernetes integration and an upstream Argo contribution are not implemented.
- **NOT CLAIMED:** One scheduler owns one journal; no multi-process lock, distributed scheduler, Kubernetes/Argo integration, containers, durable directory fsync on all platforms, leases, remote plugins, untrusted-code isolation, backoff timing, upstream OSS contribution, or production on-call history. Adapters must honor cancellation; a noncooperative adapter can prevent completion. Cancellation does not roll back completed effects. Direct Run callers must bind the workflow spec as the CLI does.

Run `python scripts/verify.py` to regenerate ignored local receipts. Benchmark/gas/bundle reports are local measurements, not a production SLO. CI and local runs are separate evidence. Passing tests are not an independent review or an accepted production release.

## Safe CV claim

> Built a Go DAG execution lab with bounded concurrency, adapter contracts, explicit safe retries, cancellation, and conservative journal recovery.

This describes a laboratory project. It does not establish years of experience, a degree, production scale, employer history, CVEs, mainnet ownership, or independent audit credentials.

## Design and review walkthrough

See [architecture](docs/ARCHITECTURE.md), [limitations](docs/LIMITATIONS.md), and [interview scenarios](docs/INTERVIEW_SCENARIOS.md). All inputs are synthetic. This is a fresh AI-assisted standalone implementation from public requirements; no private source, customer data, credentials or proprietary code was copied. MIT license applies to this lab.
