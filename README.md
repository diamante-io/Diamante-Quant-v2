# Diamante Quant v2

> **Mirror Notice**  
> This public repository is a read-only mirror of our private codebase. Issues and PRs are welcome here, but canonical development occurs in the private repo and is synced periodically.

**Diamante Quant v2** is a next-generation, modular blockchain that unifies three execution paradigms—Type-3 zkEVM, enterprise Chaincode, and a native WASM VM ("DNA")—over a hybrid consensus pipeline: PoH (pre-ordering), DPoS (validator election), and aBFT (deterministic finality). The protocol adopts a post-quantum cryptography (PQC) baseline by default and includes a confidentiality layer for shielded transfers and private computation.

**Whitepaper**: [Diamante Quantum White Paper](docs/whitepaper.pdf)

**Status**: Active testnet engineering; APIs and internals subject to change.

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture at a Glance](#architecture-at-a-glance)
- [Modules](#modules)
  - [Consensus](#consensus)
  - [Cryptography (PQC Baseline)](#cryptography-pqc-baseline)
  - [Networking / P2P](#networking--p2p)
  - [Ledger / State](#ledger--state)
  - [Transactions & Mempool](#transactions--mempool)
  - [Unified Smart-Contract Stack](#unified-smart-contract-stack)
  - [Cross-VM Messaging (CVM)](#cross-vm-messaging-cvm)
  - [Confidentiality Layer](#confidentiality-layer)
  - [Storage / Data](#storage--data)
  - [Governance & Token](#governance--token)
  - [Validator Operations & SRE](#validator-operations--sre)
  - [API / SDK](#api--sdk)
  - [Monitoring & Analytics](#monitoring--analytics)
  - [Testing & Benchmarks](#testing--benchmarks)
  - [DevOps & Deployment](#devops--deployment)
  - [Interoperability](#interoperability)
- [Performance Targets](#performance-targets)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [License](#license)
- [Alignment Notes](#alignment-notes)

---

## Key Features

- **Hybrid Consensus**: PoH → DPoS → aBFT for low latency and deterministic finality.
- **Post-Quantum by Default**: ML-KEM (CRYSTALS-Kyber) for KEM, ML-DSA (CRYSTALS-Dilithium) for signatures; SLH-DSA (SPHINCS+) as stateless alternative.
- **Unified Contracts**: Type-3 zkEVM, Chaincode, and DNA/WASM in one network.
- **CVM Atomicity**: Cross-VM atomic calls with ordered messaging and all-or-nothing commits.
- **Confidentiality**: Commitment/nullifier model + zk proofs for shielded transfers and private compute.
- **Tiered Storage**: LMDB (canonical), Redis (hot cache), MongoDB (archival/analytics), SQLite (edge/light).
- **Observability & SRE**: Prometheus/Grafana dashboards, OpenTelemetry traces, alerting & runbooks.

## Architecture at a Glance

- **Time-to-Finality (upper-bound)**: T_poh + R·Δ + T_exec
- **Throughput (approx.)**: (B − ε)/ŝ · 1/T_block
- **Bandwidth per validator** (example @100k TPS, 250B/tx): ≈ 23.8 MB/s (≈190 Mbps)

## Modules

### Consensus

- **Pipeline**: PoH (pre-ordering) → DPoS (election/proposal) → aBFT (finality).
- **Goals**: sub-second TTF, deterministic safety for f < ⌊n/3⌋.
- **Implementation Notes**: deterministic serialization; bounded timeouts; proposer schedule aligned with stake.

### Cryptography (PQC Baseline)

- **Key Exchange**: ML-KEM (CRYSTALS-Kyber)
- **Signatures**: ML-DSA (CRYSTALS-Dilithium); optional SLH-DSA (SPHINCS+) where stateless ops are preferred.
- **Roadmap**: hybrid ECDSA+Dilithium support for migration paths (if enabled by network policy).

### Networking / P2P

- Encrypted transport (TLS 1.3), rate limiting, partition handling under partial synchrony.
- Service layer: REST/gRPC for wallets, explorers, and bulk ingestion.

### Ledger / State

- Deterministic state machine; canonical state in LMDB.
- Snapshots/Checkpoints for fast sync; indexers for queries.

### Transactions & Mempool

- Typed envelopes with stable JSON fields.
- Validation: signatures, nonces, fee/gas checks; spam/DoS controls.
- Prioritization by fee and policy.

### Unified Smart-Contract Stack

- **Type-3 zkEVM**: Solidity/Vyper compatibility; zk proof verification on-chain.
- **Chaincode**: enterprise logic (Go/Java/Node) with auditable state hashes.
- **DNA/WASM**: resource-oriented semantics (ownership/linearity) for safety and performance.

### Cross-VM Messaging (CVM)

- Ordered message bus across zkEVM / DNA / Chaincode.
- Atomicity: multi-VM transactions commit or revert as a whole.
- Determinism: explicit ordering, capability checks, and gas/accounting rules.

### Confidentiality Layer

- Commitments & Nullifiers to hide values/ownership and prevent double-spend.
- zk Proofs (SNARK/STARK backends) to validate correctness without revealing inputs.
- Note: pairing-based SNARKs are not PQ-safe; STARK path available per network policy.

### Storage / Data

- **LMDB** (authoritative state), **Redis** (hot keys), **MongoDB** (archival/analytics), **SQLite** (light/edge).
- Data growth planning and pruning/archival policies for high-TPS workloads.

### Governance & Token

- **$DIAM** for fees, staking, governance, collateral.
- DPoS elections; proposal/threshold/quorum rules with timelock enactment.

### Validator Operations & SRE

- **Dashboards & Alerts**: uptime, missed blocks, peer count, CPU/memory, sync lag.
- **Runbooks**: upgrades, rollback, incident response; restart-invariance checks.

### API / SDK

- REST/gRPC for tx submission, status, bulk ops; versioned under `/api/v1`.
- SDKs (Go/JS/Python) for wallets, dApps, indexers.

### Monitoring & Analytics

- Prometheus/Grafana, OpenTelemetry; anomaly detection hooks.

### Testing & Benchmarks

- Throughput/latency suites with machine-readable outputs (e.g., `report.json`).
- Targets (example): ≥100k TPS, P99 ≤ 10 ms (environment-dependent).

### DevOps & Deployment

- Docker/Kubernetes manifests; deterministic genesis; expected-hash checks.
- CI: lint, build, test; CD for dev/test networks.

### Interoperability

- CVM intra-chain atomicity; bridges/light-client verifiers for extra-chain messaging.

## Performance Targets

- **Goal**: 100,000+ TPS with < 1 s finality (hardware and network dependent).
- Example derivations in whitepaper; validate with provided load-test suites and SRE playbooks.

## Repository Layout

```
/api/                    # REST/gRPC handlers, typed schemas
/app/                    # Application layer integration
/benchmarks/             # Load/perf suites
/cmd/                    # Node executables (e.g., realtestnet)
/common/                 # Shared utilities and types
/consensus/              # PoH, DPoS, aBFT implementation
/contracts/              # Smart contract interfaces
/crypto/                 # Kyber, Dilithium, SPHINCS+ integration
/fees/                   # Fee calculation and management
/ledger/                 # State machine, receipts
/lightnode/              # Light client implementation
/metrics/                # Performance metrics collection
/migration/              # Data migration utilities
/mobile/                 # Mobile SDK support
/monitoring/             # Dashboards, alerts, OTel
/network/                # P2P, peer mgmt, envelopes
/sdk/                    # Client SDKs
/security/               # Security framework and validation
/storage/                # LMDB/Redis/Mongo/SQLite adapters
/tests/                  # Integration and unit tests
/transaction/            # Transaction processing and validation
/types/                  # Core type definitions
/vm/                     # zkEVM, DNA/WASM, Chaincode
/wallet/                 # Wallet management and key derivation
/docker-deployment/      # Compose/manifests, configs
```

## Getting Started

> **Note**: This repo is a mirror; some scripts/manifests may point to private resources. Follow the whitepaper and docs for public equivalents.

```bash
# Clone the mirror
git clone https://github.com/diamante-io/diamante-quant-v2.git
cd diamante-quant-v2

# Install dependencies
go mod download

# Build (example)
go build -o realtestnet ./cmd/realtestnet

# Run a local test stack (example)
cd docker-deployment
docker-compose -f docker-compose.yml up -d

# Verify status (node ports in compose)
curl http://localhost:8090/api/v1/status

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./benchmarks/...
```

### Prerequisites

- **Go**: 1.21+ required
- **Docker**: For containerized deployment
- **MongoDB**: For archival storage (optional)
- **Redis**: For caching layer (optional)
- **Ubuntu**: 20.04+ recommended for production

### Configuration

Configuration is managed through environment variables. See `.env.example` for a template:

```bash
MONGO_URI=mongodb://localhost:27017
REDIS_URL=redis://localhost:6379
API_PORT=8090
P2P_PORT=30303
METRICS_PORT=9090
```

## Contributing

Contributions are welcome via issues and PRs. For large changes, please open an issue first to align on scope and architecture.

- **Code Style**: Follow Go conventions and run `go fmt`
- **Testing**: Add tests for new features
- **Documentation**: Update relevant docs
- **Security disclosures**: Please report privately per [SECURITY.md](SECURITY.md)

## License

Licensed under the [MIT License](LICENSE).

---

**Built with ❤️ by the Diamante team**

*Empowering the future of decentralized technology with quantum-safe, enterprise-grade blockchain infrastructure.*
