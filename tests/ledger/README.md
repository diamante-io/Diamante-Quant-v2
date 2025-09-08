# Ledger Module Tests

This directory contains comprehensive tests for the Diamante ledger module, which is critical for blockchain state management and EVM execution.

## Test Coverage

### Unit Tests (`unit/`)

1. **common_ledger_adapter_test.go**
   - Tests for the CommonLedgerAdapter that bridges the API ledger with common.LedgerAPI
   - Account management (creation, updates, balance operations)
   - Transaction processing and validation
   - Block operations (commit, retrieval, range queries)
   - Smart contract lifecycle (deploy, execute, update, remove)
   - Snapshot and restore functionality
   - Cache effectiveness testing
   - Concurrent operation safety
   - Error handling and edge cases

2. **evm_executor_test.go**
   - Tests for EVM transaction execution
   - Contract deployment and calls
   - Balance and nonce management
   - Gas calculations and limits
   - Transaction validation
   - Error propagation
   - Concurrent execution safety

3. **state_db_test.go**
   - Tests for the Ethereum StateDB implementation
   - Account operations (create, exist, empty checks)
   - Balance operations (get, set, add, sub)
   - Nonce management
   - Code operations (set, get, hash)
   - Storage operations (state, committed state)
   - Self-destruct functionality
   - Snapshot and revert mechanisms
   - Access list management
   - Transient storage (EIP-1153)
   - Log management
   - Refund tracking
   - State commitment

4. **evm_runtime_test.go**
   - Tests for EVM runtime components
   - Event management and filtering
   - Contract verification
   - State proof generation and verification
   - Precompiled contracts
   - Runtime types and configurations

5. **ledger_edge_cases_test.go**
   - Edge case testing for error conditions
   - Nil pointer handling
   - Empty/invalid input validation
   - Overflow/underflow conditions
   - Interface casting failures
   - Timeout handling
   - Panic recovery
   - Resource exhaustion scenarios

### Integration Tests (`integration/`)

1. **ledger_integration_test.go**
   - End-to-end transaction flow testing
   - EVM contract deployment and execution scenarios
   - Block creation and retrieval workflows
   - Smart contract lifecycle integration
   - Concurrent transaction processing
   - State consistency verification
   - Complex multi-step scenarios

### Test Utilities (`testutil/`)

1. **test_helpers.go**
   - Common test setup and teardown
   - Mock implementations
   - Test data generators
   - Assertion helpers
   - Benchmarking utilities

## Known Issues

The tests have identified some compatibility issues with the current codebase:

1. **Cache Interface Mismatch**: The cache expects `*types.CacheValue` but the ledger adapter tries to store different types directly. This needs to be addressed by either:
   - Wrapping values in CacheValue structs
   - Creating a more flexible cache interface
   - Using type-specific caches

2. **EVM Type Changes**: The StateDB returns `*uint256.Int` instead of `*big.Int` for balances, which is a change in newer go-ethereum versions. The EVMExecutor needs to be updated to handle this.

## Running the Tests

```bash
# Run all ledger tests
go test ./tests/ledger/...

# Run unit tests only
go test ./tests/ledger/unit

# Run integration tests only
go test ./tests/ledger/integration

# Run specific test
go test -v ./tests/ledger/unit -run TestCommonLedgerAdapter

# Run with race detection
go test -race ./tests/ledger/...

# Run benchmarks
go test -bench=. ./tests/ledger/unit

# Generate coverage report
go test -coverprofile=coverage.out ./tests/ledger/...
go tool cover -html=coverage.out -o coverage.html
```

## Test Organization

Tests are organized following these principles:

1. **Unit tests** focus on individual components in isolation
2. **Integration tests** verify interactions between components
3. **Edge case tests** ensure robust error handling
4. **Benchmark tests** measure performance characteristics
5. **Concurrent tests** verify thread safety

## Adding New Tests

When adding new tests:

1. Use the test helpers in `testutil/` for common setup
2. Follow the existing naming conventions
3. Include both positive and negative test cases
4. Test edge cases and error conditions
5. Add benchmarks for performance-critical paths
6. Ensure tests are deterministic and repeatable

## Coverage Goals

The target coverage for the ledger module is:
- Unit test coverage: 80%+
- Integration test coverage: 60%+
- Critical path coverage: 100%

## Future Improvements

1. Add more EVM-specific test cases for complex contracts
2. Implement property-based testing for state transitions
3. Add fuzzing for transaction validation
4. Create performance regression tests
5. Add more integration tests with other modules