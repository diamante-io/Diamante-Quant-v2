# Concurrency Improvements

This document outlines the plan for improving concurrency handling in the Diamnet consensus engine.

## Current State

The current implementation uses several mutexes to protect shared state:

### HybridConsensus

- `stateMu`: Protects the running state
- `blockHeightMu`: Protects the last block height
- `lastBlockHashMu`: Protects the last block hash
- `finalizedEventsMu`: Protects the finalized events map
- `pendingEventsMu`: Protects the pending events slice
- `checkpointsMu`: Protects the checkpoints map
- `errorCountMu`: Protects the error count map
- `eventProcessingMu`: Serializes event processing

### EventFlowManager

- `mu`: Protects all event-related state (pendingEvents, finalizedEvents, etc.)
- `metricsMu`: Protects metrics-related state

### ValidatorManager

- `validatorsMu`: Protects validator-related state
- `stakeMu`: Protects stake-related state
- `performanceMu`: Protects performance-related state
- `rewardMu`: Protects reward-related state

## Issues

1. **Mutex Granularity**: Some mutexes protect too much state, leading to unnecessary contention.
2. **Lock Ordering**: There's no clear documentation or consistent ordering of lock acquisition, which could lead to deadlocks.
3. **Lock Contention**: Some methods hold locks for longer than necessary, especially when making external calls.
4. **Deadlock Detection**: There's no deadlock detection mechanism in place.

## Improvements

### 1. Establish Lock Ordering

To prevent deadlocks, we need to establish a consistent lock ordering policy. When multiple locks need to be acquired, they should always be acquired in the same order.

#### Lock Ordering Policy

1. `stateMu`
2. `blockHeightMu`
3. `lastBlockHashMu`
4. `finalizedEventsMu`
5. `pendingEventsMu`
6. `checkpointsMu`
7. `errorCountMu`
8. `eventProcessingMu`
9. `validatorsMu`
10. `stakeMu`
11. `performanceMu`
12. `rewardMu`
13. `metricsMu`

### 2. Reduce Lock Contention

#### HybridConsensus

1. **ProcessBlock**: This method holds locks for too long. We should release locks before making external calls.
2. **FinalizeEvent**: This method acquires `finalizedEventsMu` but then makes an external call to `validatorManager.RewardEventFinalization`. We should release the lock before making this call.

#### EventFlowManager

1. **processPendingEvents**: This method acquires `mu` to get a copy of pending events, but then releases it before processing. This is good practice and should be continued.
2. **handleFinalizedEvent**: This method holds `mu` for the entire duration, including when making an external call to `hc.validatorManager.RewardEventFinalization`. We should release the lock before making this call.

#### ValidatorManager

1. **AddValidator**: This method acquires both `validatorsMu` and `stakeMu`, which could lead to deadlocks if other methods acquire these locks in a different order. We should ensure consistent lock ordering.
2. **UpdateStake**: Similar to `AddValidator`, this method acquires both `validatorsMu` and `stakeMu`.

### 3. Add Deadlock Detection

We'll add a simple deadlock detection mechanism that logs a warning if a lock is held for too long. This will help identify potential deadlocks during development and testing.

#### Implementation

1. Add a `lockTimeout` constant to each component.
2. Modify lock acquisition to use a timeout.
3. Log a warning if a lock acquisition times out.

### 4. Use More Fine-Grained Locks

In some cases, we can use more fine-grained locks to reduce contention. For example, in `EventFlowManager`, we could use separate locks for `pendingEvents`, `finalizedEvents`, and `eventsByHeight`.

#### Implementation

1. Replace `mu` in `EventFlowManager` with separate locks for different state variables.
2. Update methods to acquire only the locks they need.

### 5. Use Read-Write Locks More Effectively

In many cases, we're using read-write locks (`sync.RWMutex`) but not taking full advantage of them. We should ensure that methods that only read state acquire read locks, not write locks.

#### Implementation

1. Review all methods that acquire locks and ensure they're using the appropriate lock type (read or write).
2. Update methods to use read locks when they only read state.

## Implementation Plan

1. **Phase 1**: Establish lock ordering and document it.
2. **Phase 2**: Reduce lock contention by releasing locks before making external calls.
3. **Phase 3**: Add deadlock detection.
4. **Phase 4**: Use more fine-grained locks.
5. **Phase 5**: Use read-write locks more effectively.

## Success Criteria

1. No deadlocks occur during normal operation or testing.
2. Lock contention is reduced, leading to better performance.
3. The code is more maintainable and easier to reason about.
