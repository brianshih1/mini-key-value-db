# Concurrency Manager

Concurrency Manager is composed of a latch manager, a lock table, and a txnWaitQueue. Now that we better understand these individual components, we can finally look at how the Concurrency Manager’s API is implemented.

### Concurrency Manager API

The API is made up of two methods: sequence_req and finish_req.

**Sequence_req: (request) → Result<Guard, SequenceReqError>**

`Sequence_req` provides isolation for the request by acquiring latches and waiting for conflicting locks to be released. Once `Sequence_req` returns a `Guard`, the request is guaranteed isolation until the guard is released.

**Finish_req: (guard) → ()**

`Finish_req` is called when the request has finished executing. It releases the request from the components of the concurrency manager and allows blocked requests to proceed.

### Implementation

**Sequence_req**

Sequence_req first [figures out](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L45) the keys it needs to acquire latches and locks for. It then creates a [loop](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L48). Each time it loops, it performs the following:

- it [acquires the latches](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L49)
- it calls the lock table’s `scan_and_enqueue` method to see if there is a conflicting lock for one of the request’s keys
  - if [there is a conflicting lock](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L51), the thread will [release the latches and wait until the lock is released](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L53). After waiting, it will have become a `reservation` for the lock. In that case, it is free to re-acquire latches and rescan the lock table in the next iteration of the loop.
  - if there isn’t a conflicting lock, the function [stops looping and returns](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L64).

**Finish_req**

Finish_req simply [releases the latches](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L76) and [dequeues](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/concurrency/concurrency_manager.rs#L77) acquired locks from the lock table.

### CockroachDB’s Implementation

The core idea behind CockroachDB’s [SequenceReq](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L185) implementation is similar to my implementation. The difference is that it has different modes, such as [OptimisticEval](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_control.go#L365) and [PessimisticEval](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_control.go#L363). If the mode is OptimisticEval, it [calls](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L270) [AcquireOptimistic](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_control.go#LL498C2-L501C62) which does not wait for conflicting latches to be released. It needs to be followed with CheckOptimisticNoConflicts to ensure correctness.

My implementation is more similar to the pessimistic evaluation approach, which [acquires latches](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L276).  CockroachDB’s SequenceReq then [calls ScanAndEnqueue](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L320) to find conflicting locks. If a conflicting lock [is found](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L326), [WaitOn](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/concurrency/concurrency_manager.go#L330) is called, which waits until the lock is released or pushes the transaction if it times out.
