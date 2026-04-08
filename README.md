# tokio-multi-lock

Safely lock multiple Tokio mutexes at once.

## The problem

When you need to lock two or more mutexes, the order matters. If task A locks mutex 1 then waits for mutex 2, while task B locks mutex 2 then waits for mutex 1, neither can proceed. This is a deadlock.

## The solution

`MultiLock` acquires all your mutexes in a consistent order (by memory address), so deadlocks between `MultiLock` users cannot happen. If it detects potential deadlock with external code, it backs off and retries.

```rust
use tokio::sync::Mutex;
use tokio_multilock::MultiLock2;

let wallet = Mutex::new(100u32);
let bank = Mutex::new(1000u32);

// Lock both at once - order of arguments doesn't affect safety
let (mut wallet, mut bank) = MultiLock2::new(&wallet, &bank).await;

// Transfer funds atomically
*wallet += 50;
*bank -= 50;
```

You get the guards back in the same order you passed the mutexes.

## Different types

Each mutex can hold a different type:

```rust
use tokio::sync::Mutex;
use tokio_multilock::MultiLock3;

let count = Mutex::new(0u64);
let log = Mutex::new(Vec::<String>::new());
let enabled = Mutex::new(true);

let (mut count, mut log, enabled) = MultiLock3::new(&count, &log, &enabled).await;

if *enabled {
    *count += 1;
    log.push("incremented".into());
}
```

Variants from `MultiLock2` up to `MultiLock26` are available.

## How it works

1. Mutexes are sorted by memory address and locked in that order
2. Once the first lock is acquired, a timeout starts
3. If the timeout fires before all locks are acquired, everything is released, we wait, then retry
4. The timeout starts at 100μs and doubles after each retry, up to 100ms

This makes the implementation deadlock-resistant rather than deadlock-free. If external code holds a lock forever, `MultiLock` will keep retrying.

## Requirements

Your Tokio runtime needs timers enabled. This is automatic with `#[tokio::main]` or `#[tokio::test]`.

## Panics

Passing the same mutex twice to `MultiLock::new()` will panic.

## License

Licensed under the [GNU Lesser General Public License v3.0 or later](https://www.gnu.org/licenses/lgpl-3.0.html).
