//! Deadlock prevention and resistance tests.

#![allow(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::unwrap_used
)]

use std::time::Duration;
use std::{ptr, sync::Arc};

use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_multilock::{MultiLock2, MultiLock3};

/// Two tasks acquiring two locks in opposite order.
/// Would deadlock with naive lock acquisition.
#[tokio::test]
async fn two_locks_opposite_order() {
    let mutex_a = Arc::new(Mutex::new(1u32));
    let mutex_b = Arc::new(Mutex::new(2u32));

    let ma1 = mutex_a.clone();
    let mb1 = mutex_b.clone();
    let ma2 = mutex_a.clone();
    let mb2 = mutex_b.clone();

    let t1 = tokio::spawn(async move {
        for _ in 0..100 {
            let (a, b) = MultiLock2::new(&ma1, &mb1).await;
            assert_eq!(*a + *b, 3);
        }
    });

    let t2 = tokio::spawn(async move {
        for _ in 0..100 {
            // Opposite order
            let (b, a) = MultiLock2::new(&mb2, &ma2).await;
            assert_eq!(*a + *b, 3);
        }
    });

    let result = timeout(Duration::from_secs(5), async {
        t1.await.unwrap();
        t2.await.unwrap();
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Three locks with all permutations across tasks.
#[tokio::test]
async fn three_locks_all_permutations() {
    let mutex_a = Arc::new(Mutex::new(1u32));
    let mutex_b = Arc::new(Mutex::new(2u32));
    let mutex_c = Arc::new(Mutex::new(3u32));

    let handles: Vec<_> = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ]
    .into_iter()
    .map(|order| {
        let ma = mutex_a.clone();
        let mb = mutex_b.clone();
        let mc = mutex_c.clone();

        tokio::spawn(async move {
            let mutexes = [&ma, &mb, &mc];

            for _ in 0..50 {
                let (g0, g1, g2) =
                    MultiLock3::new(mutexes[order[0]], mutexes[order[1]], mutexes[order[2]]).await;

                assert_eq!(*g0 + *g1 + *g2, 6);
            }
        })
    })
    .collect();

    let result = timeout(Duration::from_secs(5), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Four tasks each holding one lock and wanting the next (circular).
#[tokio::test]
async fn circular_dependency_four_locks() {
    let m0 = Arc::new(Mutex::new(0u32));
    let m1 = Arc::new(Mutex::new(1u32));
    let m2 = Arc::new(Mutex::new(2u32));
    let m3 = Arc::new(Mutex::new(3u32));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let locks = [m0.clone(), m1.clone(), m2.clone(), m3.clone()];

            tokio::spawn(async move {
                for _ in 0..50 {
                    let first = &locks[i];
                    let second = &locks[(i + 1) % 4];

                    let (a, b) = MultiLock2::new(first, second).await;
                    assert_eq!((*a + *b) % 4, (2 * i + 1) as u32 % 4);
                }
            })
        })
        .collect();

    let result = timeout(Duration::from_secs(5), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Many tasks contending for overlapping subsets of locks.
#[tokio::test]
async fn overlapping_lock_subsets() {
    let locks: Vec<_> = (0..6).map(|i| Arc::new(Mutex::new(i))).collect();

    let handles: Vec<_> = (0..10)
        .map(|task_id| {
            let l0 = locks[task_id % 6].clone();
            let l1 = locks[(task_id + 1) % 6].clone();
            let l2 = locks[(task_id + 2) % 6].clone();

            tokio::spawn(async move {
                for _ in 0..30 {
                    let (a, b, c) = MultiLock3::new(&l0, &l1, &l2).await;
                    let _ = *a + *b + *c;
                }
            })
        })
        .collect();

    let result = timeout(Duration::from_secs(5), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Dining philosophers with 5 philosophers.
#[tokio::test]
async fn dining_philosophers() {
    let forks: Vec<_> = (0..5).map(|i| Arc::new(Mutex::new(i))).collect();

    let handles: Vec<_> = (0..5)
        .map(|philosopher| {
            let left = forks[philosopher].clone();
            let right = forks[(philosopher + 1) % 5].clone();

            tokio::spawn(async move {
                for _ in 0..50 {
                    let (_l, _r) = MultiLock2::new(&left, &right).await;
                    let () = tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    let result = timeout(Duration::from_secs(5), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

#[tokio::test]
#[should_panic(expected = "MultiLock received the same Mutex more than once")]
async fn duplicate_mutex_panics() {
    let mutex = Mutex::new(1u32);
    let _ = MultiLock2::new(&mutex, &mutex).await;
}

#[tokio::test]
async fn bad_lock_scenario() {
    let mutexes = (Mutex::new(()), Mutex::new(()));

    let (gtr, lsr) = {
        if ptr::from_ref(&mutexes.0) < ptr::from_ref(&mutexes.1) {
            (mutexes.1, mutexes.0)
        } else {
            (mutexes.0, mutexes.1)
        }
    };

    let gtr = Arc::new(gtr);
    let lsr = Arc::new(lsr);

    let bad_task = tokio::spawn({
        let gtr = gtr.clone();
        let lsr = lsr.clone();

        async move {
            let gtr_g = gtr.lock().await;

            sleep(Duration::from_secs(5)).await;

            let lsr_g = lsr.lock().await;

            sleep(Duration::from_secs(5)).await;

            *gtr_g;
            *lsr_g;
        }
    });

    let polite_task = tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;

        let (gtr_g, lsr_g) = MultiLock2::new(&gtr, &lsr).await;

        *gtr_g;
        *lsr_g;
    });

    let result = timeout(Duration::from_secs(20), async {
        polite_task.await.expect("should be Ok");
        bad_task.await.expect("should be Ok");
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Multiple "bad" tasks acquiring locks in wrong order while `MultiLock` tasks compete.
#[tokio::test]
async fn many_bad_actors() {
    let locks: Vec<_> = (0..4).map(|i| Arc::new(Mutex::new(i))).collect();

    let mut handles = Vec::new();

    // 2 bad actors each holding lock[i] then wanting lock[(i+2)%4] (non-circular)
    for i in 0..2 {
        let first = locks[i].clone();
        let second = locks[(i + 2) % 4].clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                let g1 = first.lock().await;
                sleep(Duration::from_millis(10)).await;
                let g2 = second.lock().await;
                sleep(Duration::from_millis(5)).await;
                drop(g2);
                drop(g1);
            }
        }));
    }

    // 4 polite tasks using MultiLock
    for i in 0..4 {
        let first = locks[i].clone();
        let second = locks[(i + 1) % 4].clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let (_g1, _g2) = MultiLock2::new(&first, &second).await;
                let () = tokio::task::yield_now().await;
            }
        }));
    }

    let result = timeout(Duration::from_secs(30), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// High contention: many tasks, few locks, mixed cooperating and non-cooperating.
#[tokio::test]
async fn high_contention_mixed() {
    let lock_a = Arc::new(Mutex::new(0u32));
    let lock_b = Arc::new(Mutex::new(0u32));

    let mut handles = Vec::new();

    // 3 tasks using raw locks in wrong order (B then A)
    for _ in 0..3 {
        let a = lock_a.clone();
        let b = lock_b.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let gb = b.lock().await;
                let () = tokio::task::yield_now().await;
                let ga = a.lock().await;
                let _ = *ga + *gb;
                drop(ga);
                drop(gb);
            }
        }));
    }

    // 5 tasks using MultiLock (will use correct order internally)
    for _ in 0..5 {
        let a = lock_a.clone();
        let b = lock_b.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let (ga, gb) = MultiLock2::new(&a, &b).await;
                let _ = *ga + *gb;
            }
        }));
    }

    let result = timeout(Duration::from_secs(30), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Stress test: rapidly acquire/release with many tasks.
#[tokio::test]
async fn stress_rapid_acquire_release() {
    let locks: Vec<_> = (0..3).map(|i| Arc::new(Mutex::new(i))).collect();

    let mut handles = Vec::new();

    // 20 tasks all competing for the same 3 locks
    for task_id in 0..20 {
        let l0 = locks[0].clone();
        let l1 = locks[1].clone();
        let l2 = locks[2].clone();

        handles.push(tokio::spawn(async move {
            for iter in 0..50 {
                // Vary the order based on task_id and iteration to create chaos
                let (a, b, c) = match (task_id + iter) % 6 {
                    0 => MultiLock3::new(&l0, &l1, &l2).await,
                    1 => MultiLock3::new(&l0, &l2, &l1).await,
                    2 => MultiLock3::new(&l1, &l0, &l2).await,
                    3 => MultiLock3::new(&l1, &l2, &l0).await,
                    4 => MultiLock3::new(&l2, &l0, &l1).await,
                    _ => MultiLock3::new(&l2, &l1, &l0).await,
                };
                let _ = *a + *b + *c;
            }
        }));
    }

    let result = timeout(Duration::from_secs(30), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Bad actors holding locks for extended periods while `MultiLock` backs off.
#[tokio::test]
async fn long_hold_backoff() {
    let lock_a = Arc::new(Mutex::new(()));
    let lock_b = Arc::new(Mutex::new(()));

    // Sort to know the correct order
    let (first, second) = if ptr::from_ref(&*lock_a) < ptr::from_ref(&*lock_b) {
        (lock_a.clone(), lock_b.clone())
    } else {
        (lock_b.clone(), lock_a.clone())
    };

    // Bad actor: holds second lock (wrong order) for a long time
    let bad = {
        let second = second.clone();
        let first = first.clone();

        tokio::spawn(async move {
            for _ in 0..3 {
                let _g = second.lock().await;
                sleep(Duration::from_millis(200)).await;
                let _g2 = first.lock().await;
                sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // MultiLock task: must back off multiple times
    let polite = {
        let first = first.clone();
        let second = second.clone();

        tokio::spawn(async move {
            for _ in 0..5 {
                let (_g1, _g2) = MultiLock2::new(&first, &second).await;
                let () = tokio::task::yield_now().await;
            }
        })
    };

    let result = timeout(Duration::from_secs(10), async {
        polite.await.unwrap();
        bad.await.unwrap();
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}

/// Chain of dependencies: task N holds lock N, wants lock N+1.
#[tokio::test]
async fn chain_dependency() {
    const CHAIN_LEN: usize = 8;
    let locks: Vec<_> = (0..CHAIN_LEN).map(|i| Arc::new(Mutex::new(i))).collect();

    let mut handles = Vec::new();

    // Each task holds lock[i] and wants lock[i+1] - classic chain deadlock pattern
    for i in 0..CHAIN_LEN - 1 {
        let curr = locks[i].clone();
        let next = locks[i + 1].clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                // Raw locks in chain order (could deadlock without backoff)
                let _g1 = curr.lock().await;
                let () = tokio::task::yield_now().await;
                let _g2 = next.lock().await;
            }
        }));
    }

    // MultiLock tasks crossing the chain
    for i in 0..CHAIN_LEN - 2 {
        let l1 = locks[i].clone();
        let l2 = locks[i + 2].clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                let (_g1, _g2) = MultiLock2::new(&l1, &l2).await;
            }
        }));
    }

    let result = timeout(Duration::from_secs(30), async {
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "Deadlock detected");
}
