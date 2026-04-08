//! Concurrency correctness tests.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_multilock::{MultiLock2, MultiLock3};

#[tokio::test]
async fn concurrent_increments() {
    let mutex_a = Arc::new(Mutex::new(0u64));
    let mutex_b = Arc::new(Mutex::new(0u64));

    let mut handles = Vec::new();

    for _ in 0..10 {
        let ma = mutex_a.clone();
        let mb = mutex_b.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut a, mut b) = MultiLock2::new(&ma, &mb).await;
                *a += 1;
                *b += 1;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 1000);
    assert_eq!(*b, 1000);
}

#[tokio::test]
async fn concurrent_transfers() {
    // Transfer value between two accounts atomically
    let account_a = Arc::new(Mutex::new(1000i64));
    let account_b = Arc::new(Mutex::new(1000i64));

    let mut handles = Vec::new();

    // Tasks transferring A -> B
    for _ in 0..5 {
        let a = account_a.clone();
        let b = account_b.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;

                if *ga >= 10 {
                    *ga -= 10;
                    *gb += 10;
                }
            }
        }));
    }

    // Tasks transferring B -> A
    for _ in 0..5 {
        let a = account_a.clone();
        let b = account_b.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;

                if *gb >= 10 {
                    *gb -= 10;
                    *ga += 10;
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (a, b) = MultiLock2::new(&account_a, &account_b).await;

    // Total should be conserved
    assert_eq!(*a + *b, 2000);
}

#[tokio::test]
async fn concurrent_three_way_rotation() {
    let mutex_a = Arc::new(Mutex::new(100u64));
    let mutex_b = Arc::new(Mutex::new(200u64));
    let mutex_c = Arc::new(Mutex::new(300u64));

    let mut handles = Vec::new();

    // Rotate values: a -> b -> c -> a
    for _ in 0..10 {
        let a = mutex_a.clone();
        let b = mutex_b.clone();
        let c = mutex_c.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                let (mut ga, mut gb, mut gc) = MultiLock3::new(&a, &b, &c).await;

                let tmp = *ga;
                *ga = *gc;
                *gc = *gb;
                *gb = tmp;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (a, b, c) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    // Total should be conserved
    assert_eq!(*a + *b + *c, 600);
}

#[tokio::test]
async fn concurrent_append_to_vecs() {
    let vec_a = Arc::new(Mutex::new(Vec::<u32>::new()));
    let vec_b = Arc::new(Mutex::new(Vec::<u32>::new()));

    let mut handles = Vec::new();

    for task_id in 0..10u32 {
        let a = vec_a.clone();
        let b = vec_b.clone();

        handles.push(tokio::spawn(async move {
            for i in 0..100u32 {
                let (mut va, mut vb) = MultiLock2::new(&a, &b).await;
                va.push(task_id * 1000 + i);
                vb.push(task_id * 1000 + i);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (a, b) = MultiLock2::new(&vec_a, &vec_b).await;

    assert_eq!(a.len(), 1000);
    assert_eq!(b.len(), 1000);
}

#[tokio::test]
async fn concurrent_readers_and_writers() {
    let data = Arc::new(Mutex::new(0u64));
    let version = Arc::new(Mutex::new(0u64));

    let mut handles = Vec::new();

    // Writers
    for _ in 0..5 {
        let d = data.clone();
        let v = version.clone();

        handles.push(tokio::spawn(async move {
            for i in 0..100u64 {
                let (mut gd, mut gv) = MultiLock2::new(&d, &v).await;
                *gd = i;
                *gv += 1;
            }
        }));
    }

    // Readers that verify consistency
    for _ in 0..5 {
        let d = data.clone();
        let v = version.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (gd, gv) = MultiLock2::new(&d, &v).await;

                // Just read both atomically - no torn reads
                let _ = (*gd, *gv);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (_, v) = MultiLock2::new(&data, &version).await;

    // 5 writers * 100 iterations
    assert_eq!(*v, 500);
}

#[tokio::test]
async fn spawn_many_short_lived_tasks() {
    let mutex_a = Arc::new(Mutex::new(0u32));
    let mutex_b = Arc::new(Mutex::new(0u32));

    let mut handles = Vec::new();

    for _ in 0..100 {
        let a = mutex_a.clone();
        let b = mutex_b.clone();

        handles.push(tokio::spawn(async move {
            let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;
            *ga += 1;
            *gb += 2;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 100);
    assert_eq!(*b, 200);
}

#[tokio::test]
async fn interleaved_different_lock_sets() {
    let m1 = Arc::new(Mutex::new(0u32));
    let m2 = Arc::new(Mutex::new(0u32));
    let m3 = Arc::new(Mutex::new(0u32));

    let mut handles = Vec::new();

    // Tasks using (m1, m2)
    for _ in 0..5 {
        let a = m1.clone();
        let b = m2.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;
                *ga += 1;
                *gb += 1;
            }
        }));
    }

    // Tasks using (m2, m3)
    for _ in 0..5 {
        let a = m2.clone();
        let b = m3.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;
                *ga += 1;
                *gb += 1;
            }
        }));
    }

    // Tasks using (m1, m3)
    for _ in 0..5 {
        let a = m1.clone();
        let b = m3.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let (mut ga, mut gb) = MultiLock2::new(&a, &b).await;
                *ga += 1;
                *gb += 1;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let (g1, g2, g3) = MultiLock3::new(&m1, &m2, &m3).await;

    // m1: 5*100 from (m1,m2) + 5*100 from (m1,m3) = 1000
    // m2: 5*100 from (m1,m2) + 5*100 from (m2,m3) = 1000
    // m3: 5*100 from (m2,m3) + 5*100 from (m1,m3) = 1000
    assert_eq!(*g1, 1000);
    assert_eq!(*g2, 1000);
    assert_eq!(*g3, 1000);
}
