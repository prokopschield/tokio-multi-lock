//! Edge case and boundary condition tests.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_multi_lock::{MultiLock2, MultiLock3};

#[tokio::test]
async fn zero_sized_types() {
    let mutex_a = Mutex::new(());
    let mutex_b = Mutex::new(());

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, ());
    assert_eq!(*b, ());
}

#[tokio::test]
async fn large_values() {
    let mutex_a = Mutex::new([0u8; 1024]);
    let mutex_b = Mutex::new([0u8; 4096]);

    let (mut a, mut b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    a[0] = 1;
    a[1023] = 255;
    b[0] = 2;
    b[4095] = 254;

    assert_eq!(a[0], 1);
    assert_eq!(a[1023], 255);
    assert_eq!(b[0], 2);
    assert_eq!(b[4095], 254);
}

#[tokio::test]
async fn arc_wrapped_mutexes() {
    let mutex_a = Arc::new(Mutex::new(1u32));
    let mutex_b = Arc::new(Mutex::new(2u32));

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 1);
    assert_eq!(*b, 2);
}

#[tokio::test]
async fn box_wrapped_mutexes() {
    let mutex_a = Box::new(Mutex::new(1u32));
    let mutex_b = Box::new(Mutex::new(2u32));

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 1);
    assert_eq!(*b, 2);
}

#[tokio::test]
async fn immediately_available_locks() {
    // No contention - locks should be acquired on first poll
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a + *b, 3);
}

#[tokio::test]
async fn many_sequential_lock_cycles() {
    let mutex_a = Mutex::new(0u64);
    let mutex_b = Mutex::new(0u64);

    for _ in 0..1000 {
        let (mut a, mut b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        *a += 1;
        *b += 1;
    }

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 1000);
    assert_eq!(*b, 1000);
}

#[tokio::test]
async fn partial_drop_then_reacquire() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let mutex_c = Mutex::new(3u32);

    let (a, b, c) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    assert_eq!(*a + *b + *c, 6);

    // Drop only some guards
    drop(a);
    drop(c);

    // b still held
    assert_eq!(*b, 2);

    drop(b);

    // Reacquire all
    let (a, b, c) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    assert_eq!(*a + *b + *c, 6);
}

#[tokio::test]
async fn reuse_same_mutexes_different_multilock_instances() {
    let mutex_a = Mutex::new(0u32);
    let mutex_b = Mutex::new(0u32);

    // First MultiLock instance
    {
        let (mut a, mut b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        *a = 10;
        *b = 20;
    }

    // Second MultiLock instance
    {
        let (mut a, mut b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        *a += 5;
        *b += 5;
    }

    // Third MultiLock instance
    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*a, 15);
    assert_eq!(*b, 25);
}

#[tokio::test]
#[should_panic(expected = "MultiLock received the same Mutex more than once")]
async fn duplicate_mutex_two() {
    let mutex = Mutex::new(1u32);
    let _ = MultiLock2::new(&mutex, &mutex).await;
}

#[tokio::test]
#[should_panic(expected = "MultiLock received the same Mutex more than once")]
async fn duplicate_mutex_three_first_two() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let _ = MultiLock3::new(&mutex_a, &mutex_a, &mutex_b).await;
}

#[tokio::test]
#[should_panic(expected = "MultiLock received the same Mutex more than once")]
async fn duplicate_mutex_three_last_two() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let _ = MultiLock3::new(&mutex_a, &mutex_b, &mutex_b).await;
}

#[tokio::test]
#[should_panic(expected = "MultiLock received the same Mutex more than once")]
async fn duplicate_mutex_three_first_and_last() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let _ = MultiLock3::new(&mutex_a, &mutex_b, &mutex_a).await;
}

#[tokio::test]
async fn string_mutation() {
    let mutex_a = Mutex::new(String::new());
    let mutex_b = Mutex::new(String::new());

    {
        let (mut a, mut b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        a.push_str("hello");
        b.push_str("world");
    }

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(&*a, "hello");
    assert_eq!(&*b, "world");
}

#[tokio::test]
async fn vec_mutation() {
    let mutex = Mutex::new(Vec::<i32>::new());
    let mutex2 = Mutex::new(0i32);

    for i in 0..100 {
        let (mut vec, mut count) = MultiLock2::new(&mutex, &mutex2).await;
        vec.push(i);
        *count += 1;
    }

    let (vec, count) = MultiLock2::new(&mutex, &mutex2).await;

    assert_eq!(vec.len(), 100);
    assert_eq!(*count, 100);
    assert_eq!(vec[0], 0);
    assert_eq!(vec[99], 99);
}
