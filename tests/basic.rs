//! Basic functionality tests.

use tokio::sync::Mutex;
use tokio_multi_lock::{MultiLock2, MultiLock3, MultiLock4};

#[tokio::test]
async fn acquire_two_locks() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);

    let (guard_a, guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*guard_a, 1);
    assert_eq!(*guard_b, 2);
}

#[tokio::test]
async fn acquire_three_locks() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let mutex_c = Mutex::new(3u32);

    let (guard_a, guard_b, guard_c) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    assert_eq!(*guard_a, 1);
    assert_eq!(*guard_b, 2);
    assert_eq!(*guard_c, 3);
}

#[tokio::test]
async fn acquire_four_locks() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);
    let mutex_c = Mutex::new(3u32);
    let mutex_d = Mutex::new(4u32);

    let (a, b, c, d) = MultiLock4::new(&mutex_a, &mutex_b, &mutex_c, &mutex_d).await;

    assert_eq!(*a + *b + *c + *d, 10);
}

#[tokio::test]
async fn mutate_through_guards() {
    let mutex_a = Mutex::new(0u32);
    let mutex_b = Mutex::new(0u32);

    {
        let (mut guard_a, mut guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        *guard_a = 10;
        *guard_b = 20;
    }

    let (guard_a, guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*guard_a, 10);
    assert_eq!(*guard_b, 20);
}

#[tokio::test]
async fn guards_released_on_drop() {
    let mutex_a = Mutex::new(1u32);
    let mutex_b = Mutex::new(2u32);

    {
        let _guards = MultiLock2::new(&mutex_a, &mutex_b).await;
    }

    // Should be able to acquire again
    let (guard_a, guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*guard_a, 1);
    assert_eq!(*guard_b, 2);
}

#[tokio::test]
async fn sequential_acquisitions() {
    let mutex_a = Mutex::new(0u32);
    let mutex_b = Mutex::new(0u32);

    for i in 0..10 {
        let (mut guard_a, mut guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;
        *guard_a += 1;
        *guard_b += 2;
        assert_eq!(*guard_a, i + 1);
        assert_eq!(*guard_b, (i + 1) * 2);
    }
}

#[tokio::test]
async fn guards_return_in_argument_order() {
    let mutex_a = Mutex::new('a');
    let mutex_b = Mutex::new('b');
    let mutex_c = Mutex::new('c');

    // Regardless of internal acquisition order, guards match argument order
    let (ga, gb, gc) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    assert_eq!(*ga, 'a');
    assert_eq!(*gb, 'b');
    assert_eq!(*gc, 'c');

    drop((ga, gb, gc));

    // Try different argument order
    let (gc, ga, gb) = MultiLock3::new(&mutex_c, &mutex_a, &mutex_b).await;

    assert_eq!(*gc, 'c');
    assert_eq!(*ga, 'a');
    assert_eq!(*gb, 'b');
}
