//! Tests for heterogeneous mutex types.

use tokio::sync::Mutex;
use tokio_multilock::{MultiLock2, MultiLock3, MultiLock4};

#[tokio::test]
async fn different_primitive_types() {
    let mutex_a = Mutex::new(42u32);
    let mutex_b = Mutex::new(-100i64);

    let (guard_a, guard_b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(*guard_a, 42u32);
    assert_eq!(*guard_b, -100i64);
}

#[tokio::test]
async fn mixed_primitives_and_strings() {
    let mutex_num = Mutex::new(42u32);
    let mutex_str = Mutex::new(String::from("hello"));

    let (guard_num, guard_str) = MultiLock2::new(&mutex_num, &mutex_str).await;

    assert_eq!(*guard_num, 42);
    assert_eq!(*guard_str, "hello");
}

#[tokio::test]
async fn vec_and_hashmap() {
    use std::collections::HashMap;

    let mutex_vec = Mutex::new(vec![1, 2, 3]);
    let mutex_map = Mutex::new(HashMap::from([("a", 1), ("b", 2)]));

    let (guard_vec, guard_map) = MultiLock2::new(&mutex_vec, &mutex_map).await;

    assert_eq!(guard_vec.len(), 3);
    assert_eq!(guard_map.get("a"), Some(&1));
}

#[tokio::test]
async fn three_different_types() {
    let mutex_a = Mutex::new(1u8);
    let mutex_b = Mutex::new("static str");
    let mutex_c = Mutex::new(vec![1.0f64, 2.0, 3.0]);

    let (a, b, c) = MultiLock3::new(&mutex_a, &mutex_b, &mutex_c).await;

    assert_eq!(*a, 1u8);
    assert_eq!(*b, "static str");
    assert_eq!(c.len(), 3);
}

#[tokio::test]
async fn four_different_types() {
    let mutex_a = Mutex::new(true);
    let mutex_b = Mutex::new('X');
    let mutex_c = Mutex::new(1.23f32);
    let mutex_d = Mutex::new(Option::<i32>::None);

    let (a, b, c, d) = MultiLock4::new(&mutex_a, &mutex_b, &mutex_c, &mutex_d).await;

    assert!(*a);
    assert_eq!(*b, 'X');
    assert!((*c - 1.23f32).abs() < 0.001);
    assert!(d.is_none());
}

#[tokio::test]
async fn mutate_heterogeneous() {
    let mutex_count = Mutex::new(0u32);
    let mutex_log = Mutex::new(Vec::<String>::new());

    for i in 0..5 {
        let (mut count, mut log) = MultiLock2::new(&mutex_count, &mutex_log).await;
        *count += 1;
        log.push(format!("iteration {i}"));
    }

    let (count, log) = MultiLock2::new(&mutex_count, &mutex_log).await;

    assert_eq!(*count, 5);
    assert_eq!(log.len(), 5);
    assert_eq!(log[0], "iteration 0");
    assert_eq!(log[4], "iteration 4");
}

#[derive(Debug, Clone, PartialEq)]
struct CustomType {
    id: u64,
    name: String,
}

#[tokio::test]
async fn custom_struct_types() {
    let mutex_a = Mutex::new(CustomType {
        id: 1,
        name: "first".into(),
    });
    let mutex_b = Mutex::new(CustomType {
        id: 2,
        name: "second".into(),
    });

    let (a, b) = MultiLock2::new(&mutex_a, &mutex_b).await;

    assert_eq!(a.id, 1);
    assert_eq!(b.name, "second");
}

#[tokio::test]
async fn option_and_result_types() {
    let mutex_opt = Mutex::new(Some(42));
    let mutex_res = Mutex::new(Result::<i32, &str>::Ok(100));

    let (opt, res) = MultiLock2::new(&mutex_opt, &mutex_res).await;

    assert_eq!(*opt, Some(42));
    assert_eq!(*res, Ok(100));
}

#[tokio::test]
async fn nested_containers() {
    let mutex_nested = Mutex::new(vec![vec![1, 2], vec![3, 4, 5]]);
    let mutex_flat = Mutex::new(vec![10, 20, 30]);

    let (nested, flat) = MultiLock2::new(&mutex_nested, &mutex_flat).await;

    assert_eq!(nested[0], vec![1, 2]);
    assert_eq!(nested[1].len(), 3);
    assert_eq!(flat.iter().sum::<i32>(), 60);
}
