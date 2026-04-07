use std::{cmp::Ordering, pin::pin};

use tokio::sync::{Mutex, MutexGuard};

pub struct MultiLock<'a, 'b, A, B>
where
    A: 'a,
    B: 'b,
{
    mutex_a: &'a Mutex<A>,
    mutex_b: &'b Mutex<B>,
}

impl<'a, 'b, A, B> MultiLock<'a, 'b, A, B> {
    pub const fn new(a: &'a Mutex<A>, b: &'b Mutex<B>) -> Self {
        Self {
            mutex_a: a,
            mutex_b: b,
        }
    }
}

impl<'a, 'b, A, B> Future for MultiLock<'a, 'b, A, B> {
    type Output = (MutexGuard<'a, A>, MutexGuard<'b, B>);

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        let addr_a = std::ptr::from_ref(self.mutex_a) as usize;
        let addr_b = std::ptr::from_ref(self.mutex_b) as usize;

        match addr_a.cmp(&addr_b) {
            Ordering::Less => {
                let poll_a = pin!(self.mutex_a.lock()).poll(cx);
                let poll_b = pin!(self.mutex_b.lock()).poll(cx);

                if let (Ready(guard_a), Ready(guard_b)) = (poll_a, poll_b) {
                    Ready((guard_a, guard_b))
                } else {
                    Pending
                }
            }
            Ordering::Greater => {
                let poll_b = pin!(self.mutex_b.lock()).poll(cx);
                let poll_a = pin!(self.mutex_a.lock()).poll(cx);

                if let (Ready(guard_a), Ready(guard_b)) = (poll_a, poll_b) {
                    Ready((guard_a, guard_b))
                } else {
                    Pending
                }
            }
            Ordering::Equal => {
                panic!("Attempted MultiLock acquisition of a single Mutex!");
            }
        }
    }
}
