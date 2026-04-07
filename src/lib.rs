use std::pin::pin;

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

        let fut_a = pin!(self.mutex_a.lock());
        let fut_b = pin!(self.mutex_b.lock());

        if let (Ready(guard_a), Ready(guard_b)) = (fut_a.poll(cx), fut_b.poll(cx)) {
            Ready((guard_a, guard_b))
        } else {
            Pending
        }
    }
}
