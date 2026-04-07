use std::{cmp::Ordering, pin::Pin};

use tokio::sync::{Mutex, MutexGuard};

pub struct MultiLock<'a, 'b, A, B>
where
    A: 'a,
    B: 'b,
{
    mutex_a: &'a Mutex<A>,
    mutex_b: &'b Mutex<B>,
    fut_a: Option<Pin<Box<dyn Future<Output = MutexGuard<'a, A>> + 'a>>>,
    fut_b: Option<Pin<Box<dyn Future<Output = MutexGuard<'b, B>> + 'b>>>,
}

impl<'a, 'b, A, B> MultiLock<'a, 'b, A, B>
where
    A: 'a,
    B: 'b,
{
    pub const fn new(a: &'a Mutex<A>, b: &'b Mutex<B>) -> Self {
        Self {
            mutex_a: a,
            mutex_b: b,
            fut_a: None,
            fut_b: None,
        }
    }
}

impl<'a, 'b, A, B> Future for MultiLock<'a, 'b, A, B>
where
    A: 'a,
    B: 'b,
{
    type Output = (MutexGuard<'a, A>, MutexGuard<'b, B>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        let addr_a = std::ptr::from_ref(self.mutex_a) as usize;
        let addr_b = std::ptr::from_ref(self.mutex_b) as usize;

        let mut fut_a = self
            .fut_a
            .take()
            .unwrap_or_else(|| Box::pin(self.mutex_a.lock()));

        let mut fut_b = self
            .fut_b
            .take()
            .unwrap_or_else(|| Box::pin(self.mutex_b.lock()));

        let (poll_a, poll_b) = match addr_a.cmp(&addr_b) {
            Ordering::Less => {
                let poll_a = fut_a.as_mut().poll(cx);
                let poll_b = fut_b.as_mut().poll(cx);

                (poll_a, poll_b)
            }
            Ordering::Greater => {
                let poll_b = fut_b.as_mut().poll(cx);
                let poll_a = fut_a.as_mut().poll(cx);

                (poll_a, poll_b)
            }
            Ordering::Equal => {
                panic!("Attempted MultiLock acquisition of a single Mutex!");
            }
        };

        match (poll_a, poll_b) {
            (Ready(guard_a), Ready(guard_b)) => Ready((guard_a, guard_b)),
            (Ready(guard_a), Pending) => {
                if addr_a < addr_b {
                    self.fut_a = Some(Box::pin(async move { guard_a }));
                } else {
                    drop(guard_a);
                }

                self.fut_b = Some(fut_b);
                Pending
            }
            (Pending, Ready(guard_b)) => {
                if addr_a < addr_b {
                    drop(guard_b);
                } else {
                    self.fut_b = Some(Box::pin(async move { guard_b }));
                }

                self.fut_a = Some(fut_a);
                Pending
            }
            (Pending, Pending) => {
                self.fut_a = Some(fut_a);
                self.fut_b = Some(fut_b);
                Pending
            }
        }
    }
}
