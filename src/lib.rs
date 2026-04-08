use std::{future::Future, pin::Pin, ptr, time::Duration};

use tokio::sync::{Mutex, MutexGuard};
use tokio::time::Sleep;

const INITIAL_TIMEOUT: Duration = Duration::from_micros(100);
const MAX_TIMEOUT: Duration = Duration::from_millis(100);

macro_rules! impl_multi_lock {
    ($name:ident, $n:expr => $([$idx:tt, $letter:ident, $lt:lifetime]),+ $(,)?) => {
        paste::paste! {
            pub struct $name<$($lt,)+ $($letter,)+>
            where
                $($letter: Send + $lt,)+
            {
                $(
                    [<mutex_ $letter:lower>]: &$lt Mutex<$letter>,
                )+
                $(
                    [<fut_ $letter:lower>]: Option<Pin<Box<dyn Future<Output = MutexGuard<$lt, $letter>> + Send + $lt>>>,
                )+
                $(
                    [<guard_ $letter:lower>]: Option<MutexGuard<$lt, $letter>>,
                )+
                order: [usize; $n],
                timeout: Duration,
                timeout_fut: Option<Pin<Box<Sleep>>>,
                backoff_fut: Option<Pin<Box<Sleep>>>,
            }

            impl<$($lt,)+ $($letter,)+> $name<$($lt,)+ $($letter,)+>
            where
                $($letter: Send + $lt,)+
            {
                #[allow(clippy::too_many_arguments)]
                pub fn new($([<$letter:lower>]: &$lt Mutex<$letter>,)+) -> Self {
                    let mut addrs = [$(($idx, ptr::from_ref([<$letter:lower>]) as usize),)+];
                    addrs.sort_by_key(|&(_, addr)| addr);

                    let mut prev = 0usize;
                    for (i, &(_, addr)) in addrs.iter().enumerate() {
                        if i > 0 && addr == prev {
                            panic!("MultiLock received the same Mutex more than once");
                        }
                        prev = addr;
                    }

                    let mut order = [0usize; $n];
                    for (i, &(idx, _)) in addrs.iter().enumerate() {
                        order[i] = idx;
                    }

                    Self {
                        $([<mutex_ $letter:lower>]: [<$letter:lower>],)+
                        $([<fut_ $letter:lower>]: None,)+
                        $([<guard_ $letter:lower>]: None,)+
                        order,
                        timeout: INITIAL_TIMEOUT,
                        timeout_fut: None,
                        backoff_fut: None,
                    }
                }
            }

            impl<$($lt,)+ $($letter,)+> Future for $name<$($lt,)+ $($letter,)+>
            where
                $($letter: Send + $lt,)+
            {
                type Output = ($(MutexGuard<$lt, $letter>,)+);

                fn poll(
                    mut self: Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Self::Output> {
                    use std::task::Poll::{Pending, Ready};

                    // If in backoff, wait for it to complete before retrying
                    if let Some(ref mut backoff) = self.backoff_fut {
                        if backoff.as_mut().poll(cx).is_pending() {
                            return Pending;
                        }
                        self.backoff_fut = None;
                    }

                    let order = self.order;

                    // Poll lock futures strictly in sorted order
                    // Only poll lock N if locks 0..N are already held
                    let mut ready_count: usize = 0;

                    for &idx in order.iter() {
                        let acquired = match idx {
                            $(
                                $idx => {
                                    // Already have this guard from a previous poll
                                    if self.[<guard_ $letter:lower>].is_some() {
                                        true
                                    } else {
                                        // Create future if needed
                                        if self.[<fut_ $letter:lower>].is_none() {
                                            self.[<fut_ $letter:lower>] = Some(Box::pin(self.[<mutex_ $letter:lower>].lock()));
                                        }

                                        let fut = self.[<fut_ $letter:lower>].as_mut().unwrap();

                                        if let Ready(guard) = fut.as_mut().poll(cx) {
                                            self.[<guard_ $letter:lower>] = Some(guard);
                                            self.[<fut_ $letter:lower>] = None;
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                }
                            )+
                            _ => unreachable!(),
                        };

                        if acquired {
                            ready_count += 1;
                        } else {
                            break; // Stop at first Pending - strict ordering
                        }
                    }

                    // All locks acquired - return immediately
                    if ready_count == $n {
                        return Ready(($(self.[<guard_ $letter:lower>].take().unwrap(),)+));
                    }

                    // If we have at least one guard, ensure timer is running
                    if ready_count > 0 && self.timeout_fut.is_none() {
                        self.timeout_fut = Some(Box::pin(tokio::time::sleep(self.timeout)));
                    }

                    // Poll the timeout if active
                    let timed_out = if let Some(ref mut timeout_fut) = self.timeout_fut {
                        timeout_fut.as_mut().poll(cx).is_ready()
                    } else {
                        false
                    };

                    if timed_out {
                        // Timeout fired - drop all guards and futures, start backoff sleep
                        $(
                            self.[<guard_ $letter:lower>] = None;
                            self.[<fut_ $letter:lower>] = None;
                        )+

                        self.timeout_fut = None;
                        let backoff = self.timeout;
                        self.timeout = (self.timeout * 2).min(MAX_TIMEOUT);

                        // Sleep before retrying to let other tasks proceed
                        let mut backoff_sleep = Box::pin(tokio::time::sleep(backoff));
                        if backoff_sleep.as_mut().poll(cx).is_pending() {
                            self.backoff_fut = Some(backoff_sleep);
                            return Pending;
                        }

                        // Backoff completed immediately - wake to retry now
                        cx.waker().wake_by_ref();
                        return Pending;
                    }

                    Pending
                }
            }
        }
    };
}

impl_multi_lock!(MultiLock2, 2 => [0, A, 'a], [1, B, 'b]);
impl_multi_lock!(MultiLock3, 3 => [0, A, 'a], [1, B, 'b], [2, C, 'c]);
impl_multi_lock!(MultiLock4, 4 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd]);
impl_multi_lock!(MultiLock5, 5 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e]);
impl_multi_lock!(MultiLock6, 6 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f]);
impl_multi_lock!(MultiLock7, 7 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g]);
impl_multi_lock!(MultiLock8, 8 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h]);
impl_multi_lock!(MultiLock9, 9 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i]);
impl_multi_lock!(MultiLock10, 10 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j]);
impl_multi_lock!(MultiLock11, 11 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k]);
impl_multi_lock!(MultiLock12, 12 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l]);
impl_multi_lock!(MultiLock13, 13 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm]);
impl_multi_lock!(MultiLock14, 14 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n]);
impl_multi_lock!(MultiLock15, 15 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o]);
impl_multi_lock!(MultiLock16, 16 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p]);
impl_multi_lock!(MultiLock17, 17 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q]);
impl_multi_lock!(MultiLock18, 18 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r]);
impl_multi_lock!(MultiLock19, 19 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's]);
impl_multi_lock!(MultiLock20, 20 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't]);
impl_multi_lock!(MultiLock21, 21 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u]);
impl_multi_lock!(MultiLock22, 22 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u], [21, V, 'v]);
impl_multi_lock!(MultiLock23, 23 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u], [21, V, 'v], [22, W, 'w]);
impl_multi_lock!(MultiLock24, 24 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u], [21, V, 'v], [22, W, 'w], [23, X, 'x]);
impl_multi_lock!(MultiLock25, 25 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u], [21, V, 'v], [22, W, 'w], [23, X, 'x], [24, Y, 'y]);
impl_multi_lock!(MultiLock26, 26 => [0, A, 'a], [1, B, 'b], [2, C, 'c], [3, D, 'd], [4, E, 'e], [5, F, 'f], [6, G, 'g], [7, H, 'h], [8, I, 'i], [9, J, 'j], [10, K, 'k], [11, L, 'l], [12, M, 'm], [13, N, 'n], [14, O, 'o], [15, P, 'p], [16, Q, 'q], [17, R, 'r], [18, S, 's], [19, T, 't], [20, U, 'u], [21, V, 'v], [22, W, 'w], [23, X, 'x], [24, Y, 'y], [25, Z, 'z]);
