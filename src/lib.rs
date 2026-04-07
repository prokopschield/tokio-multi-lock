use std::{future::Future, pin::Pin, ptr};

use tokio::sync::{Mutex, MutexGuard};

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
            }

            impl<$($lt,)+ $($letter,)+> $name<$($lt,)+ $($letter,)+>
            where
                $($letter: Send + $lt,)+
            {
                #[allow(clippy::too_many_arguments)]
                pub fn new($([<$letter:lower>]: &$lt Mutex<$letter>,)+) -> Self {
                    Self {
                        $([<mutex_ $letter:lower>]: [<$letter:lower>],)+
                        $([<fut_ $letter:lower>]: None,)+
                    }
                }

                fn compute_order(&self) -> [usize; $n] {
                    let mut addrs = [$(($idx, ptr::from_ref(self.[<mutex_ $letter:lower>]) as usize),)+];
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

                    order
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

                    let order = self.compute_order();

                    $(
                        let mut [<fut_ $letter:lower>]: Option<Pin<Box<dyn Future<Output = MutexGuard<$lt, $letter>> + Send + $lt>>> =
                            Some(self.[<fut_ $letter:lower>].take().unwrap_or_else(|| {
                                Box::pin(self.[<mutex_ $letter:lower>].lock())
                            }));
                    )+

                    $(
                        let mut [<guard_ $letter:lower>]: Option<MutexGuard<$lt, $letter>> = None;
                    )+

                    let mut ready_bits: u32 = 0;

                    for (pos, &idx) in order.iter().enumerate() {
                        match idx {
                            $(
                                $idx => {
                                    let fut = [<fut_ $letter:lower>].as_mut().unwrap();

                                    if let Ready(guard) = fut.as_mut().poll(cx) {
                                        [<guard_ $letter:lower>] = Some(guard);
                                        [<fut_ $letter:lower>] = None;
                                        ready_bits |= 1 << pos;
                                    }
                                }
                            )+
                            _ => unreachable!(),
                        }
                    }

                    let prefix_len = ready_bits.trailing_ones() as usize;

                    if prefix_len == $n {
                        return Ready(($([<guard_ $letter:lower>].unwrap(),)+));
                    }

                    for (pos, &idx) in order.iter().enumerate() {
                        match idx {
                            $(
                                $idx => {
                                    if pos < prefix_len {
                                        let guard = [<guard_ $letter:lower>].take().unwrap();
                                        self.[<fut_ $letter:lower>] = Some(Box::pin(async move { guard }));
                                    } else if [<fut_ $letter:lower>].is_none() {
                                        drop([<guard_ $letter:lower>].take());
                                    } else {
                                        self.[<fut_ $letter:lower>] = [<fut_ $letter:lower>].take();
                                    }
                                }
                            )+
                            _ => unreachable!(),
                        }
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
