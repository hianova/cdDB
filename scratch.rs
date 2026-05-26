use core::cell::UnsafeCell;

pub struct ColumnArray<T, const N: usize> {
    buffers: [UnsafeCell<[Option<T>; N]>; 2],
}

impl<T, const N: usize> ColumnArray<T, N> {
    pub fn new() -> Self {
        Self {
            buffers: [
                UnsafeCell::new(core::array::from_fn(|_| None)),
                UnsafeCell::new(core::array::from_fn(|_| None)),
            ],
        }
    }
}
