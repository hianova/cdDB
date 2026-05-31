#![cfg(feature = "loom")]
use cdDB::qsbr::{QsbrManager, WorkerNode, WorkerState};
use std::sync::Arc;
use cdDB::platform::atomic::AtomicPtr;
use loom::thread;

#[test]
fn test_qsbr_worker_registration() {
    loom::model(|| {
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut::<WorkerNode>()));
        
        let w_clone1 = workers.clone();
        let t1 = thread::spawn(move || {
            let worker_state = Arc::new(WorkerState::new());
            let new_node = Box::into_raw(Box::new(WorkerNode {
                worker: worker_state,
                next: AtomicPtr::new(core::ptr::null_mut()),
            }));
            loop {
                let head = w_clone1.load(loom::sync::atomic::Ordering::Acquire);
                unsafe { (*new_node).next.store(head, loom::sync::atomic::Ordering::Relaxed) };
                if w_clone1.compare_exchange(head, new_node, loom::sync::atomic::Ordering::Release, loom::sync::atomic::Ordering::Relaxed).is_ok() {
                    break;
                }
            }
        });
        
        let w_clone2 = workers.clone();
        let t2 = thread::spawn(move || {
            let worker_state = Arc::new(WorkerState::new());
            let new_node = Box::into_raw(Box::new(WorkerNode {
                worker: worker_state,
                next: AtomicPtr::new(core::ptr::null_mut()),
            }));
            loop {
                let head = w_clone2.load(loom::sync::atomic::Ordering::Acquire);
                unsafe { (*new_node).next.store(head, loom::sync::atomic::Ordering::Relaxed) };
                if w_clone2.compare_exchange(head, new_node, loom::sync::atomic::Ordering::Release, loom::sync::atomic::Ordering::Relaxed).is_ok() {
                    break;
                }
            }
        });
        
        t1.join().unwrap();
        t2.join().unwrap();
        
        let mut count = 0;
        let mut curr = workers.load(loom::sync::atomic::Ordering::Acquire);
        while !curr.is_null() {
            count += 1;
            curr = unsafe { (*curr).next.load(loom::sync::atomic::Ordering::Acquire) };
        }
        
        assert_eq!(count, 2);
        
        let mut curr = workers.load(loom::sync::atomic::Ordering::Acquire);
        while !curr.is_null() {
            let next = unsafe { (*curr).next.load(loom::sync::atomic::Ordering::Acquire) };
            unsafe { drop(Box::from_raw(curr)); }
            curr = next;
        }
    });
}
