//! A counting allocator: each target installs it as the global allocator and
//! bounds the heap one iteration may grow by, so a buffer that grows with the
//! input stream instead of with one frame fails the run instead of passing.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            grow(new_size);
        }
        new
    }
}

fn grow(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

/// Run `f` and fail when the heap grew by more than `limit` bytes at any
/// point during it. Without the counting allocator installed (the seed replay
/// test) nothing is counted and the bound holds trivially.
pub fn bounded<T>(limit: usize, what: &str, f: impl FnOnce() -> T) -> T {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let value = f();
    let grown = PEAK.load(Ordering::Relaxed).saturating_sub(base);
    if grown > limit {
        crate::violation(format!(
            "{what} grew the heap by {grown} bytes, over its {limit}-byte bound"
        ));
    }
    value
}
