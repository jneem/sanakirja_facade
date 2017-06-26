// Copyright 2015 Pierre-Étienne Meunier and Florent Becker.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std;
use std::sync::{RwLock, RwLockReadGuard, Mutex, MutexGuard};
use std::ptr::copy_nonoverlapping;
use std::collections::{HashSet, HashMap};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;
use memmap;

#[doc(hidden)]
pub const CURRENT_VERSION: u64 = 0;

const OFF_MAP_LENGTH: isize = 1;
const OFF_CURRENT_FREE: isize = 2;
// We need a fixed page size for compatibility reasons.
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_64: u64 = 4096;

pub const ZERO_HEADER: isize = 24; // size of the header on page 0, in bytes.

/// Errors that can occur while transacting.
#[derive(Debug)]
pub enum Error {
    /// IO errors, from the `std::io` module.
    IO(std::io::Error),

    /// The mmap was not large enough to allocate all required memory.
    NotEnoughSpace,

    /// Lock poisoning error.
    Poison,

    /// You tried to create a mutable transaction while another one was active.
    MultipleWriters,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::NotEnoughSpace => {
                write!(f,
                       "Not enough space. Try opening the environment with a larger size.")
            }
            Error::Poison => write!(f, "Lock poisoning error"),
            Error::MultipleWriters =>
                write!(f, "You tried to create a multable transaction while another one was active"),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref err) => err.description(),
            Error::NotEnoughSpace => {
                "Not enough space. Try opening the environment with a larger size."
            }
            Error::Poison => "Poison error",
            Error::MultipleWriters => "Multiple writers",
        }
    }
    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::NotEnoughSpace => None,
            Error::Poison => None,
            Error::MultipleWriters => None,
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IO(e)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Error {
        Error::Poison
    }
}

impl<T> From<std::sync::TryLockError<T>> for Error {
    fn from(e: std::sync::TryLockError<T>) -> Error {
        match e {
            std::sync::TryLockError::Poisoned(_) => Error::Poison,
            std::sync::TryLockError::WouldBlock => Error::MultipleWriters,
        }
    }
}

pub enum MemSource {
    File {
        file: File,
        lock_file: File,
        mmap: memmap::Mmap,
    },
    Mem {
        vec: Vec<u8>,
    },
}

impl Drop for MemSource {
    fn drop(&mut self) {
        if let MemSource::File { ref lock_file, .. } = *self {
            // TODO: what does it mean if the unlock fails? We shouldn't panic in a destructor...
            let _ = lock_file.unlock();
        }
    }
}

// Lock order: first take thread locks, then process locks.

// Why are there two synchronization mechanisms?
// Because we would need to upgrade the read lock into a write lock,
// and there is no real way to do this with standard mechanisms.
// So, we take a mutex to make sure no other mutable transaction can start,
// and then at the time of writing, we also take the RwLock.

/// Environment, required to start any transactions. Thread-safe, but
/// opening the same database several times in the same process is not
/// cross-platform.
pub struct Env {
    length: u64,
    mem_source: MemSource,
    //backing_file: File,

    /// It is undefined behavior to have a file mmapped for than once.
    //lock_file: File,
    //mmap: Option<memmap::Mmap>,
    map: *mut u8,
    first_unused_page: Mutex<u64>,
    /// Ensure all reads are done when sync starts.
    lock: RwLock<()>,

    /// Ensure only one mutable transaction can be started.
    mutable: Mutex<()>,
}
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

impl Drop for Env {
    fn drop(&mut self) {
        // Try to truncate the backing file if we can.
        if let MemSource::File { ref file, .. } = self.mem_source {
            if let Ok(f) = self.first_unused_page.lock() {
                if *f > 0 {
                    let _ = file.set_len(*f);
                }
            }
        }
    }
}


/// An immutable transaction.
pub struct Txn<'env> {
    env: &'env Env,
    guard: RwLockReadGuard<'env, ()>,
}

/// A mutable transaction.
pub struct MutTxn<'env> {
    env: &'env Env,
    mutable: Option<MutexGuard<'env, ()>>,
    pub parent: Option<Box<MutTxn<'env>>>,

    /// The offset from the beginning of the file, of the first free
    /// page at the end of the file. Note that there might be other
    /// free pages in the file.
    last_page: u64,

    /// current page storing the list of free pages.
    current_list_page: Page,

    /// length of the current page of free pages.
    current_list_length: u64,

    /// position in the current page of free pages.
    current_list_position: u64,

    /// Offsets of pages that were allocated by this transaction, and
    /// have not been freed since.
    occupied_clean_pages: HashSet<u64>,

    /// Offsets of pages that were allocated by this transaction, and
    /// then freed.
    free_clean_pages: Vec<u64>,

    /// Offsets of old pages freed by this transaction. These were
    /// *not* allocated by this transaction.
    free_pages: Vec<u64>,

    pub roots: HashMap<usize, u64>,
}

impl<'env> Drop for Txn<'env> {
    fn drop(&mut self) {
        *self.guard;
    }
}
impl<'env> Drop for MutTxn<'env> {
    fn drop(&mut self) {
        debug!("dropping transaction");
        if let Some(ref mut guard) = self.mutable {
            debug!("dropping guard");
            **guard
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct Statistics {
    pub free_pages: HashSet<u64>,
    pub bookkeeping_pages: Vec<u64>,
    pub total_pages: usize,
    pub reference_counts: HashMap<u64, u64>,
}


impl Env {

    /// File size of the database path, if it exists.
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64, Error> {
        let db_path = path.as_ref().join("db");
        debug!("db_path = {:?}, len = {:?}", db_path, std::fs::metadata(&db_path)?.len());
        Ok(std::fs::metadata(&db_path)?.len())
    }

    /// Length of the environment.
    pub fn size(&self) -> u64 {
        self.length
    }

    /// Returns a new environment backed by memory.
    pub fn new_memory_backed(length: u64) -> Result<Env, Error> {
        let mut vec = vec![0; length as usize];
        let map = vec.as_mut_ptr();
        Ok(Env {
            mem_source: MemSource::Mem { vec: vec },
            length: length,
            map: map,
            first_unused_page: Mutex::new(0),
            lock: RwLock::new(()),
            mutable: Mutex::new(()),
        })
    }

    /// Initialize an environment. `length` must be a strictly
    /// positive multiple of 4096. The same file can only be open in
    /// one process or thread at the same time, and this is enforced
    /// by a locked file.
    pub fn new<P: AsRef<Path>>(path: P, length: u64) -> Result<Env, Error> {
        // let length = (1 as u64).shl(log_length);
        let db_path = path.as_ref().join("db");


        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path.as_ref()
                  .join("db")
                  .with_extension("lock"))?;

        try!(lock_file.lock_exclusive());


        let db_exists = std::fs::metadata(&db_path).is_ok();
        let length = if let Ok(meta) = std::fs::metadata(&db_path) {
            std::cmp::max(meta.len(), length)
        } else {
            length
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&db_path)?;
        file.allocate(length)?;
        let mut mmap = memmap::Mmap::open(&file, memmap::Protection::ReadWrite)?;
        debug!("mmap: {:?}", mmap.len());
        let map = mmap.mut_ptr();
        if !db_exists {
            unsafe {
                std::ptr::write_bytes(map, 0, PAGE_SIZE);
                *(map as *mut u64) = CURRENT_VERSION.to_le();
            }
        } else {
            assert!(unsafe { u64::from_le(*(map as *const u64)) == CURRENT_VERSION })
        }
        debug!("metadata.len() = {:?}", std::fs::metadata(&db_path).map(|x| x.len()));
        let env = Env {
            mem_source: MemSource::File {
                mmap: mmap,
                file: file,
                lock_file: lock_file,
            },
            length: length,
            map: map,
            first_unused_page: Mutex::new(0),
            lock: RwLock::new(()),
            mutable: Mutex::new(()),
        };
        Ok(env)
    }

    /// Start a read-only transaction.
    pub fn txn_begin<'env>(&'env self) -> Result<Txn<'env>, Error> {
        let read = try!(self.lock.read());
        Ok(Txn {
            env: self,
            guard: read,
        })
    }

    /// Start a mutable transaction. Mutable transactions that go out
    /// of scope are automatically aborted.
    pub fn mut_txn_begin<'env>(&'env self) -> Result<MutTxn<'env>, Error> {
        unsafe {
            let last_page = u64::from_le(*((self.map as *const u64).offset(OFF_MAP_LENGTH)));
            let current_list_page = u64::from_le(*((self.map as *const u64)
                .offset(OFF_CURRENT_FREE)));

            debug!("map header = {:?}, {:?}", last_page, current_list_page);
            let guard = self.mutable.try_lock()?;
            debug!("lock ok");
            assert!(current_list_page < self.length);
            let current_list_page = Page {
                data: self.map.offset(current_list_page as isize),
                offset: current_list_page,
            };
            let current_list_length = if current_list_page.offset == 0 {
                0
            } else {
                u64::from_le(*((current_list_page.data as *const u64).offset(1)))
            };
            Ok(MutTxn {
                env: self,
                mutable: Some(guard),
                parent: None,
                last_page: if last_page == 0 {
                    PAGE_SIZE_64
                } else {
                    last_page
                },
                current_list_page: current_list_page,
                current_list_length: current_list_length,
                current_list_position: current_list_length,
                occupied_clean_pages: HashSet::new(),
                free_clean_pages: Vec::new(),
                free_pages: Vec::new(),
                roots: HashMap::new(),
            })
        }
    }

    /// Compute statistics about pages. This is a potentially costlty
    /// operation, as we need to go through all bookkeeping pages.
    #[doc(hidden)]
    #[cfg(test)]
    pub fn statistics(&self) -> Statistics {
        unsafe {
            let total_pages = u64::from_le(*((self.map as *const u64)
                .offset(OFF_MAP_LENGTH))) as usize;
            let mut free_pages = HashSet::new();
            let mut bookkeeping_pages = Vec::new();
            let mut cur = u64::from_le(*((self.map as *const u64).offset(OFF_CURRENT_FREE)));
            while cur != 0 {
                bookkeeping_pages.push(cur);
                let p = self.map.offset(cur as isize) as *const u64;
                let prev = u64::from_le(*p);
                let len = u64::from_le(*(p.offset(1))); // size (number of u64).
                debug!("bookkeeping page: {:?}, {} {}", cur, prev, len);
                {
                    let mut p: *const u64 = (p).offset(2);
                    let mut i = 0;
                    while i < len {
                        let free_page = u64::from_le(*p);
                        if !free_pages.insert(free_page) {
                            panic!("free page counted twice: {:?}", free_page)
                        }
                        p = p.offset(1);
                        i += 1
                    }
                }
                cur = prev
            }
            let refcounts = HashMap::new();
            Statistics {
                total_pages: total_pages / PAGE_SIZE,
                free_pages: free_pages,
                bookkeeping_pages: bookkeeping_pages,
                reference_counts: refcounts,
            }
        }
    }
}

/// This is a semi-owned page: just as we can mutate several indices
/// of an array in the same scope, we must be able to get several
/// pages from a single environment in the same scope. However, pages
/// don't outlive their environment. Pages longer than one PAGE_SIZE
/// might trigger calls to munmap when they go out of scope.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Page {
    pub data: *const u8,
    pub offset: u64,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct MutPage {
    pub data: *mut u8,
    pub offset: u64,
}

impl MutPage {
    pub fn as_page(&self) -> Page {
        Page {
            data: self.data,
            offset: self.offset,
        }
    }
}
#[doc(hidden)]
pub trait LoadPage {
    fn load_page(&self, off: u64) -> Page;
    fn len(&self) -> u64;
    fn root_(&self, num: usize) -> u64;
}

impl<'env> LoadPage for Txn<'env> {
    /// Find the appropriate map segment
    fn load_page(&self, off: u64) -> Page {
        debug!("load_page: off={:?}, length = {:?}", off, self.env.length);
        assert!(off < self.env.length);
        unsafe {
            Page {
                data: self.env.map.offset(off as isize),
                offset: off,
            }
        }
    }

    fn len(&self) -> u64 {
        self.env.length
    }

    fn root_(&self, num: usize) -> u64 {
        assert!(ZERO_HEADER as usize + ((num + 1) << 3) < PAGE_SIZE);
        unsafe {
            u64::from_le(*((self.env.map.offset(ZERO_HEADER) as *const u64).offset(num as isize)))
        }
    }
}


impl<'env> LoadPage for MutTxn<'env> {
    fn load_page(&self, off: u64) -> Page {
        if off >= self.env.length {
            panic!("{:?} >= {:?}", off, self.env.length)
        }
        unsafe {
            Page {
                data: self.env.map.offset(off as isize),
                offset: off,
            }
        }
    }

    fn len(&self) -> u64 {
        self.env.length
    }

    fn root_(&self, num: usize) -> u64 {
        if let Some(root) = self.roots.get(&num) {
            *root
        } else {
            assert!(ZERO_HEADER + ((num as isize + 1) << 3) < (PAGE_SIZE as isize));
            unsafe {
                u64::from_le(*((self.env.map.offset(ZERO_HEADER) as *const u64)
                    .offset(num as isize)))
            }
        }
    }
}






#[derive(Debug)]
#[doc(hidden)]
pub enum Cow {
    Page(Page),
    MutPage(MutPage),
}

impl Cow {
    pub fn as_page(&self) -> Page {
        match self {
            &Cow::Page(ref p) => p.clone(),
            &Cow::MutPage(ref p) => p.as_page(),
        }
    }
}

impl<'env> MutTxn<'env> {
    /// Start a mutable transaction.
    // Despite the signature, this can't actually fail.
    pub fn mut_txn_begin<'txn>(self) -> Result<MutTxn<'env>, Error> {
        let mut txn = MutTxn {
            env: self.env,
            mutable: None,
            parent: None,
            last_page: self.last_page,
            current_list_page: Page {
                data: self.current_list_page.data,
                offset: self.current_list_page.offset,
            },
            current_list_length: self.current_list_length,
            current_list_position: self.current_list_position,
            occupied_clean_pages: HashSet::new(),
            free_clean_pages: Vec::new(),
            free_pages: Vec::new(),
            roots: self.roots.clone(), // reference_counts:self.reference_counts
        };
        txn.parent = Some(Box::new(self));
        Ok(txn)
    }

    #[doc(hidden)]
    pub fn set_root_(&mut self, num: usize, value: u64) {
        self.roots.insert(num, value);
    }

    #[doc(hidden)]
    pub fn load_cow_page(&mut self, off: u64) -> Cow {
        trace!("transaction::load_mut_page: {:?} {:?}",
               off,
               self.occupied_clean_pages);
        assert!(off < self.env.length);
        if off != 0 && self.occupied_clean_pages.contains(&off) {
            unsafe {
                Cow::MutPage(MutPage {
                    data: self.env.map.offset(off as isize),
                    offset: off,
                })
            }
        } else {
            unsafe {
                let d = self.env.map.offset(off as isize);
                Cow::Page(Page {
                    data: d,
                    offset: off,
                })
            }
        }
    }

    #[doc(hidden)]
    pub unsafe fn free_page(&mut self, offset: u64) {
        trace!("transaction::free page: {:?}", offset);
        if self.occupied_clean_pages.remove(&offset) {
            self.free_clean_pages.push(offset);
        } else {
            // Else, register it for freeing (we cannot reuse it in this transaction).
            self.free_pages.push(offset)
        }
    }

    pub fn base(&self) -> *mut u8 {
        self.env.map
    }

    /// Pop a free page from the list of free pages.
    fn free_pages_pop(&mut self) -> Option<u64> {
        trace!("free_pages_pop, current_list_position:{}",
               self.current_list_position);
        if self.current_list_page.offset == 0 {
            None
        } else {
            if self.current_list_position == 0 {
                let previous_page =
                    unsafe { u64::from_le(*(self.current_list_page.data as *const u64)) };
                trace!("free_pages_pop, previous page:{}", previous_page);
                if previous_page == 0 {
                    None
                } else {
                    // free page (i.e. push to the list of old
                    // free pages), move to previous bookkeeping
                    // pages, and call recursively.
                    self.free_pages.push(self.current_list_page.offset);
                    unsafe {
                        self.current_list_page = Page {
                            data: self.env.map.offset(previous_page as isize),
                            offset: previous_page,
                        };
                        self.current_list_length =
                            u64::from_le(*((self.current_list_page.data as *const u64).offset(1)))
                    }
                    self.current_list_position = self.current_list_length;
                    self.free_pages_pop()
                }
            } else {
                let pos = self.current_list_position;
                // find the page at the top.
                self.current_list_position -= 1;
                trace!("free_pages_pop, new position:{}",
                       self.current_list_position);
                unsafe {
                    Some(u64::from_le(*((self.current_list_page.data as *mut u64)
                        .offset(1 + pos as isize))))
                }
            }
        }
    }

    /// Allocate a single page.
    pub fn alloc_page(&mut self) -> Result<MutPage, Error> {
        trace!("alloc page");
        // If we have allocated and freed a page in this transaction, use it first.
        if let Some(page) = self.free_clean_pages.pop() {
            trace!("clean page reuse:{}", page);
            self.occupied_clean_pages.insert(page);
            Ok(MutPage {
                data: unsafe { self.env.map.offset(page as isize) },
                offset: page,
            })
        } else {
            // Else, if there are free pages, take one.
            if let Some(page) = self.free_pages_pop() {
                trace!("using an old free page: {}", page);
                self.occupied_clean_pages.insert(page);
                Ok(MutPage {
                    data: unsafe { self.env.map.offset(page as isize) },
                    offset: page,
                })
            } else {
                // Else, allocate in the free space.
                let last = self.last_page;
                trace!("eating the free space: {}", last);
                if self.last_page + PAGE_SIZE_64 < self.env.length {
                    self.last_page += PAGE_SIZE_64;
                    self.occupied_clean_pages.insert(last);
                    Ok(MutPage {
                        data: unsafe { self.env.map.offset(last as isize) },
                        offset: last,
                    })
                } else {
                    Err(Error::NotEnoughSpace)
                }
            }
        }
    }
}



/// Transactions that can be committed.
pub trait Commit {
    /// Commit the transaction.
    fn commit(self) -> Result<(), Error>;
}

impl<'env> MutTxn<'env> {
    fn commit_to_parent(mut self) -> MutTxn<'env> {
        let mut parent = self.parent.take().unwrap();
        parent.last_page = self.last_page;
        parent.current_list_page = Page {
            offset: self.current_list_page.offset,
            data: self.current_list_page.data,
        };
        parent.current_list_length = self.current_list_length;
        parent.current_list_position = self.current_list_position;
        parent.occupied_clean_pages.extend(self.occupied_clean_pages.iter());
        parent.free_clean_pages.extend(self.free_clean_pages.iter());
        parent.free_pages.extend(self.free_pages.iter());
        for (u, v) in self.roots.iter() {
            parent.roots.insert(*u, *v);
        }
        *parent
    }

    fn commit_to_env(mut self) -> Result<(), Error> {
        // Tasks:
        //
        // - allocate new pages (copy-on-write) to write the new list
        // of free pages, including edited "stack pages".
        //
        // - write top of the stack
        // - write user data
        //
        // everything can be sync'ed at any time, except that the
        // first page needs to be sync'ed last.
        unsafe {
            // Copy the current bookkeeping page to a newly allocated page.
            let mut current_page = try!(self.alloc_page());
            if self.current_list_page.offset != 0 {
                // If there was at least one bookkeeping page before.
                debug!("commit: realloc BK, copy {:?}", self.current_list_position);
                copy_nonoverlapping(self.current_list_page.data as *const u64,
                                    current_page.data as *mut u64,
                                    2 + self.current_list_position as usize);
                *((current_page.data as *mut u64).offset(1)) = self.current_list_position.to_le();

                // and free the previous current bookkeeping page.
                debug!("freeing BK page {:?}", self.current_list_page.offset);
                self.free_pages.push(self.current_list_page.offset);

            } else {
                // Else, init the page.
                *(current_page.data as *mut u64) = 0; // previous page: none
                *((current_page.data as *mut u64).offset(1)) = 0; // len: 0
            }

            while !(self.free_pages.is_empty() && self.free_clean_pages.is_empty()) {
                debug!("commit: pushing");
                // If page is full, or this is the first page, allocate new page.
                let len = u64::from_le(*((current_page.data as *const u64).offset(1)));
                debug!("len={:?}", len);
                if 16 + len * 8 + 8 >= PAGE_SIZE_64 {
                    debug!("commit: current is full, len={}", len);
                    // 8 more bytes wouldn't fit in this page, time to allocate a new one

                    let p = self.free_pages
                        .pop()
                        .unwrap_or_else(|| self.free_clean_pages.pop().unwrap());

                    let new_page = MutPage {
                        data: self.env.map.offset(p as isize),
                        offset: p,
                    };

                    debug!("commit {} allocated {:?}", line!(), new_page.offset);
                    // Write a reference to the current page (which cannot be null).
                    *(new_page.data as *mut u64) = current_page.offset.to_le();
                    // Write the length of the new page (0).
                    *((new_page.data as *mut u64).offset(1)) = 0;

                    current_page = new_page;
                } else {
                    // push
                    let p = self.free_pages
                        .pop()
                        .unwrap_or_else(|| self.free_clean_pages.pop().unwrap());
                    debug!("commit: push {}", p);

                    // increase length.
                    *((current_page.data as *mut u64).offset(1)) = (len + 1).to_le();
                    // write pointer.
                    *((current_page.data as *mut u64).offset(2 + len as isize)) = p.to_le();
                }
            }
            // Take lock
            {
                debug!("commit: taking local lock");
                *self.env.lock.write().unwrap();
                if let MemSource::File { ref lock_file, .. } = self.env.mem_source {
                    debug!("commit: taking file lock");
                    lock_file.lock_exclusive().unwrap();
                }
                debug!("commit: lock ok");
                for (u, v) in self.roots.iter() {
                    *((self.env.map.offset(ZERO_HEADER) as *mut u64).offset(*u as isize)) =
                        (*v).to_le();
                }
                // synchronize all maps. Since PAGE_SIZE is not always
                // an actual page size, we flush the first two pages
                // last, instead of just the last one.
                debug!("env.length = {:?}", self.env.length);
                if let MemSource::File { ref mmap, .. } = self.env.mem_source {
                    mmap.flush_range(2 * PAGE_SIZE, (self.env.length - 2 * PAGE_SIZE_64) as usize)?;
                }
                *((self.env.map as *mut u64).offset(OFF_MAP_LENGTH))
                    = self.last_page.to_le();
                *((self.env.map as *mut u64).offset(OFF_CURRENT_FREE))
                    = current_page.offset.to_le();
                if let MemSource::File { ref mmap, .. } = self.env.mem_source {
                    mmap.flush_range(0, 2 * PAGE_SIZE)?;
                }

                if let MemSource::File { ref lock_file, .. } = self.env.mem_source {
                    debug!("commit: releasing lock");
                    lock_file.unlock().unwrap();
                }
                {
                    let mut last = self.env.first_unused_page.lock().unwrap();
                    debug!("last_page = {:?}", self.last_page);
                    *last = self.last_page;
                }
                if let Some(guard) = std::mem::replace(&mut self.mutable, None) {
                    debug!("dropping guard");
                    *guard
                }

                Ok(())
            }
        }
    }
}

impl<'env> Commit for MutTxn<'env> {
    /// Commit a transaction. This is guaranteed to be atomic: either
    /// the commit succeeds, and all the changes made during the
    /// transaction are written to disk. Or the commit doesn't
    /// succeed, and we're back to the state just before starting the
    /// transaction.
    fn commit(self) -> Result<(), Error> {
        let mut txn = self;
        while txn.parent.is_some() {
            txn = txn.commit_to_parent();
        }
        txn.commit_to_env()
    }
}
