#![feature(conservative_impl_trait, try_from)]

extern crate byteorder;
extern crate either;
extern crate rand;
extern crate sanakirja;

use sanakirja::{LoadPage, Transaction};
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::Path;

mod buf;
mod db;
mod representable;

use representable::Wrapper;

pub use buf::LargeBuf;
pub use db::{Db, WriteDb};
pub use representable::{StoredHeader, Storable, Stored};
pub use sanakirja::{Alignment, Commit};
pub type Error = sanakirja::Error;
pub type Result<T> = std::result::Result<T, Error>;

// Size, in bytes, of the header on page 0.
const ZERO_HEADER: isize = 24;

/// Represents the chunk of physical memory backing the entire database.
///
/// The main thing you can do with a `Storage` is to request `PAGE_SIZE`-sized chunks of
/// (read-only) memory.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Storage<'sto> {
    base: *mut u8,
    len: usize,
    marker: PhantomData<&'sto u8>,
}

impl<'sto> Storage<'sto> {
    pub fn get_page(&self, offset: usize) -> &'sto [u8] {
        if offset % (PAGE_SIZE as usize) != 0 {
            panic!("tried to load an unaligned page");
        }
        if offset.checked_add(PAGE_SIZE as usize).unwrap() > self.len {
            panic!("tried to load an invalid page");
        }
        unsafe {
            // The cast to isize is ok, because self.len is at most isize::MAX
            std::slice::from_raw_parts(self.base.offset(offset as isize), PAGE_SIZE as usize)
        }
    }
}

impl<'sto> LoadPage for Storage<'sto> {
    fn load_page(&self, off: u64) -> sanakirja::Page {
        assert!(off < self.len as u64);
        unsafe {
            sanakirja::Page {
                data: self.base.offset(off as isize),
                offset: off
            }
        }
    }

    fn len(&self) -> u64 {
        self.len as u64
    }

    fn root_(&self, num: usize) -> u64 {
        assert!(ZERO_HEADER as usize + ((num + 1) << 3) < PAGE_SIZE);
        unsafe {
            u64::from_le(*((self.base.offset(ZERO_HEADER) as *const u64).offset(num as isize)))
        }
    }
}

/// A mutable view into the physical memory backing the entire database.
///
/// With one of these, you can request `PAGE_SIZE`-sized chunks of mutable memory. You need to be
/// careful what you do with this memory, and only write to the parts of it that you "own."
/// Writing elsewhere could cause database corruption (although not memory unsafety).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MutStorage<'sto> {
    base: *mut u8,
    len: usize,
    marker: PhantomData<&'sto u8>,
}

impl<'sto> LoadPage for MutStorage<'sto> {
    fn load_page(&self, off: u64) -> sanakirja::Page {
        assert!(off < self.len as u64);
        unsafe {
            sanakirja::Page {
                data: self.base.offset(off as isize) as *const u8,
                offset: off
            }
        }
    }

    fn len(&self) -> u64 {
        self.len as u64
    }

    fn root_(&self, num: usize) -> u64 {
        assert!(ZERO_HEADER as usize + ((num + 1) << 3) < PAGE_SIZE);
        unsafe {
            u64::from_le(*((self.base.offset(ZERO_HEADER) as *const u64).offset(num as isize)))
        }
    }
}

impl<'sto> sanakirja::skiplist::SkipList for Storage<'sto> {}
impl<'sto> sanakirja::Transaction for Storage<'sto> {}
impl<'sto> sanakirja::skiplist::SkipList for MutStorage<'sto> {}
impl<'sto> sanakirja::Transaction for MutStorage<'sto> {}

/// The size of a page in the database. This may not be the same as the page size of the underlying
/// platform.
pub const PAGE_SIZE: usize = 4096;

pub struct MutPage<'sto> {
    storage: Storage<'sto>,
    offset: usize,
}

impl<'sto> MutPage<'sto> {
    pub fn buf(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.storage.base.offset(self.offset as isize), PAGE_SIZE)
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

pub struct Alloc<'a, 'sto: 'a> {
    txn: &'a WriteTxn<'sto>
}

impl<'a, 'sto: 'a> Alloc<'a, 'sto> {
    pub fn alloc_page(&mut self) -> Result<MutPage<'sto>> {
        let sk_page = self.txn.with_mut_txn(|txn| txn.alloc_page())?;
        Ok(MutPage {
            storage: self.txn.storage(),
            offset: sk_page.offset.try_into().unwrap(),
        })
    }

    pub fn free_page(&mut self, offset: usize) {
        self.txn.with_mut_txn(|txn| unsafe { txn.free_page(offset as u64) });
    }

    pub fn storage(&self) -> Storage<'sto> {
        self.txn.storage()
    }
}

/// The environment containing all the databases.
///
/// All the databases in an environment share a chunk of memory, and an environment is backed on
/// disk by a single file.
///
/// TODO: discuss synchronization and transactions
pub struct Env {
    sk_env: Box<sanakirja::Env>
}

/// A read-only view into an environment.
///
/// There can be multiple `ReadTxn`s active for the same environment, and they can also coexist
/// with a `WriteTxn` (although in that case you'll need to construct the `ReadTxn` by using
/// `WriteTxn::snapshot`).
///
/// In order to do anything useful with a `ReadTxn`, you'll want to retrieve a `Db` using
/// `ReadTxn::root_db`.
pub struct ReadTxn<'sto> {
    storage: Storage<'sto>,
    // This is useful for a ReadTxn that's obtained from snapshotting a WriteTxn. In that case, the
    // WriteTxn probably has some modified root Dbs, meaning that the modified Dbs' first pages are
    // not stored in the first page of the storage. Here, we keep track of where they actually are.
    roots: HashMap<usize, u64>,
}

enum WrappedTxn<'sto> {
    // The empty variant is needed because at some point we need to move the MutTxn out of a
    // RefCell, and we need a temporary value to put there before moving a different MutTxn back
    // in. But you should basically never expect to see a WrappedTxn::Empty.
    Empty,
    Owned(sanakirja::MutTxn<'sto>),
    // We use a pointer here instead of a borrow to avoid having an extra lifetime parameter. In
    // order to maintain memory safety, you need to be careful not to let a WrappedTxn::Borrowed
    // last too long. Fortunately, we only create them in one place:
    // (namely, <Wrapper as Representable>::drop_value).
    Borrowed(*mut sanakirja::MutTxn<'sto>),
}

/// A write-only view into an environment.
///
/// There can only be one simultaneous write-only view to any environment, but read-only views
/// (i.e. `ReadTxn`s) can co-exist with a `WriteTxn`. In this case, the `ReadTxn`s will see stale
/// data: any modifications made by the `WriteTxn` will not be reflected in the `ReadTxn`s.
pub struct WriteTxn<'sto> {
    sk_txn: RefCell<WrappedTxn<'sto>>
}

impl Env {
    /// Returns the size (in bytes) of the database file living in `path`, which should point to a
    /// directory.
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        sanakirja::Env::file_size(path)
    }

    /// Opens the environment stored in `path`, which should point to a directory.
    ///
    /// `max_length` specifies the maximum size (in bytes) that the database may grow to. This can
    /// be made very large without any performance penalty.
    pub fn open<P: AsRef<Path>>(path: P, max_length: u64) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new(path, max_length)?)
        })
    }

    /// Creates a new environment that is backed by memory only.
    ///
    /// This is mainly useful for testing.
    pub fn memory_backed(length: u64) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new_memory_backed(length)?)
        })
    }

    /// Begins a read-only transaction.
    pub fn reader<'env>(&'env self) -> Result<ReadTxn<'env>> {
        let sk_txn = self.sk_env.txn_begin()?;
        let storage = Storage {
            base: sk_txn.load_page(0).data as *mut u8,
            len: sk_txn.len() as usize,
            marker: std::marker::PhantomData,
        };
        Ok(ReadTxn {
            storage,
            roots: HashMap::new(),
        })
    }

    /// Begins a write-only transaction.
    ///
    /// Only one such transaction can be active at once; this is enforced by runtime checks: if you
    /// try to open a second writer, this method will return an error:
    ///
    /// ```
    /// # let env = sanakirja_facade::Env::memory_backed(4096).unwrap();
    /// let first = env.writer();
    /// let second = env.writer();
    /// assert!(first.is_ok());
    /// assert!(second.is_err());
    /// ```
    pub fn writer<'env>(&'env self) -> Result<WriteTxn<'env>> {
        Ok(WriteTxn {
            sk_txn: RefCell::new(WrappedTxn::Owned(self.sk_env.mut_txn_begin()?)),
        })
    }
}

impl<'sto> ReadTxn<'sto> {
    pub(crate) fn storage(&self) -> Storage<'sto> {
        self.storage
    }

    /// Returns a read-only view into one of the "root" databases in this environment.
    ///
    /// An environment can have up to 508 root databases, which are numbered starting from zero.
    /// There is (not yet, at least) no way for the environment to know which types are stored in
    /// which of these databases, so you'll need to supply your own type parameters (and they'd
    /// better be correct, or else you'll be reading garbage).
    pub fn root_db<K: Stored<'sto>, V: Stored<'sto>>(&self, idx: usize) -> Db<'sto, K, V> {
        let sk_db = if let Some(offset) = self.roots.get(&(1 + idx)) {
            println!("found a moved root db: offset {:?}", *offset);
            sanakirja::Db(*offset, std::marker::PhantomData)
        } else {
            self.storage.root(idx).unwrap()
        };
        Db {
            sk_db: sk_db,
            storage: self.storage(),
        }
    }
}

impl<'sto> WriteTxn<'sto> {
    pub(crate) fn storage(&self) -> Storage<'sto> {
        self.with_txn(|txn| {
            Storage {
                base: txn.base(),
                len: txn.len() as usize,
                marker: std::marker::PhantomData,
            }
        })
    }

    pub(crate) fn allocator<'a>(&'a self) -> Alloc<'a, 'sto> {
        Alloc { txn: self }
    }

    /// Returns a write-only copy of one of the "root" databases in this environment. See
    /// `ReadTxn::root_db` for more about root databases.
    ///
    /// *Important*: this is a write-only *copy* of the root database, so any changes you make to
    /// it won't be reflected in the original database. If you want to actually change the root
    /// database, you'll want to call `WriteTxn::set_root_sb` after making your changes. Here's an
    /// example:
    ///
    /// ```
    /// # let env = sanakirja_facade::Env::memory_backed(4096 * 10).unwrap();
    /// # {
    /// #     let w = env.writer().unwrap();
    /// #     {
    /// #         let db = w.create_db::<u64, u64>();
    /// #         println!("created db {:?}", db);
    /// #         w.set_root_db(0, db);
    /// #     }
    /// #     w.commit().unwrap();
    /// # }
    /// let writer = env.writer().unwrap();
    /// // Get the first root database. We happen to know that it's a Db<u64, u64>.
    /// let mut write_db = writer.root_db::<u64, u64>(0);
    /// write_db.insert(1, 23).unwrap();
    /// let reader = writer.snapshot();
    /// // Here, we're getting the unmodified Db.
    /// let read_db = reader.root_db::<u64, u64>(0);
    /// assert!(read_db.get(&1).is_none());
    ///
    /// // Let's try reading again, but first write the modified Db back.
    /// writer.set_root_db(0, write_db);
    /// let reader = writer.snapshot();
    /// // Since we called set_root_db above, we're getting the modified database now.
    /// let read_db = reader.root_db::<u64, u64>(0);
    /// assert!(read_db.get(&1) == Some(23));
    /// ```
    pub fn root_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&'txn self, idx: usize) -> WriteDb<'txn, 'sto, K, V> {
        let sk_db = self.with_txn(|txn| txn.root(idx).unwrap());
        WriteDb {
            sk_db: sk_db,
            txn: self,
        }
    }

    /// Creates a new write-only database.
    pub fn create_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&'txn self) -> WriteDb<'txn, 'sto, K, V> {
        // TODO: this can fail if there's not enough space
        let sk_db = self.with_mut_txn(|txn| txn.create_db().unwrap());
        WriteDb {
            sk_db: sk_db,
            txn: self,
        }
    }

    /// Designates `db` as a "root" database. See `ReadTxn::root_db` for more about root databases.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= 508`, since there is room for at most 508 root databases.
    pub fn set_root_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&self, idx: usize, db: WriteDb<'txn, 'sto, K, V>) {
        self.with_mut_txn(|txn| txn.set_root(idx, db.sk_db));
    }

    /// Commits the changes from this transaction to disk.
    ///
    /// This may fail, but it is guaranteed to be atomic. That is, either all of the changes from
    /// this transaction will be committed, or none will.
    pub fn commit(self) -> Result<()> {
        match self.sk_txn.into_inner() {
            WrappedTxn::Empty => panic!("tried to commit an empty transaction"),
            WrappedTxn::Borrowed(_) => panic!("tried to commit a borrowed transaction"),
            WrappedTxn::Owned(txn) => txn.commit()?,
        }
        Ok(())
    }

    /// Creates a read-only snapshot of this transaction.
    ///
    /// All of the changes that this `WriteTxn` made will be reflected in the snapshot, but any
    /// subsequent changes won't be.
    pub fn snapshot<'a>(&'a self) -> ReadTxn<'a> {
        let roots = self.with_txn(|t| t.roots.clone());
        println!("creating snapshot. roots {:?}", roots);
        // This scope ensures that we drop txn_ref before calling self.storage(), which needs to
        // borrow self.sk_txn.
        {
            let mut txn_ref = self.sk_txn.borrow_mut();
            let mut old_txn = WrappedTxn::Empty;
            std::mem::swap(&mut *txn_ref, &mut old_txn);
            let old_txn = match old_txn {
                WrappedTxn::Owned(t) => t,
                _ => panic!("tried to snapshot an invalid transaction")
            };
            *txn_ref = WrappedTxn::Owned(old_txn.mut_txn_begin().unwrap());
        }

        let s = self.storage();
        ReadTxn {
            storage: Storage {
                base: s.base,
                len: s.len,
                marker: s.marker,
            },
            roots: roots,
        }
    }

    pub(crate) fn with_mut_txn<F, Out>(&self, f: F) -> Out
    where F: FnOnce(&mut sanakirja::MutTxn<'sto>) -> Out
    {
        match *self.sk_txn.borrow_mut() {
            WrappedTxn::Empty => panic!("we should never have an empty transaction"),
            WrappedTxn::Owned(ref mut txn) => f(txn),
            WrappedTxn::Borrowed(txn_ptr) => unsafe { f(&mut *txn_ptr) },
        }
    }

    pub(crate) fn with_txn<F, Out>(&self, f: F) -> Out
    where F: FnOnce(&sanakirja::MutTxn<'sto>) -> Out
    {
        match *self.sk_txn.borrow() {
            WrappedTxn::Empty => panic!("we should never have an empty transaction"),
            WrappedTxn::Owned(ref txn) => f(txn),
            WrappedTxn::Borrowed(txn_ptr) => unsafe { f(&*txn_ptr) },
        }
    }

    pub(crate) fn from_mut_ref(txn: &mut sanakirja::MutTxn<'sto>) -> WriteTxn<'sto> {
        WriteTxn {
            sk_txn: RefCell::new(WrappedTxn::Borrowed(txn as *mut _))
        }
    }
}
