#![feature(conservative_impl_trait, try_from)]

//! `sanakirja_facade` is a strongly typed, (hopefully) safe interface for a persistent, fast
//! key-value store (namely, [sanakirja](https://pijul.com/documentation/sanakirja/)).
//!
//! # Overview
//!
//! `sanakirja_facade` is transactional: writes either completely succeed or completely fail, with
//! no changes made. Data can be read while a write transaction is in process, and they will not
//! see any of the pending changes. When the write transaction wants to commit its changes, it
//! will need to wait until all read transactions are finished.
//!
//! In contrast to databases like BerkeleyDB and LMDB, `sanakirja_facade` has a strongly-typed
//! interface. That is, `sanakirja_facade` allows you to store and retrieve native rust types,
//! instead of requiring you to serialize everything as byte arrays. You can even extend the
//! universe of types that `sanakirja_facade` knows how to store.
//!
//! For managing its storage, `sanakirja_facade` borrows a technique popularized by
//! [LMDB](https://symas.com/lightning-memory-mapped-database/): the database file is mapped into
//! a large, contiguous chunk of memory. This is simple and very efficient, but it has one
//! implication that you should know about: when you first open the database environment, you get
//! to decide how large the memory mapping is. (Equivalently, you need to put an upper bound on how
//! large the database is allowed to grow.) This limitation exists because it isn't possible to
//! (portably and reliably) *grow* memory mappings once they've been made.
//!
//! # Using
//!
//! ## The weakly typed interface
//!
//! There are two interfaces to `sanakirja_facade`, one of which is more strongly typed than the
//! other. We'll start by describing the weakly typed interface, because the concepts involved are
//! also useful for understanding the strongly typed interface.
//!
//! The weakly typed interface is built around the [`Env`][Env], [`ReadTxn`][ReadTxn], and
//! [`WriteTxn`][WriteTxn] types, but if you only want to read from the database then you only need
//! the first two. An `Env` (pronounced "environment") contains a bunch of "root databases," each
//! of which is basically an ordered map. To get read access to these databases, we need to open a
//! `ReadTxn` (pronounced, "read transaction"); to get write access, we need to open a `WriteTxn`
//! ("write transaction").
//!
//! Here's an example where we open an environment for reading only.
//!
//! ```
//! # extern crate sanakirja_facade;
//! # use sanakirja_facade::*;
//!
//! # fn main() {
//!     // Read an Env from a file (commented out, because you should supply your own path).
//!     // let env = Env::open("my_db").expect("couldn't open env");
//!     # let env = Env::open_anonymous(PAGE_SIZE * 100).unwrap();
//!     # {
//!     #     let writer = env.writer().unwrap();
//!     #     writer.create_root_db::<u64,u64>(0).unwrap().insert(&1, &2).unwrap();
//!     #     writer.commit().unwrap();
//!     # }
//!     let reader = env.reader().unwrap();
//!     // We happen to know that the root database number zero is a map from `u64` to `u64`.
//!     let db = reader.root_db::<u64, u64>(0).unwrap();
//!     println!("{:?}", db.get(&1));
//! # }
//! ```
//!
//! The main issue with the weakly typed interface is that every time we access a root database, we
//! need to tell sanakirja which types we expect it to contain. Sanakirja can't help us if we get
//! it wrong: if we had said `<u32, u32>` instead of `<u64, u64>`, sanakirja wouldn't have known
//! enough to complain. Instead we would have gotten back a map containing garbage data.
//!
//! Anyway, let's talk about writing and how it interacts with reading. You open a writer with the
//! [`Env::writer`][Env::writer] method. From there, you can open existing root databases using
//! [`WriteTxn::root_db`][WriteTxn::root_db] or create new ones using
//! [`WriteTxn::create_root_db`][WriteTxn::create_root_db]. Then you can modify those databases by
//! adding or deleting entries.
//!
//! The crucial point about `WriteTxn`s is that any modifications you make are invisible to any
//! active `ReadTxn`s. This is a consequence of sanakirja's transactional design: all of the
//! changes you make in a `WriteTxn` are batched up, and only actually written when you call
//! [`WriteTxn::commit`][WriteTxn::commit].
//!
//! ```
//! # extern crate sanakirja_facade;
//! # use sanakirja_facade::*;
//!
//! # fn main() {
//!     // Create an empty environment, not backed by any file.
//!     let env = Env::open_anonymous(PAGE_SIZE * 100).unwrap();
//!     let writer = env.writer().unwrap();
//!     {   // Make a new scope for creating a root database, since our reference
//!         // to the root database needs to go out of scope before we commit our
//!         // transaction.
//!         let mut db = writer.create_root_db::<u64, u64>(0).unwrap();
//!         db.insert(&1, &2);
//!
//!         // None of the changes we just did can be seen by a `ReadTxn`: it doesn't
//!         // even think there are any root databases yet.
//!         let reader = env.reader().unwrap();
//!         assert!(reader.root_db::<u64, u64>(0).is_none());
//!     }
//!     // Now commit the writer. Readers that we create after this will see the changes.
//!     writer.commit();
//!     let reader = env.reader().unwrap();
//!     let db = reader.root_db::<u64, u64>(0).unwrap();
//!     assert_eq!(db.get(&1), Some(2));
//! # }
//! ```
//!
//! ## The strongly typed interface
//!
//! The strongly typed interface has one major advantage over the weakly typed one: you don't have
//! to remember, every time you open a root database, what types are stored in it. Instead, you
//! declare the types once, and sanakirja_facade will generate typesafe wrappers for you.  To use
//! the strongly typed interface, you will need to add a dependency on the
//! `sanakirja_facade_derive` crate, by adding
//!
//! ```text
//! [dependencies]
//! sanakirja_facade_derive = "0.1.0"
//! ```
//!
//! to your `Cargo.toml` file, and then adding
//!
//! ```text
//! #[macro_use] extern crate sanakirja_facade_derive;
//! ```
//!
//! at the top-level of your crate. Once you've done that, you can auto-derive your own strongly
//! typed database interfaces like this:
//!
//! ```
//! # extern crate sanakirja_facade;
//! # #[macro_use] extern crate sanakirja_facade_derive;
//! use sanakirja_facade::*;
//!
//! // This #[derive(TypedEnv)] creates three new structs: `MyEnv`, `MyReadTxn`, and `MyWriteTxn`.
//! //
//! // `MyEnv` is a bit like `Env`, except that the `reader()` function returns a `MyReadTxn` and
//! // the `writer()` function returns a `MyWriteTxn`.
//! //
//! // `MyReadTxn` is a bit like `ReadTxn`, except that instead of accessing the root databases
//! // using numeric indices, there are non-generic, named functions for accessing both of the
//! // root databases that we declare below.
//! //
//! // `MyWriteTxn` is a bit like `WriteTxn`, except that it also uses named functions instead of
//! // indices for accessing the root databases.
//! #[derive(TypedEnv)]
//! struct MySchema<'env> {
//!     // The first map in my environment maps from `u64`s to large buffers.
//!     my_first_db: Db<'env, u64, sanakirja_facade::LargeBuf<'env>>,
//!     // Dbs can be nested in other Dbs.
//!     my_nested_db: Db<'env, u64, Db<'env, u64, u64>>,
//! }
//!
//! fn main() {
//!     // Create an empty, memory-backed database.
//!     let env = MyEnv::open_anonymous(PAGE_SIZE * 100).unwrap();
//!     let writer = env.writer().unwrap();
//!     writer.my_first_db().insert(&123, &b"Here's a buffer with text in it."[..]).unwrap();
//!
//!     // In order to write nested Dbs into our database, we need to create them first.
//!     let mut db = writer.create_db().unwrap();
//!     db.insert(&7, &8).unwrap();
//!     writer.my_nested_db().insert(&234, &db).unwrap();
//!
//!     // If we were to call env.reader(), it would look as though our database were empty.
//!     // That's because of sanakirja's transactional approach. If we want to actually see
//!     // the data that we just inserted, we can either call `writer.snapshot()` to get
//!     // a reader with a snapshot of the current state, or we can call `writer.commit()` to
//!     // finish our transaction before getting a reader with `env.reader()`. Let's do the
//!     // first option:
//!     let reader = writer.snapshot();
//!     let buf = reader.my_first_db().get(&123).unwrap();
//!     assert_eq!(buf.iter().next().unwrap(), &b"Here's a buffer with text in it."[..]);
//!     let db = reader.my_nested_db().get(&234).unwrap();
//!     assert_eq!(db.get(&7), Some(8));
//! }
//! ```
//!
//! # Extending
//!
//! This section is not written yet...
//!
//! [Env]: struct.Env.html
//! [Env::writer]: struct.Env.html#method.writer
//! [ReadTxn]: struct.ReadTxn.html
//! [WriteTxn]: struct.WriteTxn.html
//! [WriteTxn::commit]: struct.WriteTxn.html#method.commit
//! [WriteTxn::root_db]: struct.WriteTxn.html#method.root_db
//! [WriteTxn::create_root_db]: struct.WriteTxn.html#method.create_root_db

extern crate byteorder;
extern crate rand;

pub extern crate sanakirja;

use sanakirja::{Commit, LoadPage, Transaction};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::Path;

mod buf;
mod db;
mod representable;

use representable::Wrapper;

pub use buf::LargeBuf;
pub use db::{Db, RootWriteDb, WriteDb};
pub use representable::{StoredHeader, Storable, Stored};
pub use sanakirja::Alignment;
pub type Error = sanakirja::Error;
pub type Result<T> = std::result::Result<T, Error>;

// Size, in bytes, of the header on page 0.
const ZERO_HEADER: isize = 24;

/// Represents the chunk of physical memory backing the entire database.
///
/// The main thing you can do with a `Storage` is to request `PAGE_SIZE`-sized chunks of
/// (read-only) memory.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Storage<'env> {
    base: *mut u8,
    len: usize,
    marker: PhantomData<&'env u8>,
}

impl<'env> Storage<'env> {
    pub fn get_page(&self, offset: usize) -> &'env [u8] {
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

impl<'env> LoadPage for Storage<'env> {
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
struct MutStorage<'env> {
    base: *mut u8,
    len: usize,
    marker: PhantomData<&'env u8>,
}

impl<'env> LoadPage for MutStorage<'env> {
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

impl<'env> sanakirja::skiplist::SkipList for Storage<'env> {}
impl<'env> sanakirja::Transaction for Storage<'env> {}
impl<'env> sanakirja::skiplist::SkipList for MutStorage<'env> {}
impl<'env> sanakirja::Transaction for MutStorage<'env> {}

/// The size of a page in the database. This may not be the same as the page size of the underlying
/// platform.
pub const PAGE_SIZE: usize = 4096;

/// A reference to a mutable page in the database.
///
/// We promise that at any time, there is only one live `MutPage` pointing to any one page.
#[derive(Debug)]
pub struct MutPage<'env> {
    storage: Storage<'env>,
    offset: usize,
}

impl<'env> MutPage<'env> {
    /// Returns this page as a mutable slice, which is guaranteed to have length `PAGE_SIZE`.
    pub fn buf(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.storage.base.offset(self.offset as isize), PAGE_SIZE)
        }
    }

    /// Returns the offset (measured from the start of the environment's backing memory) of this
    /// page. This is guaranteed to be a multiple of `PAGE_SIZE`.
    pub fn offset(&self) -> usize {
        self.offset
    }
}

/// You'll need to know about `Alloc` if you want to write your own advanced impls for `Stored`.
///
/// Specifically, suppose you want to implement `Stored` for a "borrow" into a large chunk of
/// memory that's stored in the database. In this case, you will need to allocated and fill that
/// memory at some point. That's what this `Alloc` struct is for: you will get access to one when
/// you implement `Storable::store`, and you can use it to request free, `PAGE_SIZE`-sized chunks
/// of memory.
///
/// For a higher-level overview on implementing `Stored`, see the crate documentation.
pub struct Alloc<'a, 'env: 'a> {
    txn: &'a WriteTxn<'env>
}

impl<'a, 'env: 'a> Alloc<'a, 'env> {
    /// Allocate a `PAGE_SIZE`-sized chunk of memory in the database. Don't forget to free it when
    /// you're done (probably in the `Stored::drop_value` function)!
    pub fn alloc_page(&mut self) -> Result<MutPage<'env>> {
        let sk_page = self.txn.with_mut_txn(|txn| txn.alloc_page())?;
        Ok(MutPage {
            storage: self.txn.storage(),
            offset: sk_page.offset.try_into().unwrap(),
        })
    }

    /// Free the page at offset `offset`, which must be a multiple of `PAGE_SIZE`.
    ///
    /// You need to be careful to free only pages which are no longer in use; otherwise, you could
    /// cause database corruption.
    ///
    /// # Panics
    ///
    /// Panics if `offset` is not a multiple of `PAGE_SIZE`, or if it points past the end of the
    /// database.
    pub fn free_page(&mut self, offset: usize) {
        if offset % PAGE_SIZE != 0 {
            panic!("offset must be a multiple of PAGE_SIZE");
        }
        self.txn.with_mut_txn(|txn| unsafe { txn.free_page(offset as u64) });
    }

    /// Returns the storage underlying this allocator.
    pub fn storage(&self) -> Storage<'env> {
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
pub struct ReadTxn<'env> {
    storage: Storage<'env>,
    // This is useful for a ReadTxn that's obtained from snapshotting a WriteTxn. In that case, the
    // WriteTxn probably has some modified root Dbs, meaning that the modified Dbs' first pages are
    // not stored in the first page of the storage. Here, we keep track of where they actually are.
    roots: HashMap<usize, u64>,
}

enum WrappedTxn<'env> {
    // The empty variant is needed because at some point we need to move the MutTxn out of a
    // RefCell, and we need a temporary value to put there before moving a different MutTxn back
    // in. But you should basically never expect to see a WrappedTxn::Empty.
    Empty,
    Owned(sanakirja::MutTxn<'env>),
    // We use a pointer here instead of a borrow to avoid having an extra lifetime parameter. In
    // order to maintain memory safety, you need to be careful not to let a WrappedTxn::Borrowed
    // last too long. Fortunately, we only create them in one place:
    // (namely, <Wrapper as Representable>::drop_value).
    Borrowed(*mut sanakirja::MutTxn<'env>),
}

/// A write-only view into an environment.
///
/// There can only be one simultaneous write-only view to any environment, but read-only views
/// (i.e. `ReadTxn`s) can co-exist with a `WriteTxn`. In this case, the `ReadTxn`s will see stale
/// data: any modifications made by the `WriteTxn` will not be reflected in the `ReadTxn`s.
pub struct WriteTxn<'env> {
    sk_txn: RefCell<WrappedTxn<'env>>,
    // We only allow one copy of a RootWriteDb at a time. Here, we maintain a list of outstanding
    // borrows.
    borrowed_roots: RefCell<HashSet<usize>>,
}

impl Env {
    /// Returns the size (in bytes) of the database file living in `path`, which should point to a
    /// directory.
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        sanakirja::Env::file_size(path)
    }

    /// Opens the environment stored in `path`, which should point to a directory.
    ///
    /// This method is mainly for read-only access, since the returned environment may not have any
    /// free space for modifications.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Env> {
        Env::open_and_grow_to(path, 0)
    }

    /// Opens the environment stored in `path`, which should point to a directory.
    ///
    /// If `max_length` is larger than the current size of the environment, the environment will
    /// grow to `max_length`. Because of the way memory-mapped files work, this can be made very
    /// large without any performance penalty.
    pub fn open_and_grow_to<P: AsRef<Path>>(path: P, max_length: usize) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new(path, max_length as u64)?)
        })
    }

    /// Creates a new environment that is backed by memory only.
    ///
    /// This is mainly useful for testing.
    pub fn open_anonymous(length: usize) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new_memory_backed(length as u64)?)
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
    /// # let env = sanakirja_facade::Env::open_anonymous(4096).unwrap();
    /// let first = env.writer();
    /// let second = env.writer();
    /// assert!(first.is_ok());
    /// assert!(second.is_err());
    /// ```
    pub fn writer<'env>(&'env self) -> Result<WriteTxn<'env>> {
        Ok(WriteTxn {
            sk_txn: RefCell::new(WrappedTxn::Owned(self.sk_env.mut_txn_begin()?)),
            borrowed_roots: RefCell::new(HashSet::new()),
        })
    }
}

impl<'env> ReadTxn<'env> {
    pub(crate) fn storage(&self) -> Storage<'env> {
        self.storage
    }

    /// Returns a read-only view into one of the "root" databases in this environment.
    ///
    /// An environment can have up to 508 root databases, which are numbered starting from zero.
    /// There is (not yet, at least) no way for the environment to know which types are stored in
    /// which of these databases, so you'll need to supply your own type parameters (and they'd
    /// better be correct, or else you'll be reading garbage).
    pub fn root_db<K: Stored<'env>, V: Stored<'env>>(&self, idx: usize) -> Option<Db<'env, K, V>> {
        let sk_db = if let Some(offset) = self.roots.get(&(1 + idx)) {
            Some(sanakirja::Db(*offset, std::marker::PhantomData))
        } else {
            self.storage.root(idx)
        };
        sk_db.map(|db| Db { sk_db: db, storage: self.storage() })
    }
}

impl<'env> WriteTxn<'env> {
    pub(crate) fn storage(&self) -> Storage<'env> {
        self.with_txn(|txn| {
            Storage {
                base: txn.base(),
                len: txn.len() as usize,
                marker: std::marker::PhantomData,
            }
        })
    }

    pub(crate) fn allocator<'a>(&'a self) -> Alloc<'a, 'env> {
        Alloc { txn: self }
    }

    /// Returns a write-only view into one of the "root" databases in this environment. See
    /// `ReadTxn::root_db` for more about root databases.
    ///
    /// Each root database can be viewed only once at a time. This is enforced at run-time: if you
    /// call `root_db(0)` while the result of a previous `root_db(0)` is still alive, this function
    /// will panic. (However, you are allowed to call `root_db(1)` while the result of a previous
    /// `root_db(0)` is still alive.
    ///
    /// Note that you get to specify your own type parameters. The environment doesn't actually
    /// know what types are stored in its root databases, so you have to provide the correct type
    /// parameters. If you don't, database corruption awaits!
    ///
    /// Because of the way copy-on-write works in sanakirja, it's a bad idea to create multiple
    /// simultaneous copies of the same root database. If you make two copies and modify them both,
    /// you could end up with two different modified copies of the root database, living in
    /// different parts of the backing memory. Since only one of these copies will eventually be
    /// saved as the new root database, you might end up missing some of the data that you thought
    /// you had inserted.
    ///
    /// # Example
    ///
    /// ```
    /// # let env = sanakirja_facade::Env::open_anonymous(4096 * 10).unwrap();
    /// # {
    /// #     let mut w = env.writer().unwrap();
    /// #     w.create_root_db::<u64, u64>(0).unwrap();
    /// #     w.commit().unwrap();
    /// # }
    /// let writer = env.writer().unwrap();
    /// // Since `root_db` borrows the transaction, create a smaller scope for that borrow to live in.
    /// {
    ///     // Get the first root database. We happen to know that it's a Db<u64, u64>.
    ///     let mut write_db = writer.root_db::<u64, u64>(0).unwrap();
    ///     write_db.insert(&1, &23).unwrap();
    /// }
    /// let reader = writer.snapshot();
    /// let read_db = reader.root_db::<u64, u64>(0).unwrap();
    /// assert!(read_db.get(&1) == Some(23));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `idx >= 508`, since there is room for at most 508 root databases.
    ///
    /// Panics if you try to access a root database that is already being accessed.
    /// ```
    pub fn root_db<'txn, K: Stored<'env>, V: Stored<'env>>(&'txn self, idx: usize) -> Option<RootWriteDb<'txn, 'env, K, V>> {
        let sk_db = self.with_txn(|txn| txn.root(idx));
        if !self.borrowed_roots.borrow_mut().insert(idx) {
            panic!("tried to borrow root {:?} multiple times", idx);
        }
        sk_db.map(|db| {
            RootWriteDb {
                idx: idx,
                db: WriteDb {
                    sk_db: db,
                    txn: self,
                }
            }
        })
    }

    /// Creates a new write-only database.
    pub fn create_db<'txn, K: Stored<'env>, V: Stored<'env>>(&'txn self)
    -> Result<WriteDb<'txn, 'env, K, V>> {
        let sk_db = self.with_mut_txn(|txn| txn.create_db())?;
        Ok(WriteDb {
            sk_db: sk_db,
            txn: self,
        })
    }

    /// Creates a new write-only database, anchored at the environment root.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= 508`, since there is room for at most 508 root databases.
    ///
    /// Panics if the index points to another existing database, which is currently being accessed.
    pub fn create_root_db<'txn, K: Stored<'env>, V: Stored<'env>>(&'txn self, idx: usize)
    -> Result<RootWriteDb<'txn, 'env, K, V>> {
        let ret = RootWriteDb {
            db: self.create_db()?,
            idx: idx,
        };
        self.set_root_db(idx, &ret.db);
        if !self.borrowed_roots.borrow_mut().insert(idx) {
            panic!("tried to create root {:?}, but it is being accessed", idx);
        }
        Ok(ret)
    }

    /// Designates `db` as a "root" database. See `ReadTxn::root_db` for more about root databases.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= 508`, since there is room for at most 508 root databases.
    fn set_root_db<'txn, K: Stored<'env>, V: Stored<'env>>(&self, idx: usize, db: &WriteDb<'txn, 'env, K, V>) {
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
    where F: FnOnce(&mut sanakirja::MutTxn<'env>) -> Out
    {
        match *self.sk_txn.borrow_mut() {
            WrappedTxn::Empty => panic!("we should never have an empty transaction"),
            WrappedTxn::Owned(ref mut txn) => f(txn),
            WrappedTxn::Borrowed(txn_ptr) => unsafe { f(&mut *txn_ptr) },
        }
    }

    pub(crate) fn with_txn<F, Out>(&self, f: F) -> Out
    where F: FnOnce(&sanakirja::MutTxn<'env>) -> Out
    {
        match *self.sk_txn.borrow() {
            WrappedTxn::Empty => panic!("we should never have an empty transaction"),
            WrappedTxn::Owned(ref txn) => f(txn),
            WrappedTxn::Borrowed(txn_ptr) => unsafe { f(&*txn_ptr) },
        }
    }

    pub(crate) fn from_mut_ref(txn: &mut sanakirja::MutTxn<'env>) -> WriteTxn<'env> {
        WriteTxn {
            sk_txn: RefCell::new(WrappedTxn::Borrowed(txn as *mut _)),
            borrowed_roots: RefCell::new(HashSet::new()),
        }
    }
}
