use byteorder::{ByteOrder, LittleEndian};
use rand;
use sanakirja;
use sanakirja::{Representable as SkRepr, Transaction};
use std;

use {Alignment, Alloc, StoredHeader, Result, Storable, Storage, Stored, Wrapper, WriteTxn};

fn cast<'env, K, V, J1, J2, U1, U2>(db: sanakirja::Db<Wrapper<'env, K, J1>, Wrapper<'env, V, U1>>)
-> sanakirja::Db<Wrapper<'env, K, J2>, Wrapper<'env, V, U2>>
where
K: Stored<'env>,
V: Stored<'env>,
J1: Storable<'env, K> + ?Sized,
J2: Storable<'env, K> + ?Sized,
U1: Storable<'env, V> + ?Sized,
U2: Storable<'env, V> + ?Sized,
{
    unsafe { std::mem::transmute(db) }
}

/// A `Db<K, V>` is an immutable ordered map from `K` to `V`.
pub struct Db<'env, K: Stored<'env>, V: Stored<'env>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'env, K, K>, Wrapper<'env, V, V>>,
    pub(crate) storage: Storage<'env>,
}

impl<'env, K: Stored<'env>, V: Stored<'env>> Copy for Db<'env, K, V> { }
impl<'env, K: Stored<'env>, V: Stored<'env>> Clone for Db<'env, K, V> {
    fn clone(&self) -> Self { *self }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> std::fmt::Debug for Db<'env, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Db({:?})", self.sk_db.0)
    }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> PartialEq for Db<'env, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.storage.base == other.storage.base
    }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> Eq for Db<'env, K, V> { }

impl<'env, K: Stored<'env>, V: Stored<'env>> PartialOrd for Db<'env, K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> Ord for Db<'env, K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk_db.0.cmp(&other.sk_db.0)
    }
}

struct Iter<'a, 'env: 'a, K: Stored<'env>, V: Stored<'env>, J: Storable<'env, K>, U: Storable<'env, V>> {
    cursor: sanakirja::Cursor<'a, Storage<'env>, Wrapper<'env, K, J>, Wrapper<'env, V, U>>,
    storage: Storage<'env>,
}

impl<'env, K: Stored<'env> + 'env, V: Stored<'env> + 'env> Db<'env, K, V> {
    /// Returns an iterator over all key/value pairs in this map, in increasing order.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(K, V)> + 'a {
        Iter {
            cursor: self.storage.iter(self.sk_db, None),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key`.
    pub fn iter_from_key<'a, Q: Storable<'env, K> + 'a>(&'a self, key: &Q)
    -> impl Iterator<Item=(K, V)> + 'a
    {
        let db = cast::<K, V, K, Q, V, V>(self.sk_db);
        Iter {
            cursor: self.storage.iter(db, Some((Wrapper::search(key), None))),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key` and whose value is at least `val`.
    pub fn iter_from_key_value<'a, Q: 'a, R: 'a>(&'a self, key: &Q, val: &R)
    -> impl Iterator<Item=(K, V)> + 'a
    where
    Q: Storable<'env, K>,
    R: Storable<'env, V>,
    {
        let db = cast(self.sk_db);
        Iter {
            cursor: self.storage.iter(db, Some((Wrapper::search(key), Some(Wrapper::search(val))))),
            storage: self.storage,
        }
    }

    /// Gets the smallest value associated with `key`, if there is one.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where Q: Storable<'env, K> + PartialEq<K>
    {
        if let Some((k, v)) = self.iter_from_key(key).next() {
            if key.eq(&k) {
                return Some(v);
            }
        }
        None
    }

    /// Creates a write-only copy of this map.
    ///
    /// Because of the way sanakirja does copy-on-write, this method is very fast (`O(1)`) and uses
    /// almost no memory. If you actually start writing to the writable copy, you'll start using up
    /// memory.
    ///
    /// # Panics
    ///
    /// Panics unless `txn`'s storage points to the same block of memory as `self.storage`.
    pub fn writable_copy<'txn>(&self, txn: &'txn WriteTxn<'env>) -> WriteDb<'txn, 'env, K, V> {
        assert_eq!(txn.storage().base, self.storage.base);
        WriteDb {
            sk_db: self.sk_db,
            txn: txn,
        }
    }
}

impl<'a, 'env, K: Stored<'env>, V: Stored<'env>, J: Storable<'env, K>, U: Storable<'env, V>> Iterator for Iter<'a, 'env, K, V, J, U> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next().map(|(k_wrapper, v_wrapper)| {
            unsafe {
                (k_wrapper.to_stored(&self.storage),
                    v_wrapper.to_stored(&self.storage))
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DbHeader {
    page_offset: u64,
}

impl StoredHeader for DbHeader {
    type PageOffsets = std::iter::Once<u64>;
    fn onpage_size(&self) -> u16 { 8 }
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::once(self.page_offset)
    }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> Stored<'env> for Db<'env, K, V> {
    fn alignment() -> Alignment { Alignment::B8 }
    type Header = DbHeader;

    fn header(&self) -> DbHeader {
        DbHeader { page_offset: self.sk_db.0 }
    }

    fn read_value(buf: &[u8], s: &Storage<'env>) -> Self {
        Db {
            sk_db: unsafe { sanakirja::Db::read_value(buf.as_ptr()) },
            storage: *s,
        }
    }

    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.sk_db.0);
    }

    fn read_header(buf: &[u8]) -> DbHeader {
        DbHeader { page_offset: LittleEndian::read_u64(buf) }
    }

    fn drop_value(&self, alloc: &mut Alloc) -> Result<()> {
        alloc.txn.with_mut_txn(|t|
            self.sk_db.drop_value(t, &mut rand::thread_rng())
        )
    }
}

impl<'env, K: Stored<'env>, V: Stored<'env>> Storable<'env, Db<'env, K, V>> for Db<'env, K, V> {
    fn store(&self, _: &mut Alloc) -> Result<Self> { Ok(*self) }
}

/// A `WriteDb<K, V>` is a write-only ordered map from `K` to `V`.
///
/// We realize that a write-only map seems pretty useless, but it's actually crucial to the safe
/// usage of this crate, because of the following two points:
///
/// * we have no way to prevent you from obtaining multiple handles pointing to the same database;
///   and
/// * references into a `WriteDb` are prone to being invalidated by modifications to that
///   `WriteDb`.
///
/// Our strategy for avoiding memory unsafety is to never give you read access to a database unless
/// it's contained in a snapshot. With that in mind:
///
/// * If you want to read from one map and write to another, then open a writer, `snapshot` it to
///   get a reader, then read from the reader while writing to the writer.
/// * If you want to interleave reads and writes to the same map, you'll need to take a snapshot
///   each time you're finished writing and want to start reading again.
pub struct WriteDb<'txn, 'env: 'txn, K: Stored<'env>, V: Stored<'env>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'env, K, K>, Wrapper<'env, V, V>>,
    pub(crate) txn: &'txn WriteTxn<'env>,
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> std::fmt::Debug for WriteDb<'txn, 'env, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WriteDb({:?})", self.sk_db.0)
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> PartialEq for WriteDb<'txn, 'env, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.txn.storage().base == other.txn.storage().base
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> Eq for WriteDb<'txn, 'env, K, V> { }

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> PartialOrd for WriteDb<'txn, 'env, K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> PartialEq<Db<'env, K, V>> for WriteDb<'txn, 'env, K, V> {
    fn eq(&self, other: &Db<'env, K, V>) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.txn.storage().base as *const u8 == other.storage.base
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> PartialOrd<Db<'env, K, V>> for WriteDb<'txn, 'env, K, V> {
    fn partial_cmp(&self, other: &Db<'env, K, V>) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> Ord for WriteDb<'txn, 'env, K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk_db.0.cmp(&other.sk_db.0)
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> Storable<'env, Db<'env, K, V>> for WriteDb<'txn, 'env, K, V> {
    fn store(&self, _: &mut Alloc) -> Result<Db<'env, K, V>> {
        Ok(Db {
            sk_db: self.sk_db,
            storage: self.txn.storage(),
        })
    }
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> WriteDb<'txn, 'env, K, V> {
    /// Inserts a binding into this map, returning `true` if a new binding was creating and `false`
    /// if the binding already existed.
    ///
    /// Note that we support multiple values associated to the same key. In particular, we only
    /// return false if this exact `(key, value)` pair was present before.
    pub fn insert<J: Storable<'env, K> + ?Sized, U: Storable<'env, V> + ?Sized>(&mut self, key: &J, val: &U) -> Result<bool> {
        let key: K = key.store(&mut self.txn.allocator())?;
        let val: V = val.store(&mut self.txn.allocator())?;
        Ok(self.txn.with_mut_txn(|txn|
            txn.put(&mut rand::thread_rng(), &mut self.sk_db, Wrapper::wrap(&key), Wrapper::wrap(&val))
        )?)
    }

    /// Removes the first binding associated with `key`, returning `true` if there was a binding
    /// associated with `key`.
    pub fn remove_first<J: Storable<'env, K> + ?Sized>(&mut self, key: &J) -> Result<bool> {
        let mut db = cast::<K, V, K, J, V, V>(self.sk_db);
        let ret = self.txn.with_mut_txn(|txn|
            txn.del(&mut rand::thread_rng(), &mut db, Wrapper::search(key), None)
        )?;
        self.sk_db = cast(db);
        Ok(ret)
    }

    /// Removes the binding `(key, val)` if it exists, returning `true` if it existed.
    pub fn remove_pair<J: Storable<'env, K> + ?Sized, U: Storable<'env, V> + ?Sized>(&mut self, key: &J, val: &U)
    -> Result<bool> {
        let mut db = cast(self.sk_db);
        let ret = self.txn.with_mut_txn(|txn|
            txn.del(&mut rand::thread_rng(), &mut db, Wrapper::search(key), Some(Wrapper::search(val)))
        )?;
        self.sk_db = cast(db);
        Ok(ret)
    }
}

/// A `RootWriteDb` is like a `WriteDb`, except that it's stored at a fixed location in the root of
/// the environment.
pub struct RootWriteDb<'txn, 'env: 'txn, K: Stored<'env>, V: Stored<'env>> {
    pub(crate) idx: usize,
    pub(crate) db: WriteDb<'txn, 'env, K, V>,
}

impl<'txn, 'env, K: Stored<'env>, V: Stored<'env>> RootWriteDb<'txn, 'env, K, V> {
    /// Inserts a binding into this map, returning `true` if a new binding was creating and `false`
    /// if the binding already existed.
    ///
    /// Note that we support multiple values associated to the same key. In particular, we only
    /// return false if this exact `(key, value)` pair was present before.
    pub fn insert<J: Storable<'env, K> + ?Sized, U: Storable<'env, V> + ?Sized>(&mut self, key: &J, val: &U) -> Result<bool> {
        self.db.insert(key, val)
    }

    /// Removes the first binding associated with `key`, returning `true` if there was a binding
    /// associated with `key`.
    pub fn remove_first<J: Storable<'env, K> + ?Sized>(&mut self, key: &J) -> Result<bool> {
        self.db.remove_first(key)
    }

    /// Removes the binding `(key, val)` if it exists, returning `true` if it existed.
    pub fn remove_pair<J: Storable<'env, K> + ?Sized, U: Storable<'env, V> + ?Sized>(&mut self, key: &J, val: &U)
    -> Result<bool> {
        self.db.remove_pair(key, val)
    }
}

impl<'txn, 'env: 'txn, K: Stored<'env>, V: Stored<'env>> Drop for RootWriteDb<'txn, 'env, K, V> {
    fn drop(&mut self) {
        self.db.txn.set_root_db(self.idx, &self.db);
        self.db.txn.borrowed_roots.borrow_mut().remove(&self.idx);
    }
}
