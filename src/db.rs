use byteorder::{ByteOrder, LittleEndian};
use rand;
use sanakirja;
use sanakirja::{Representable as SkRepr, Transaction};
use std;

use {Alignment, Alloc, StoredHeader, Result, Storable, Storage, Stored, Wrapper, WriteTxn};

fn cast<'sto, K, V, J1, J2, U1, U2>(db: sanakirja::Db<Wrapper<'sto, K, J1>, Wrapper<'sto, V, U1>>)
-> sanakirja::Db<Wrapper<'sto, K, J2>, Wrapper<'sto, V, U2>>
where
K: Stored<'sto>,
V: Stored<'sto>,
J1: Storable<'sto, K>,
J2: Storable<'sto, K>,
U1: Storable<'sto, V>,
U2: Storable<'sto, V>,
{
    unsafe { std::mem::transmute(db) }
}

pub struct Db<'sto, K: Stored<'sto>, V: Stored<'sto>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'sto, K, K>, Wrapper<'sto, V, V>>,
    pub(crate) storage: Storage<'sto>,
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Copy for Db<'sto, K, V> { }
impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Clone for Db<'sto, K, V> {
    fn clone(&self) -> Self { *self }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> std::fmt::Debug for Db<'sto, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Db({:?})", self.sk_db.0)
    }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> PartialEq for Db<'sto, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.storage.base == other.storage.base
    }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Eq for Db<'sto, K, V> { }

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> PartialOrd for Db<'sto, K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Ord for Db<'sto, K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk_db.0.cmp(&other.sk_db.0)
    }
}

struct Iter<'a, 'sto: 'a, K: Stored<'sto>, V: Stored<'sto>, J: Storable<'sto, K>, U: Storable<'sto, V>> {
    cursor: sanakirja::Cursor<'a, Storage<'sto>, Wrapper<'sto, K, J>, Wrapper<'sto, V, U>>,
    storage: Storage<'sto>,
}

impl<'sto, K: Stored<'sto> + 'sto, V: Stored<'sto> + 'sto> Db<'sto, K, V> {
    /// Returns an iterator over all key/value pairs in the database.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(K, V)> + 'a {
        Iter {
            cursor: self.storage.iter(self.sk_db, None),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key`.
    pub fn iter_from_key<'a, Q: Storable<'sto, K> + 'a>(&'a self, key: &Q)
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
    Q: Storable<'sto, K>,
    R: Storable<'sto, V>,
    {
        let db = cast(self.sk_db);
        Iter {
            cursor: self.storage.iter(db, Some((Wrapper::search(key), Some(Wrapper::search(val))))),
            storage: self.storage,
        }
    }

    /// Gets the smallest value associated with `key`, if there is one.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where Q: Storable<'sto, K> + PartialEq<K>
    {
        if let Some((k, v)) = self.iter_from_key(key).next() {
            if key.eq(&k) {
                return Some(v);
            }
        }
        None
    }

    /// Converts this `Db` to a writable one.
    ///
    /// # Panics
    ///
    /// Panics unless `txn`'s storage points to the same block of memory as `self.storage`.
    pub fn writable_copy<'txn>(&self, txn: &'txn WriteTxn<'sto>) -> WriteDb<'txn, 'sto, K, V> {
        //assert!(storage.base as *const u8 == self.storage.base);
        WriteDb {
            sk_db: self.sk_db,
            txn: txn,
        }
    }
}

impl<'a, 'sto, K: Stored<'sto>, V: Stored<'sto>, J: Storable<'sto, K>, U: Storable<'sto, V>> Iterator for Iter<'a, 'sto, K, V, J, U> {
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

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Stored<'sto> for Db<'sto, K, V> {
    fn alignment() -> Alignment { Alignment::B8 }
    type Header = DbHeader;

    fn header(&self) -> DbHeader {
        DbHeader { page_offset: self.sk_db.0 }
    }

    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self {
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

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Storable<'sto, Db<'sto, K, V>> for Db<'sto, K, V> {
    fn store(&self, _: &mut Alloc) -> Result<Self> { Ok(*self) }
}

#[derive(Clone, Copy)]
pub struct WriteDb<'txn, 'sto: 'txn, K: Stored<'sto>, V: Stored<'sto>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'sto, K, K>, Wrapper<'sto, V, V>>,
    pub(crate) txn: &'txn WriteTxn<'sto>,
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> std::fmt::Debug for WriteDb<'txn, 'sto, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WriteDb({:?})", self.sk_db.0)
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> PartialEq for WriteDb<'txn, 'sto, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.txn.storage().base == other.txn.storage().base
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> Eq for WriteDb<'txn, 'sto, K, V> { }

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> PartialOrd for WriteDb<'txn, 'sto, K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> PartialEq<Db<'sto, K, V>> for WriteDb<'txn, 'sto, K, V> {
    fn eq(&self, other: &Db<'sto, K, V>) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.txn.storage().base as *const u8 == other.storage.base
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> PartialOrd<Db<'sto, K, V>> for WriteDb<'txn, 'sto, K, V> {
    fn partial_cmp(&self, other: &Db<'sto, K, V>) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> Ord for WriteDb<'txn, 'sto, K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk_db.0.cmp(&other.sk_db.0)
    }
}

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> Storable<'sto, Db<'sto, K, V>> for WriteDb<'txn, 'sto, K, V> {
    fn store(&self, _: &mut Alloc) -> Result<Db<'sto, K, V>> {
        Ok(Db {
            sk_db: self.sk_db,
            storage: self.txn.storage(),
        })
    }
}

// As the name suggests, you can write to a WriteDb. But you can't read from one, or turn it back
// into a readable Db, because the storage underlying a WriteDb is prone to invalidation.
impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> WriteDb<'txn, 'sto, K, V> {
    // TODO: return an enum to indicate whether the binding existed already
    pub fn insert<J: Storable<'sto, K>, U: Storable<'sto, V>>(&mut self, key: J, val: U) -> Result<()> {
        let key: K = key.store(&mut self.txn.allocator())?;
        let val: V = val.store(&mut self.txn.allocator())?;
        self.txn.with_mut_txn(|txn|
            txn.put(&mut rand::thread_rng(), &mut self.sk_db, Wrapper::wrap(&key), Wrapper::wrap(&val))
        )?;
        Ok(())
    }

    pub fn remove_first<J: Storable<'sto, K>>(&mut self, key: &J) -> Result<()> {
        let mut db = cast::<K, V, K, J, V, V>(self.sk_db);
        self.txn.with_mut_txn(|txn|
            txn.del(&mut rand::thread_rng(), &mut db, Wrapper::search(key), None)
        )?;
        self.sk_db = cast(db);
        Ok(())
    }

    pub fn remove_pair<J: Storable<'sto, K>, U: Storable<'sto, V>>(&mut self, key: &K, val: &V)
    -> Result<()> {
        let mut db = cast(self.sk_db);
        self.txn.with_mut_txn(|txn|
            txn.del(&mut rand::thread_rng(), &mut db, Wrapper::search(key), Some(Wrapper::search(val)))
        )?;
        self.sk_db = cast(db);
        Ok(())
    }
}
