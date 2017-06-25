use byteorder::{ByteOrder, LittleEndian};
use rand;
use sanakirja;
use sanakirja::{Representable as SkRepr, Transaction};
use std;

use {Alignment, ReprHeader, Result, Storable, Storage, Stored, Wrapper, WriteTxn};

fn cast<'sto, K, V, J1, J2, U1, U2>(db: sanakirja::Db<Wrapper<'sto, K, J1>, Wrapper<'sto, V, U1>>)
-> sanakirja::Db<Wrapper<'sto, K, J2>, Wrapper<'sto, V, U2>>
where
K: Stored<'sto>,
V: Stored<'sto>,
J1: Storable<K>,
J2: Storable<K>,
U1: Storable<V>,
U2: Storable<V>,
{
    unsafe { std::mem::transmute(db) }
}

#[derive(Clone, Copy)]
pub struct Db<'sto, K: Stored<'sto>, V: Stored<'sto>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'sto, K, K>, Wrapper<'sto, V, V>>,
    pub(crate) storage: Storage<'sto>,
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

struct Iter<'a, 'sto: 'a, K: Stored<'sto>, V: Stored<'sto>, J: Storable<K>, U: Storable<V>> {
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
    pub fn iter_from_key<'a, Q: Storable<K> + 'a>(&'a self, key: &Q)
    -> impl Iterator<Item=(K, V)> + 'a
    {
        let db = cast::<K, V, K, Q, V, V>(self.sk_db);
        Iter {
            cursor: self.storage.iter(db, Some((Wrapper::wrap(key), None))),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key` and whose value is at least `val`.
    pub fn iter_from_key_value<'a, Q: 'a, R: 'a>(&'a self, key: &Q, val: &R)
    -> impl Iterator<Item=(K, V)> + 'a
    where
    Q: Storable<K>,
    R: Storable<V>,
    {
        let db = cast(self.sk_db);
        Iter {
            cursor: self.storage.iter(db, Some((Wrapper::wrap(key), Some(Wrapper::wrap(val))))),
            storage: self.storage,
        }
    }

    /// Gets the smallest value associated with `key`, if there is one.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where Q: Storable<K> + PartialEq<K>
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

impl<'a, 'sto, K: Stored<'sto>, V: Stored<'sto>, J: Storable<K>, U: Storable<V>> Iterator for Iter<'a, 'sto, K, V, J, U> {
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

impl ReprHeader for DbHeader {
    type PageOffsets = std::iter::Once<u64>;
    fn onpage_size(&self) -> u16 { 8 }
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::once(self.page_offset)
    }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Storable<Db<'sto, K, V>> for Db<'sto, K, V> {
    fn alignment() -> Alignment { Alignment::B8 }
    fn onpage_size(&self) -> u16 { 8 }
    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.sk_db.0);
    }
}

impl<'sto, K: Stored<'sto>, V: Stored<'sto>> Stored<'sto> for Db<'sto, K, V> {
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

    fn read_header(buf: &[u8]) -> DbHeader {
        DbHeader { page_offset: LittleEndian::read_u64(buf) }
    }

    fn drop_value(&self, txn: &mut WriteTxn) -> Result<()> {
        self.sk_db.drop_value(&mut *txn.sk_txn.borrow_mut(), &mut rand::thread_rng())
    }
}

#[derive(Clone, Copy)]
pub struct WriteDb<'txn, 'sto: 'txn, K: Stored<'sto>, V: Stored<'sto>> {
    pub(crate) sk_db: sanakirja::Db<Wrapper<'sto, K, K>, Wrapper<'sto, V, V>>,
    pub(crate) txn: &'txn WriteTxn<'sto>,
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

impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> Storable<Db<'sto, K, V>> for WriteDb<'txn, 'sto, K, V> {
    fn alignment() -> Alignment { Alignment::B8 }
    fn onpage_size(&self) -> u16 { 8 }
    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.sk_db.0);
    }
}

// As the name suggests, you can write to a WriteDb. But you can't read from one, or turn it back
// into a readable Db, because the storage underlying a WriteDb is prone to invalidation.
impl<'txn, 'sto, K: Stored<'sto>, V: Stored<'sto>> WriteDb<'txn, 'sto, K, V> {
    // TODO: return an enum to indicate whether the binding existed already
    pub fn insert<J: Storable<K>, U: Storable<V>>(&mut self, key: J, val: U) -> Result<()> {
        let mut db = cast(self.sk_db);
        self.txn.sk_txn.borrow_mut()
            .put(&mut rand::thread_rng(), &mut db, Wrapper::wrap(&key), Wrapper::wrap(&val))?;
        Ok(())
    }

    pub fn remove_first<J: Storable<K>>(&mut self, key: &J) -> Result<()> {
        let mut db = cast::<K, V, K, J, V, V>(self.sk_db);
        self.txn.sk_txn.borrow_mut()
            .del(&mut rand::thread_rng(), &mut db, Wrapper::wrap(key), None)?;
        Ok(())
    }

    pub fn remove_pair<J: Storable<K>, U: Storable<V>>(&mut self, key: &K, val: &V)
    -> Result<()> {
        let mut db = cast(self.sk_db);
        self.txn.sk_txn.borrow_mut()
            .del(&mut rand::thread_rng(), &mut db, Wrapper::wrap(key), Some(Wrapper::wrap(val)))?;
        Ok(())
    }
}
