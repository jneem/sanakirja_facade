use byteorder::{ByteOrder, LittleEndian};
use rand;
use sanakirja;
use sanakirja::{Representable as SkRepr, Transaction};
use std;

use {Alignment, MutTxn, ReprHeader, Representable, Result, Searchable, Storage, Wrapper};

#[derive(Clone, Copy)]
pub struct Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    pub(crate) sk_db: sanakirja::Db<Wrapper<'sto, K>, Wrapper<'sto, V>>,
    pub(crate) storage: Storage<'sto>,
}

impl<'sto, K, V> PartialEq for Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    fn eq(&self, other: &Self) -> bool {
        self.sk_db.0 == other.sk_db.0
            && self.storage.base == other.storage.base
    }
}

impl<'sto, K, V> Eq for Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
}

impl<'sto, K, V> PartialOrd for Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sk_db.0.partial_cmp(&other.sk_db.0)
    }
}

impl<'sto, K, V> Ord for Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sk_db.0.cmp(&other.sk_db.0)
    }
}

struct Iter<'a, 'sto: 'a, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    cursor: sanakirja::Cursor<'a, Storage<'sto>, Wrapper<'sto, K>, Wrapper<'sto, V>>,
    storage: Storage<'sto>,
}

impl<'sto, K, V> Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    /// Returns an iterator over all key/value pairs in the database.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(K::Borrowed, V::Borrowed)> + 'a {
        Iter {
            cursor: self.storage.iter(&self.sk_db, None),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key`.
    pub fn iter_from_key<'a, Q>(&'a self, key: &Q) -> impl Iterator<Item=(K::Borrowed, V::Borrowed)> + 'a
    where Q: Searchable<K>
    {
        Iter {
            cursor: self.storage.iter(&self.sk_db, Some((Wrapper::new(key), None))),
            storage: self.storage,
        }
    }

    /// Returns an iterator over key/value pairs, starting with the first one whose key is at least
    /// `key` and whose value is at least `val`.
    pub fn iter_from_key_value<'a, Q, R>(&'a self, key: &Q, val: &R)
    -> impl Iterator<Item=(K::Borrowed, V::Borrowed)> + 'a
    where
    Q: Searchable<K>,
    R: Searchable<V>,
    {
        Iter {
            cursor: self.storage.iter(
                &self.sk_db,
                Some((Wrapper::new(key), Some(Wrapper::new(val))))),
            storage: self.storage,
        }
    }

    /// Gets the smallest value associated with `key`, if there is one.
    pub fn get<Q>(&self, key: &Q) -> Option<V::Borrowed>
    where Q: Searchable<K> + PartialEq<K::Borrowed>
    {
        if let Some((k, v)) = self.iter_from_key(key).next() {
            if key.eq(&k) {
                return Some(v);
            }
        }
        None
    }
}

impl<'a, 'sto, K, V> Iterator for Iter<'a, 'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    type Item = (K::Borrowed, V::Borrowed);

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next().map(|(k_wrapper, v_wrapper)| {
            unsafe {
                (k_wrapper.on_page_to_borrowed(&self.storage),
                    v_wrapper.on_page_to_borrowed(&self.storage))
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

impl<'sto, K, V> Representable<'sto> for Db<'sto, K, V>
where
K: Representable<'sto>,
V: Representable<'sto>,
{
    type Borrowed = Self;
    type Header = DbHeader;

    fn header(&self) -> DbHeader {
        DbHeader { page_offset: self.sk_db.0 }
    }

    fn alignment() -> Alignment { Alignment::B8 }

    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.sk_db.0);
    }

    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self::Borrowed {
        Db {
            sk_db: unsafe { sanakirja::Db::read_value(buf.as_ptr()) },
            storage: *s,
        }
    }

    fn read_header(buf: &[u8]) -> DbHeader {
        DbHeader { page_offset: LittleEndian::read_u64(buf) }
    }

    fn drop_value(&self, txn: &mut MutTxn) -> Result<()> {
        self.sk_db.drop_value(&mut txn.sk_txn, &mut rand::thread_rng())
    }
}

