#![feature(conservative_impl_trait)]

extern crate byteorder;
extern crate either;
extern crate rand;
extern crate sanakirja;

use sanakirja::{LoadPage, Transaction};
use std::marker::PhantomData;
use std::path::Path;

mod db;
mod representable;

use representable::Wrapper;

pub use db::Db;
pub use representable::{Representable, ReprHeader, Searchable};
pub use sanakirja::Alignment;
pub type Error = sanakirja::Error;
pub type Result<T> = std::result::Result<T, Error>;

// Size, in bytes, of the header on page 0.
const ZERO_HEADER: isize = 24;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Storage<'sto> {
    base: *const u8,
    len: usize,
    marker: PhantomData<&'sto u8>,
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

impl<'sto> sanakirja::skiplist::SkipList for Storage<'sto> {}
impl<'sto> sanakirja::Transaction for Storage<'sto> {}

pub const PAGE_SIZE: usize = 4096;

pub struct Env {
    sk_env: sanakirja::Env
}

pub struct Txn<'txn> {
    sk_txn: sanakirja::Txn<'txn>
}

#[repr(C)]
pub struct MutTxn<'txn> {
    sk_txn: sanakirja::MutTxn<'txn>
}

impl Env {
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        sanakirja::Env::file_size(path)
    }

    pub fn new<P: AsRef<Path>>(path: P, length: u64) -> Result<Env> {
        Ok(Env { sk_env: sanakirja::Env::new(path, length)? })
    }

    pub fn begin_txn<'env>(&'env self) -> Result<Txn<'env>> {
        Ok(Txn { sk_txn: self.sk_env.txn_begin()? })
    }

    pub fn begin_mut_txn<'env>(&'env mut self) -> Result<MutTxn<'env>> {
        Ok(MutTxn { sk_txn: self.sk_env.mut_txn_begin()? })
    }
}

impl<'sto> Txn<'sto> {
    pub fn storage(&self) -> Storage<'sto> {
        Storage {
            base: self.sk_txn.load_page(0).data,
            len: self.sk_txn.len() as usize,
            marker: std::marker::PhantomData,
        }
    }

    pub fn root_db<K, V>(&self, idx: usize) -> Option<Db<'sto, K, V>>
    where
    K: Representable<'sto>,
    V: Representable<'sto>,
    {
        if let Some(sk_db) = self.sk_txn.root(idx) {
            Some(Db {
                sk_db: sk_db,
                storage: self.storage(),
            })
        } else {
            None
        }
    }
}


