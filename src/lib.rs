#![feature(conservative_impl_trait)]

extern crate byteorder;
extern crate either;
extern crate rand;
extern crate sanakirja;

use sanakirja::{LoadPage, Transaction};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::path::Path;

mod db;
mod representable;

use representable::Wrapper;

pub use db::{Db, WriteDb};
pub use representable::{ReprHeader, Storable, Stored};
pub use sanakirja::{Alignment, Commit};
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

pub const PAGE_SIZE: usize = 4096;

pub struct Env {
    sk_env: Box<sanakirja::Env>
}

pub struct ReadTxn<'txn> {
    sk_txn: sanakirja::Txn<'txn>
}

// FIXME: in order to implement sanakirja::Representable::drop_value in terms of
// Stored::drop_value, we need a way to convert &mut sanakirja::MutTxn to WriteTxn.
pub struct WriteTxn<'sto> {
    sk_txn: RefCell<sanakirja::MutTxn<'sto>>
}

impl Env {
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        sanakirja::Env::file_size(path)
    }

    pub fn open<P: AsRef<Path>>(path: P, length: u64) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new(path, length)?)
        })
    }

    pub fn memory_backed(length: u64) -> Result<Env> {
        Ok(Env {
            sk_env: Box::new(sanakirja::Env::new_memory_backed(length)?)
        })
    }

    pub fn begin_txn<'env>(&'env self) -> Result<ReadTxn<'env>> {
        Ok(ReadTxn { sk_txn: self.sk_env.txn_begin()? })
    }

    pub fn begin_mut_txn<'sto>(&'sto mut self) -> Result<WriteTxn<'sto>> {
        Ok(WriteTxn {
            sk_txn: RefCell::new(self.sk_env.mut_txn_begin()?),
        })
    }

    /*
    pub fn commit<'env>(self, txn: WriteTxn<'env>) -> Result<Self> {
        txn.sk_txn.into_inner().commit()?;
        Ok(self)
    }
    */
}

impl<'sto> ReadTxn<'sto> {
    pub fn storage(&self) -> Storage<'sto> {
        Storage {
            base: self.sk_txn.load_page(0).data,
            len: self.sk_txn.len() as usize,
            marker: std::marker::PhantomData,
        }
    }

    pub fn root_db<K: Stored<'sto>, V: Stored<'sto>>(&self, idx: usize) -> Db<'sto, K, V> {
        let sk_db = self.sk_txn.root(idx).unwrap();
        Db {
            sk_db: sk_db,
            storage: self.storage(),
        }
    }
}

impl<'sto> WriteTxn<'sto> {
    pub fn storage(&self) -> MutStorage<'sto> {
        MutStorage {
            base: self.sk_txn.borrow().base(),
            len: self.sk_txn.borrow().len() as usize,
            marker: std::marker::PhantomData,
        }
    }

    pub fn root_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&'txn self, idx: usize) -> WriteDb<'txn, 'sto, K, V> {
        let sk_db = self.sk_txn.borrow().root(idx).unwrap();
        WriteDb {
            sk_db: sk_db,
            txn: self,
        }
    }

    pub fn create_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&'txn self) -> WriteDb<'txn, 'sto, K, V> {
        // TODO: this can fail if there's not enough space
        let sk_db = self.sk_txn.borrow_mut().create_db().unwrap();
        WriteDb {
            sk_db: sk_db,
            txn: self,
        }
    }

    pub fn set_root_db<'txn, K: Stored<'sto>, V: Stored<'sto>>(&self, idx: usize, db: WriteDb<'txn, 'sto, K, V>) {
        self.sk_txn.borrow_mut().set_root(idx, db.sk_db);
    }

    pub fn commit(self) -> Result<()> {
        self.sk_txn.into_inner().commit()?;
        Ok(())
    }
}
