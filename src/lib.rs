#![feature(try_from)]

extern crate rand;
extern crate sanakirja;

use sanakirja::LoadPage;

use std::cmp::{PartialOrd, Ord, Ordering};
use std::convert::TryInto;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use rand::Rng;

pub type Error = sanakirja::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub struct Storage<'sto> {
    base: *const u8,
    len: usize,
    marker: PhantomData<&'sto u8>,
}

pub const PAGE_SIZE: usize = 4096;

pub trait ReprHeader: Copy {
    type PageOffsets: Iterator<Item=u64>;
    fn onpage_size(&self) -> u16;
    fn page_offsets(&self) -> Self::PageOffsets;
}

/// This trait is for things that can be written to databases. They should be fairly small, or else
/// it will be costly to read and write them. However, this trait has a trick up its sleeve for
/// dealing with large buffers: you can implement this trait for a "reference" to a large buffer
/// that is actually stored in some backing storage. See `LargeBuf` for an example.
pub trait Representable<'sto>: Ord {
    /// When reading from the database, sometimes we'd prefer just to return a reference instead of
    /// copying the data. That's why the return type of `read_value` is `Self::Borrowed` instead of
    /// `Self`. For small, plain-old-data types implementing `Representable`, `Self::Borrowed`
    /// would probably just be equal to `Self` (but associated type defaults are unstable).
    type Borrowed: Representable<'sto> + PartialOrd<Self>;

    type Header: ReprHeader;

    fn header(&self) -> Self::Header;

    /// Write this object to a buffer, which is guaranteed to have length `self.onpage_size()`.
    fn write_value(&self, buf: &mut [u8]);

    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self::Borrowed;
    fn read_header(buf: &[u8]) -> Self::Header;

    /// Frees all the pages owned by this value.
    fn drop_value(&self, txn: &mut MutTxn) -> Result<()>;

    /// How much space do we need to write this object to a database?
    fn onpage_size(&self) -> u16 {
        self.header().onpage_size()
    }
}

enum Wrapper<'a, 'sto, T: 'a> where T: Representable<'sto> {
    OffPage {
        ptr: &'a T,
    },
    OnPage {
        ptr: *const u8,
        marker: PhantomData<&'sto Storage<'sto>>,
    },
}

impl<'a, 'sto, T: Representable<'sto>> Debug for Wrapper<'a, 'sto, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        unimplemented!();
    }
}

impl<'a, 'sto, T: Representable<'sto>> Copy for Wrapper<'a, 'sto, T> { }

impl<'a, 'sto, T: Representable<'sto>> Clone for Wrapper<'a, 'sto, T> {
    fn clone(&self) -> Self { *self }
}

// In various places, we take a pointer and turn it into a slice of fixed length (e.g., 8).
// There is a small chance that this slice extends past the edge of the mmap'ed region (for
// example, if the pointer points to a value of length 4, right at the edge of a page). In this
// case, I think that even creating the slice would be undefined behavior.
//
// Here, we avoid UB by using the fact that our mmap'ed region is an integer number of pages,
// and that all the buffers we pass around are contained within a single page.
unsafe fn mk_slice<'a>(p: *const u8, len: u64) -> &'a [u8] {
    // Note the panic here. If the conversion of `len` into `usize` ever overflows, we've hit
    // some corruption (still, proper error handling would be nice).
    let mut len: usize = len.try_into().unwrap();
    let next_page_offset = (p as usize - 1) % PAGE_SIZE + 1;
    len = std::cmp::min(len, next_page_offset);
    std::slice::from_raw_parts(p, len)
}

unsafe fn mk_mut_slice<'a>(p: *mut u8, len: u64) -> &'a mut [u8] {
    // Note the panic here. If the conversion of `len` into `usize` ever overflows, we've hit
    // some corruption (still, proper error handling would be nice).
    let mut len: usize = len.try_into().unwrap();
    let next_page_offset = (p as usize - 1) % PAGE_SIZE + 1;
    len = std::cmp::min(len, next_page_offset);
    std::slice::from_raw_parts_mut(p, len)
}

impl<'a, 'sto, T> sanakirja::Representable for Wrapper<'a, 'sto, T>
where
T: Representable<'sto>
{
    type PageOffsets = <<T as Representable<'sto>>::Header as ReprHeader>::PageOffsets;
    fn page_offsets(&self) -> Self::PageOffsets {
        match *self {
            Wrapper::OffPage { ptr } => {
                ptr.header().page_offsets()
            }
            Wrapper::OnPage { ptr, .. } => {
                unsafe {
                    T::read_header(mk_slice(ptr, 16)).page_offsets()
                }
            }
        }
    }

    fn onpage_size(&self) -> u16 {
        match *self {
            Wrapper::OffPage { ptr } => ptr.onpage_size(),
            Wrapper::OnPage { ptr, .. } => unsafe {
                T::read_header(mk_slice(ptr, 16)).onpage_size()
            }
        }
    }

    unsafe fn write_value(&self, p: *mut u8) {
        match *self {
            Wrapper::OffPage { ptr } => {
                ptr.write_value(mk_mut_slice(p, ptr.onpage_size() as u64));
            }
            Wrapper::OnPage { ptr, .. } => {
                // TODO: explain this
                std::ptr::copy_nonoverlapping(ptr, p, self.onpage_size() as usize);
            }
        }
    }

    unsafe fn read_value(p: *const u8) -> Self {
        Wrapper::OnPage {
            ptr: p,
            marker: PhantomData,
        }
    }

    unsafe fn cmp_value<Tn: LoadPage>(&self, txn: &Tn, x: Self) -> Ordering {
        let read = |ptr: *const u8| -> T::Borrowed {
            let size = T::read_header(mk_slice(ptr, 16)).onpage_size();
            let slice = mk_slice(ptr, size as u64);

            // Find the storage buffer underlying `txn` and make a `Storage` struct out of it.
            let base = txn.load_page(0).data;
            // The length here is fishy; the problem is that a sanakirja::LoadPage doesn't expose
            // any way to find the length. So here we're relying on `T::read_value` to be
            // well-behaved and not try to read past the end of the buffer.
            let len = std::usize::MAX;
            let storage = Storage { base, len, marker: PhantomData };
            T::read_value(slice, &storage)
        };

        use Wrapper::*;
        match (*self, x) {
            (OffPage { ptr: p }, OffPage { ptr: q }) => p.cmp(q),
            (OnPage { ptr: p, .. }, OffPage { ptr: q }) => read(p).partial_cmp(q).unwrap(),
            (OffPage { ptr: p }, OnPage { ptr: q, .. }) => read(q).partial_cmp(p).unwrap(),
            (OnPage { ptr: p, .. }, OnPage { ptr: q, .. }) => read(p).cmp(&read(q)),
        }
    }

    fn alignment() -> sanakirja::Alignment {
        unimplemented!();
    }

    unsafe fn skip(p: *mut u8) -> *mut u8 {
        unimplemented!();
    }

    fn drop_value<Tn, R: Rng>(&self, _: &mut sanakirja::MutTxn<Tn>, _: &mut R) -> Result<()> {
        unimplemented!();
    }
}

pub struct Env {
    sk_env: sanakirja::Env
}

pub struct Txn<'txn> {
    sk_txn: sanakirja::Txn<'txn>
}

pub struct MutTxn<'txn> {
    sk_txn: sanakirja::MutTxn<'txn, ()>
}

// The trait bounds on K and V are a little unfortunate, because we're allowing sanakirja internals
// to leak out...
pub struct Db<'txn, T: 'txn, K, V>
where
K: sanakirja::Representable,
V: sanakirja::Representable,
{
    sk_db: sanakirja::Db<K, V>,
    txn: &'txn T,
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

