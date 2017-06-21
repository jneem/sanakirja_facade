use byteorder::{ByteOrder, LittleEndian};
use either::Either;
use rand::Rng;
use sanakirja;
use std;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use PAGE_SIZE;
use {Alignment, MutTxn, Result, Storage};

pub trait ReprHeader: Copy {
    type PageOffsets: Iterator<Item=u64>;
    fn onpage_size(&self) -> u16;
    fn page_offsets(&self) -> Self::PageOffsets;
}

/// This trait is for things that can be written to databases. They should be fairly small, or else
/// it will be costly to read and write them. However, this trait has a trick up its sleeve for
/// dealing with large buffers: you can implement this trait for a "reference" to a large buffer
/// that is actually stored in some backing storage. See `LargeBuf` for an example.
pub trait Representable<'sto>: Ord + Searchable<Self> {
    /// When reading from the database, sometimes we'd prefer just to return a reference instead of
    /// copying the data. That's why the return type of `read_value` is `Self::Borrowed` instead of
    /// `Self`. For small, plain-old-data types implementing `Representable`, `Self::Borrowed`
    /// would probably just be equal to `Self` (but associated type defaults are unstable).
    type Borrowed: Representable<'sto> + PartialOrd<Self> + Searchable<Self>;

    type Header: ReprHeader;

    fn header(&self) -> Self::Header;
    fn alignment() -> Alignment;

    /// Write this object to a buffer, which is guaranteed to have length `self.onpage_size()`.
    fn write_value(&self, buf: &mut [u8]);

    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self::Borrowed;

    /// Reads the header from a buffer.
    ///
    /// The length of the buffer is a little hard to predict: it starts from some unpredictable
    /// location and continues until the end of the enclosing page.
    fn read_header(buf: &[u8]) -> Self::Header;

    /// Frees all the pages owned by this value.
    fn drop_value(&self, txn: &mut MutTxn) -> Result<()>;

    /// How much space do we need to write this object to a database?
    fn onpage_size(&self) -> u16 {
        self.header().onpage_size()
    }
}

/// If `S: Searchable<K>` then `S` can be used to search in a database for something of type `K`.
///
/// This is a hack, to work around the fact that we need to wrap `Representable`s to get them in
/// and out of sanakirja; we'd really like to be able to just require `S: Ord<K>`.
pub trait Searchable<K: ?Sized> {
    /// Returns either a `&K` or a pointer into the database where something of type `K` is stored.
    fn storage_pointer(&self) -> Either<*const u8, &K>;
}

impl<K> Searchable<K> for K {
    fn storage_pointer(&self) -> Either<*const u8, &K> {
        Either::Right(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct U64ReprHeader {}

impl ReprHeader for U64ReprHeader {
    type PageOffsets = std::iter::Empty<u64>;
    fn onpage_size(&self) -> u16 { 8 }
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}

impl<'sto> Representable<'sto> for u64 {
    type Borrowed = u64;
    type Header = U64ReprHeader;
    fn header(&self) -> U64ReprHeader { U64ReprHeader {} }
    fn read_header(_: &[u8]) -> U64ReprHeader { U64ReprHeader {} }
    fn alignment() -> Alignment { Alignment::B8 }
    fn drop_value(&self, _: &mut MutTxn) -> Result<()> { Ok(()) }

    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, *self)
    }

    fn read_value(buf: &[u8], _: &Storage<'sto>) -> u64 {
        LittleEndian::read_u64(buf)
    }
}

pub(crate) enum Wrapper<'sto, T> where T: Representable<'sto> {
    // sanakirja::Representable requires Copy, but our Representable doesn't. So we can't store a
    // full T here, but only a pointer. This means we need to be careful not to use the pointer
    // for too long.
    //
    // The point is that Wrapper::OffPage is not supposed to live long. Given a T, we create a
    // Wrapper::OffPage in order to write the T into a database; then we throw away the wrapper.
    // When we're reading from a database, or copying/moving things around within the database then
    // the wrapper might last longer, but it will be a Wrapper::OnPage (which is guaranteed to be
    // valid for the lifetime of the storage).
    OffPage {
        ptr: *const T,
    },
    OnPage {
        ptr: *const u8,
        marker: PhantomData<&'sto Storage<'sto>>,
    },
}

impl<'sto, T: Representable<'sto>> Wrapper<'sto, T> {
    pub unsafe fn on_page_to_borrowed(&self, storage: &Storage<'sto>) -> T::Borrowed {
        let ptr = match *self {
            Wrapper::OffPage { .. } => {
                panic!("tried to borrow something that was already off the page")
            }
            Wrapper::OnPage { ptr, .. } => ptr,
        };
        let size = T::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size();
        let slice = mk_slice(ptr, size as usize);
        T::read_value(slice, storage)
    }

    pub fn new<Q: Searchable<T>>(val: &Q) -> Wrapper<'sto, T> {
        match val.storage_pointer() {
            Either::Right(p) => Wrapper::OffPage { ptr: p as *const T },
            Either::Left(ptr) => Wrapper::OnPage { ptr, marker: std::marker::PhantomData },
        }
    }
}

impl<'sto, T: Representable<'sto>> Debug for Wrapper<'sto, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            Wrapper::OffPage { ptr } => f.write_fmt(format_args!("OffPage({:?})", ptr)),
            Wrapper::OnPage { ptr, .. } => f.write_fmt(format_args!("OnPage({:?})", ptr)),
        }
    }
}

impl<'sto, T: Representable<'sto>> Copy for Wrapper<'sto, T> { }

impl<'sto, T: Representable<'sto>> Clone for Wrapper<'sto, T> {
    fn clone(&self) -> Self { *self }
}

// The next two functions are for creating slices that are contained in a single page.
//
// In various places, we take a pointer and turn it into a slice of fixed length (e.g., 8).  There
// is some chance that this slice extends past the edge of the mmap'ed region (for example, if the
// pointer points to a value of length 4, right at the edge of a page). In this case, I think that
// even creating the slice would be undefined behavior.
unsafe fn mk_slice<'a>(p: *const u8, mut len: usize) -> &'a [u8] {
    let next_page_offset = (p as usize - 1) % PAGE_SIZE + 1;
    len = std::cmp::min(len, next_page_offset);
    std::slice::from_raw_parts(p, len)
}

unsafe fn mk_mut_slice<'a>(p: *mut u8, mut len: usize) -> &'a mut [u8] {
    let next_page_offset = (p as usize - 1) % PAGE_SIZE + 1;
    len = std::cmp::min(len, next_page_offset);
    std::slice::from_raw_parts_mut(p, len)
}

unsafe fn read_value<'sto, V, L>(ptr: *const u8, loader: &L) -> V::Borrowed
where
V: Representable<'sto>,
L: sanakirja::LoadPage
{
    let size = V::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size();
    let slice = mk_slice(ptr, size as usize);

    // Find the storage buffer underlying `loader` and make a `Storage` struct out of it.
    let base = loader.load_page(0).data;
    // The cast is ok: when we open the transaction we ensure that its length fits in a usize.
    let storage = Storage { base, len: loader.len() as usize, marker: PhantomData };
    V::read_value(slice, &storage)
 }

impl<'sto, T> sanakirja::Representable for Wrapper<'sto, T>
where
T: Representable<'sto>
{
    type PageOffsets = <<T as Representable<'sto>>::Header as ReprHeader>::PageOffsets;
    fn page_offsets(&self) -> Self::PageOffsets {
        match *self {
            Wrapper::OffPage { ptr } => {
                unsafe { (*ptr).header().page_offsets() }
            }
            Wrapper::OnPage { ptr, .. } => {
                unsafe {
                    T::read_header(mk_slice(ptr, PAGE_SIZE)).page_offsets()
                }
            }
        }
    }

    fn onpage_size(&self) -> u16 {
        match *self {
            Wrapper::OffPage { ptr } => unsafe { (*ptr).onpage_size() },
            Wrapper::OnPage { ptr, .. } => unsafe {
                T::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size()
            }
        }
    }

    unsafe fn write_value(&self, p: *mut u8) {
        match *self {
            Wrapper::OffPage { ptr } => {
                (*ptr).write_value(mk_mut_slice(p, (*ptr).onpage_size() as usize));
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

    unsafe fn cmp_value<Tn: sanakirja::LoadPage>(&self, txn: &Tn, x: Self) -> Ordering {
        use self::Wrapper::*;
        let read = |ptr: *const u8| read_value::<T, Tn>(ptr, txn);
        match (*self, x) {
            (OffPage { ptr: p }, OffPage { ptr: q }) => (*p).cmp(&*q),
            (OnPage { ptr: p, .. }, OffPage { ptr: q }) => read(p).partial_cmp(&*q).unwrap(),
            (OffPage { ptr: p }, OnPage { ptr: q, .. }) => read(q).partial_cmp(&*p).unwrap(),
            (OnPage { ptr: p, .. }, OnPage { ptr: q, .. }) => read(p).cmp(&read(q)),
        }
    }

    fn alignment() -> sanakirja::Alignment {
        <T as Representable>::alignment()
    }

    unsafe fn skip(p: *mut u8) -> *mut u8 {
        let size = T::read_header(mk_slice(p, PAGE_SIZE)).onpage_size();
        p.offset(size as isize)
    }

    fn drop_value<R: Rng>(&self, txn: &mut sanakirja::MutTxn, _: &mut R) -> Result<()> {
        unsafe {
            // Since MutTxn is just a wrapper around sanakirja::MutTxn, we can convert the pointer.
            let my_txn = std::mem::transmute::<&mut sanakirja::MutTxn, &mut MutTxn>(txn);
            match *self {
                Wrapper::OffPage { ptr } => (*ptr).drop_value(my_txn),
                Wrapper::OnPage { ptr: p, .. } => {
                    let val = read_value::<T, _>(p, txn);
                    val.drop_value(my_txn)
                },
            }
        }
    }
}

