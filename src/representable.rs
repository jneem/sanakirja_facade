use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use sanakirja;
use sanakirja::LoadPage;
use std;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use PAGE_SIZE;
use {Alignment, Alloc, WriteTxn, Result, Storage};

/// Objects that are stored in the database often need to store some important metadata along with
/// them. This trait represents that metadata.
pub trait StoredHeader: Copy {
    /// This has to do with garbage collection, and could probably do with a better name.
    type PageOffsets: Iterator<Item=u64>;
    /// This has to do with garbage collection, and could probably do with a better name.
    fn page_offsets(&self) -> Self::PageOffsets;

    /// How much space does the object need in the database?
    fn onpage_size(&self) -> u16;
}

/// This is the trait for objects that are stored in a database. They should be fairly small, or
/// else it will be costly to read and write them. However, since they can also store references
/// into the database, you can implement `Stored` for a "reference" into a large buffer that is
/// stored in the database.
///
/// If you want to store custom datatypes in your sanakirja database, this is the main trait that
/// you need to implement.
pub trait Stored<'sto>: Storable<'sto, Self> + Sized {
    /// This is the metadata associated with this type.
    type Header: StoredHeader;

    /// Returns my metadata.
    fn header(&self) -> Self::Header;

    /// Reads a header from the database.
    ///
    /// The buffer `buf` is the same one that gets passed to `read_value`.
    fn read_header(buf: &[u8]) -> Self::Header;

    /// Reads the entire object from the database.
    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self;

    /// Deletes this object from the database.
    ///
    /// This function can be empty unless the object allocated its own pages. In case your object
    /// does allocate its own pages, see the crate documentation for more information about memory
    /// allocation and garbage collection in sanakirja.
    fn drop_value<'a>(&self, alloc: &mut Alloc<'a, 'sto>) -> Result<()>;

    /// How large of a buffer do `write_value` and `read_value` need?
    fn onpage_size(&self) -> u16 {
        self.header().onpage_size()
    }

    /// How should `write_value`'s buffer be aligned?
    fn alignment() -> Alignment;

    /// Writes `self` to `buf`.
    fn write_value(&self, buf: &mut [u8]);
}

/// When the type `S` implements `Storable<T>`, it means that we can write `S` into a database that
/// expects to store `T`. For example, `LargeBuf` is a large buffer that's stored in the database,
/// and `[u8]` implements `Storable<LargeBuf>`, since it's sometimes useful to take a buffer
/// outside the database and write it to the database.
///
/// `Storable<T>` requires `PartialOrd<T>`, because if you want to write something into a database
/// then you first need to find out where it goes (and our databases are stored as ordered maps).
///
/// `Stored` implies `Storable<Self>`, because if you read something from the database then you
/// might want to write it back somewhere.
///
/// # Warning
///
/// When implementing `Storable<T>`, you need to be very careful that the format you write is
/// identical to the format that `T` expects. Otherwise, you'll probably get database corruption.
pub trait Storable<'sto, T>: PartialOrd<T> {
    fn store<'a>(&self, alloc: &mut Alloc<'a, 'sto>) -> Result<T>;
}

#[derive(Clone, Copy, Debug)]
pub struct U64StoredHeader {}

impl StoredHeader for U64StoredHeader {
    type PageOffsets = std::iter::Empty<u64>;
    fn onpage_size(&self) -> u16 { 8 }
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}

impl<'sto> Storable<'sto, u64> for u64 {
    fn store(&self, _: &mut Alloc) -> Result<u64> { Ok(*self) }
}

impl<'sto> Stored<'sto> for u64 {
    type Header = U64StoredHeader;
    fn header(&self) -> U64StoredHeader { U64StoredHeader {} }
    fn read_header(_: &[u8]) -> U64StoredHeader { U64StoredHeader {} }
    fn drop_value(&self, _: &mut Alloc) -> Result<()> { Ok(()) }
    fn alignment() -> Alignment { Alignment::B8 }
    fn read_value(buf: &[u8], _: &Storage<'sto>) -> u64 {
        LittleEndian::read_u64(buf)
    }
    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, *self)
    }
}

// Wrap a Stored in a Wrapper to make it sanakirja::Representable.
//
// We also take another parameter, `T`. This is a hack to let us search in sanakirja databases
// using something that's comparable to the stored type, but not equal to it.
pub(crate) enum Wrapper<'sto, S: Stored<'sto>, T: ?Sized> {
    // sanakirja::Representable requires Copy, but Stored and T don't. So we can't store S or T
    // here, but only a pointer. This means we need to be careful not to use the pointer for too
    // long.
    //
    // The point is that Wrapper::OffPage is not supposed to live long. Given a T, we create a
    // Wrapper::OffPage in order to write the T into a database; then we throw away the wrapper.
    // When we're reading from a database, or copying/moving things around within the database then
    // the wrapper might last longer, but it will be a Wrapper::OnPage (which is guaranteed to be
    // valid for the lifetime of the storage).
    OffPage {
        ptr: *const S,
    },
    OnPage {
        ptr: *const u8,
        marker: PhantomData<(S, &'sto ())>,
    },
    Searcher {
        ptr: *const T,
    }
}

impl<'sto, S: Stored<'sto>, T: ?Sized> Wrapper<'sto, S, T> {
    /// Given a `Wrapper` that's stored in the database, return the corresponding `Stored`.
    ///
    /// # Panics
    ///
    /// Panics if the wrapper is wrapping something that isn't stored in the database.
    pub unsafe fn to_stored(&self, storage: &Storage<'sto>) -> S {
        match *self {
            Wrapper::OnPage { ptr, .. } => {
                let size = S::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size();
                let slice = mk_slice(ptr, size as usize);
                S::read_value(slice, storage)
            },
            _ => {
                panic!("tried to convert something that wasn't on-page")
            },
        }
    }

    /// Wraps a `Stored`.
    pub fn wrap(val: &S) -> Wrapper<'sto, S, T> {
        Wrapper::OffPage { ptr: val as *const S }
    }

    /// Wraps something for searching.
    ///
    /// You need to be careful when using the resulting `Wrapper`, since most of the functions in
    /// `impl Representable for Wrapper` panic when they see this kind of wrapper. Basically, you
    /// can only use this `Wrapper` for searching in a database, and not for inserting.
    pub fn search(val: &T) -> Wrapper<'sto, S, T> {
        Wrapper::Searcher { ptr: val as *const T }
    }
}

impl<'sto, S: Stored<'sto>, T: ?Sized> Debug for Wrapper<'sto, S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            Wrapper::OffPage { ptr } => f.write_fmt(format_args!("OffPage({:?})", ptr)),
            Wrapper::OnPage { ptr, .. } => f.write_fmt(format_args!("OnPage({:?})", ptr)),
            Wrapper::Searcher { ptr } => f.write_fmt(format_args!("Searcher({:?})", ptr)),
        }
    }
}

impl<'sto, S: Stored<'sto>, T: ?Sized> Copy for Wrapper<'sto, S, T> { }

impl<'sto, S: Stored<'sto>, T: ?Sized> Clone for Wrapper<'sto, S, T> {
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

// This is our main interface to sanakirja. Every one of our `Stored` types gets put in a `Wrapper`
// in order to hand it to sanakirja for storing. The `Wrapper` implements
// `sanakirja::Representable`, mainly by forwarding things to functions defined in `Stored`.
impl<'sto, S, T> sanakirja::Representable for Wrapper<'sto, S, T>
where
S: Stored<'sto>,
T: Storable<'sto, S> + ?Sized,
{
    type PageOffsets = <<S as Stored<'sto>>::Header as StoredHeader>::PageOffsets;
    fn page_offsets(&self) -> Self::PageOffsets {
        match *self {
            Wrapper::OffPage { ptr } => unsafe { (*ptr).header().page_offsets() },
            Wrapper::OnPage { ptr, .. } => {
                unsafe {
                    S::read_header(mk_slice(ptr, PAGE_SIZE)).page_offsets()
                }
            }
            Wrapper::Searcher { .. } => {
                panic!("asked for page offsets of a searcher");
            }
        }
    }

    fn onpage_size(&self) -> u16 {
        match *self {
            Wrapper::OffPage { ptr } => unsafe { (*ptr).onpage_size() },
            Wrapper::OnPage { ptr, .. } => unsafe {
                S::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size()
            },
            Wrapper::Searcher { .. } => {
                panic!("tried to get on-page size of a searcher");
            },
        }
    }

    unsafe fn write_value(&self, p: *mut u8) {
        match *self {
            Wrapper::OffPage { ptr } => {
                (*ptr).write_value(mk_mut_slice(p, (*ptr).onpage_size() as usize));
            },
            Wrapper::OnPage { ptr, .. } => {
                // This means that the thing we want to write is already stored in the database
                // somewhere. However it was represented before, we can just copy that
                // representation to the new location.
                std::ptr::copy_nonoverlapping(ptr, p, self.onpage_size() as usize);
            },
            Wrapper::Searcher { .. } => {
                panic!("tried to write a searcher");
            },
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

        let base = txn.load_page(0).data as *mut u8;
        let storage = Storage { base, len: txn.len() as usize, marker: PhantomData };

        let read = |ptr: *const u8| {
            OnPage::<S, T> { ptr, marker: std::marker::PhantomData }.to_stored(&storage)
        };
        match (*self, x) {
            (OnPage { ptr: p, .. }, OffPage { ptr: q }) => (*q).partial_cmp(&read(p)).unwrap(),
            (OffPage { ptr: p }, OnPage { ptr: q, .. }) => (*p).partial_cmp(&read(q)).unwrap(),
            (OnPage { ptr: p, .. }, Searcher { ptr: q }) => (*q).partial_cmp(&read(p)).unwrap(),
            (Searcher { ptr: p }, OnPage { ptr: q, .. }) => (*p).partial_cmp(&read(q)).unwrap(),
            (OnPage { ptr: p, .. }, OnPage { ptr: q, .. }) => read(p).partial_cmp(&read(q)).unwrap(),
            _ => panic!("trying to compare two things that weren't on-page"),
        }
    }

    fn alignment() -> sanakirja::Alignment {
        <S as Stored>::alignment()
    }

    unsafe fn skip(p: *mut u8) -> *mut u8 {
        let size = S::read_header(mk_slice(p, PAGE_SIZE)).onpage_size();
        p.offset(size as isize)
    }

    fn drop_value<R: Rng>(&self, txn: &mut sanakirja::MutTxn, _: &mut R) -> Result<()> {
        let base = txn.load_page(0).data as *mut u8;
        let storage = Storage { base, len: txn.len() as usize, marker: PhantomData };
        unsafe {
            match *self {
                w@Wrapper::OnPage { .. } => {
                    let val = w.to_stored(&storage);
                    // The sanakirja interface doesn't give us any control over the lifetime of the
                    // storage, so we need to hack it in with a transmute.
                    let txn: &mut sanakirja::MutTxn<'sto> = std::mem::transmute(txn);
                    let my_txn = WriteTxn::from_mut_ref(txn);
                    val.drop_value(&mut my_txn.allocator())
                },
                _ => {
                    panic!("tried to drop something that wasn't stored");
                },
            }
        }
    }
}

