use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use sanakirja;
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

// Here is how representing things should work:
//
// There is a trait representing things that are stored in a database. This includes things like
// page_offsets and drop_value. Call this trait Stored, for now.
//
// A database is parametrized over the types that are stored in it. That is, it's a
// Db<K: Stored, V: Stored>.
//
// In order to look up something in a Db<K, V>, you just need a type that's Ord<K>.
//
// There's another trait representing things that can be written to a database. Call it
// Storable<T: Stored> for now, where S: Storable<T: Stored> means that S can be written to a
// database that stores things of type T.

/// This is the trait for objects that are stored in a database. They should be fairly small, or
/// else it will be costly to read and write them. However, since they can also store references
/// into the database, you can implement `Stored` for a view into a large buffer.
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
/// and `[u8]` implements `Writable<LargeBuf>`, since it's sometimes useful to take a buffer
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

pub(crate) enum Wrapper<'sto, S: Stored<'sto>, T> {
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
    // This is a hack to allow searching for Stored types using something that can be compared to
    // them.
    Searcher {
        ptr: *const T,
    }
}

impl<'sto, S: Stored<'sto>, T> Wrapper<'sto, S, T> {
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

    pub fn wrap(val: &S) -> Wrapper<'sto, S, T> {
        Wrapper::OffPage { ptr: val as *const S }
    }

    pub fn search(val: &T) -> Wrapper<'sto, S, T> {
        Wrapper::Searcher { ptr: val as *const T }
    }
}

impl<'sto, S: Stored<'sto>, T> Debug for Wrapper<'sto, S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            Wrapper::OffPage { ptr } => f.write_fmt(format_args!("OffPage({:?})", ptr)),
            Wrapper::OnPage { ptr, .. } => f.write_fmt(format_args!("OnPage({:?})", ptr)),
            Wrapper::Searcher { ptr } => f.write_fmt(format_args!("Searcher({:?})", ptr)),
        }
    }
}

impl<'sto, S: Stored<'sto>, T> Copy for Wrapper<'sto, S, T> { }

impl<'sto, S: Stored<'sto>, T> Clone for Wrapper<'sto, S, T> {
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

unsafe fn read_stored<'sto, S, L>(ptr: *const u8, loader: &L) -> S
where
S: Stored<'sto>,
L: sanakirja::LoadPage,
{
    let size = S::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size();
    let slice = mk_slice(ptr, size as usize);

    // Find the storage buffer underlying `loader` and make a `Storage` struct out of it.
    let base = loader.load_page(0).data as *mut u8;
    // The cast is ok: when we open the transaction we ensure that its length fits in a usize.
    let storage = Storage { base, len: loader.len() as usize, marker: PhantomData };
    S::read_value(slice, &storage)
 }

impl<'sto, S, T> sanakirja::Representable for Wrapper<'sto, S, T>
where
S: Stored<'sto>,
T: Storable<'sto, S>,
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
                // TODO: explain this
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
        let read = |ptr: *const u8| read_stored::<S, Tn>(ptr, txn);
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
        unsafe {
            match *self {
                Wrapper::OnPage { ptr: p, .. } => {
                    let val = read_stored::<S, _>(p, txn);
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

