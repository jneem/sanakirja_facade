use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use sanakirja;
use std;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use PAGE_SIZE;
use {Alignment, WriteTxn, Result, Storage};

pub trait ReprHeader: Copy {
    type PageOffsets: Iterator<Item=u64>;
    fn onpage_size(&self) -> u16;
    fn page_offsets(&self) -> Self::PageOffsets;
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

pub trait Stored<'sto>: Storable<Self> + Sized {
    type Header: ReprHeader;

    fn header(&self) -> Self::Header;
    fn read_header(buf: &[u8]) -> Self::Header;
    fn read_value(buf: &[u8], s: &Storage<'sto>) -> Self;
    fn drop_value(&self, txn: &mut WriteTxn) -> Result<()>;
}

pub trait Storable<T>: PartialOrd<T> {
    /// Writes `self` to `buf` in a format compatible with `T`.
    fn write_value(&self, buf: &mut [u8]);

    /// How large of a buffer does `write_value` need?
    fn onpage_size(&self) -> u16;

    /// How should `write_value`'s buffer be aligned?
    fn alignment() -> Alignment;
}

/*
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
    fn drop_value(&self, txn: &mut WriteTxn) -> Result<()>;

    /// How much space do we need to write this object to a database?
    fn onpage_size(&self) -> u16 {
        self.header().onpage_size()
    }
}

/// When the type `S` implements `Writable<T>`, it means that we can write `S` into a database that
/// expects to store `T`. This is useful if converting `S` to `T` is expensive, but writing `S` to
/// a database in `T`'s format has the same cost as writing `T`.
///
/// Here's a trivial example it `Writable` in action:
///
/// ```
/// impl Writable<u64> for u32 {
///     fn write_value(&self, buf: &mut [u8]) {
///         LittleEndian::write_u64(buf, *self as u64);
///     }
///     fn onpage_size(&self) -> u16 { 8 }
/// }
/// ```
///
/// With the impl above, we would be able to insert `u32`s into a database that expects `u64`s. Of
/// course this isn't particularly useful, because we could also just cast before inserting. But
/// this would be useful in cases where conversion before insertion is expensive.
///
/// # Warning
///
/// When implementing `Writable<T>`, you need to be very careful that the format you write is
/// identical to the format that `T` expects. Otherwise, you'll probably get database corruption.
pub trait Writable<T>: Ord<T> {
    /// Writes `self` to `buf` in a format compatible with `T`.
    fn write_value(&self, buf: &mut [u8]);

    /// How large of a buffer does `write_value` need?
    fn onpage_size(&self) -> u16;

    /// How should `write_value`'s buffer be aligned?
    fn alignment() -> Alignment;
}
*/

#[derive(Clone, Copy, Debug)]
pub struct U64ReprHeader {}

impl ReprHeader for U64ReprHeader {
    type PageOffsets = std::iter::Empty<u64>;
    fn onpage_size(&self) -> u16 { 8 }
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}

impl Storable<u64> for u64 {
    fn alignment() -> Alignment { Alignment::B8 }
    fn onpage_size(&self) -> u16 { 8 }

    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, *self)
    }
}

impl<'sto> Stored<'sto> for u64 {
    type Header = U64ReprHeader;
    fn header(&self) -> U64ReprHeader { U64ReprHeader {} }
    fn read_header(_: &[u8]) -> U64ReprHeader { U64ReprHeader {} }
    fn drop_value(&self, _: &mut WriteTxn) -> Result<()> { Ok(()) }
    fn read_value(buf: &[u8], _: &Storage<'sto>) -> u64 {
        LittleEndian::read_u64(buf)
    }
}

pub(crate) enum Wrapper<'sto, S: Stored<'sto>, T> {
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
        marker: PhantomData<(S, &'sto ())>,
    },
}

impl<'sto, S: Stored<'sto>, T> Wrapper<'sto, S, T> {
    pub unsafe fn to_stored(&self, storage: &Storage<'sto>) -> S {
        let ptr = match *self {
            Wrapper::OffPage { .. } => {
                panic!("tried to convert something that was off the page")
            }
            Wrapper::OnPage { ptr, .. } => ptr,
        };
        let size = S::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size();
        let slice = mk_slice(ptr, size as usize);
        S::read_value(slice, storage)
    }

    pub fn wrap(val: &T) -> Wrapper<'sto, S, T> {
        Wrapper::OffPage { ptr: val as *const T }
    }
}

impl<'sto, S: Stored<'sto>, T> Debug for Wrapper<'sto, S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            Wrapper::OffPage { ptr } => f.write_fmt(format_args!("OffPage({:?})", ptr)),
            Wrapper::OnPage { ptr, .. } => f.write_fmt(format_args!("OnPage({:?})", ptr)),
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
    let base = loader.load_page(0).data;
    // The cast is ok: when we open the transaction we ensure that its length fits in a usize.
    let storage = Storage { base, len: loader.len() as usize, marker: PhantomData };
    S::read_value(slice, &storage)
 }

impl<'sto, S, T> sanakirja::Representable for Wrapper<'sto, S, T>
where
S: Stored<'sto>,
T: Storable<S>,
{
    type PageOffsets = <<S as Stored<'sto>>::Header as ReprHeader>::PageOffsets;
    fn page_offsets(&self) -> Self::PageOffsets {
        match *self {
            Wrapper::OffPage { .. } => {
                panic!("asked for page offsets of something off-page");
            }
            Wrapper::OnPage { ptr, .. } => {
                unsafe {
                    S::read_header(mk_slice(ptr, PAGE_SIZE)).page_offsets()
                }
            }
        }
    }

    fn onpage_size(&self) -> u16 {
        match *self {
            Wrapper::OffPage { ptr } => unsafe { (*ptr).onpage_size() },
            Wrapper::OnPage { ptr, .. } => unsafe {
                S::read_header(mk_slice(ptr, PAGE_SIZE)).onpage_size()
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
        let read = |ptr: *const u8| read_stored::<S, Tn>(ptr, txn);
        match (*self, x) {
            (OffPage { .. }, OffPage { .. }) => panic!("trying to compare two off-page values"),
            (OnPage { ptr: p, .. }, OffPage { ptr: q }) => (*q).partial_cmp(&read(p)).unwrap(),
            (OffPage { ptr: p }, OnPage { ptr: q, .. }) => (*p).partial_cmp(&read(q)).unwrap(),
            (OnPage { ptr: p, .. }, OnPage { ptr: q, .. }) => read(p).partial_cmp(&read(q)).unwrap(),
        }
    }

    fn alignment() -> sanakirja::Alignment {
        <T as Storable<S>>::alignment()
    }

    unsafe fn skip(p: *mut u8) -> *mut u8 {
        let size = S::read_header(mk_slice(p, PAGE_SIZE)).onpage_size();
        p.offset(size as isize)
    }

    fn drop_value<R: Rng>(&self, txn: &mut sanakirja::MutTxn, _: &mut R) -> Result<()> {
        unsafe {
            // Since MutTxn is just a wrapper around sanakirja::MutTxn, we can convert the pointer.
            // FIXME: oops, this isn't true any more...
            let my_txn = std::mem::transmute::<&mut sanakirja::MutTxn, &mut WriteTxn>(txn);
            match *self {
                Wrapper::OffPage { .. } => { Ok(()) },
                Wrapper::OnPage { ptr: p, .. } => {
                    let val = read_stored::<S, _>(p, txn);
                    val.drop_value(my_txn)
                },
            }
        }
    }
}

