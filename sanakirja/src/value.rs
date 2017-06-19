// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use {PageT, MutTxn, PAGE_SIZE, Representable};
use transaction::LoadPage;
use std;
use std::marker::PhantomData;
const VALUE_HEADER_LEN: usize = 8;

/// Unsafe values, the unsafe representation of a value.
#[derive(Clone,Copy)]
pub enum UnsafeValue {
    #[doc(hidden)]
    Direct { p: *const u8, len: u64 },

    #[doc(hidden)]
    Large { offset: u64, len: u64 },
}
impl std::fmt::Debug for UnsafeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        match *self {
            UnsafeValue::Direct { p, len } => {
                let x = std::str::from_utf8(unsafe {
                    std::slice::from_raw_parts(p, std::cmp::min(len as usize, 10))
                });
                if let Ok(x) = x {
                    try!(write!(f, "UnsafeValue::Direct({})", x))
                } else {
                    try!(write!(f, "UnsafeValue::Direct()"))
                }
            }
            UnsafeValue::Large { offset, len } => {
                try!(write!(f, "UnsafeValue::Large({:?}, {:?})", offset, len))
            }
        }
        Ok(())
    }
}


/// Maximum size of a value inlined on a page. Values larger than this
/// must be stored in separate pages.
pub const MAX_INLINE_SIZE: u64 = 506;

impl Representable for UnsafeValue {
    fn onpage_size(&self) -> u16 {
        match *self {
            UnsafeValue::Direct { len, .. } => len as u16 + 8,
            UnsafeValue::Large { .. } => 16,
        }
    }
    unsafe fn write_value(&self, ptr: *mut u8) {
        debug!("write_value {:?}", ptr);
        match *self {
            UnsafeValue::Direct { len, p } => {
                assert!(len <= MAX_INLINE_SIZE);
                {
                    let ptr = ptr as *mut u64;
                    *ptr = len.to_le();
                }
                std::ptr::copy_nonoverlapping(p, ptr.offset(8), len as usize)
            }
            UnsafeValue::Large { len, offset } => {
                let ptr = ptr as *mut u64;
                *ptr = len.to_le();
                *(ptr.offset(1)) = offset.to_le()
            }
        }
    }
    unsafe fn read_value(ptr: *const u8) -> Self {
        let len = u64::from_le(*(ptr as *const u64));
        if len <= MAX_INLINE_SIZE {
            UnsafeValue::Direct {
                len: len,
                p: ptr.offset(8),
            }
        } else {
            let ptr = ptr as *const u64;
            UnsafeValue::Large {
                len: len,
                offset: u64::from_le(*(ptr.offset(1))),
            }
        }
    }

    unsafe fn cmp_value<T: LoadPage>(&self, txn: &T, y: Self) -> std::cmp::Ordering {
        let x = Value::from_unsafe(self, txn);
        let y = Value::from_unsafe(&y, txn);
        use std::iter::Iterator;
        x.cmp(y)
    }

    fn drop_value<T, R>(&self, txn: &mut MutTxn<T>, _: &mut R) -> Result<(), super::Error> {
        match *self {
            UnsafeValue::Large { mut offset, mut len } => {
                debug!("drop value {:?} {:?}", offset, len);
                loop {
                    let page = txn.load_page(offset).offset(0);
                    if len > PAGE_SIZE as u64 {
                        len -= PAGE_SIZE as u64 - VALUE_HEADER_LEN as u64;
                        unsafe {
                            let next_offset = u64::from_le(*(page as *const u64));
                            txn.free_page(offset);
                            offset = next_offset
                        }
                    } else {
                        unsafe {
                            txn.free_page(offset);
                        }
                        break;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    type PageOffsets = std::iter::Once<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        match *self {
            UnsafeValue::Large { offset, .. } => std::iter::once(offset),
            UnsafeValue::Direct { .. } => {
                let mut x = std::iter::once(0);
                x.next();
                x
            },
        }

    }
}




/// Iterator over parts of a value. On values of size at most 4096
/// bytes, the iterator will run exactly once. On larger values, it
/// returns all parts of the value, in order.
pub enum Value<'a, T: 'a> {
    /// On-page value.
    Direct {
        /// Pointer to the value.
        p: *const u8,
        /// Length.
        len: u64,
        /// Lifetime marker.
        txn: PhantomData<&'a T>,
    },

    /// Indirect value, just an offset to a page and a length are
    /// stored on the page, as two u64.
    Large {
        /// Transaction from which this page can be retrieved.
        txn: &'a T,
        /// Offset of the page from the beginning of the file.
        offset: u64,
        /// Total length of this value.
        len: u64,
    },
}

impl<'a, T: LoadPage> std::fmt::Debug for Value<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        let it: Value<_> = self.clone();
        try!(write!(f, "Value ({:?}) {{ value: [", self.len()));
        let mut first = true;
        for x in it {
            if !first {
                try!(write!(f, ", {:?}", std::str::from_utf8(x)))
            } else {
                try!(write!(f, "{:?}", std::str::from_utf8(x)));
                first = false;
            }
        }
        try!(write!(f, "] }}"));
        Ok(())
    }
}


impl<'a, T: LoadPage> Iterator for Value<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            &mut Value::Large { ref txn, ref mut offset, ref mut len } => {
                debug!("iterator: {:?}, {:?}", offset, len);
                if *len == 0 {
                    None
                } else {
                    if *len <= PAGE_SIZE as u64 {
                        unsafe {
                            let page = txn.load_page(*offset).offset(0);
                            let slice = std::slice::from_raw_parts(page.offset(0), *len as usize);
                            *len = 0;
                            Some(slice)
                        }
                    } else {
                        unsafe {
                            let page = txn.load_page(*offset).offset(0);
                            // change the pointer of "current page" to the next page
                            *offset = u64::from_le(*(page as *const u64));
                            let l = PAGE_SIZE - VALUE_HEADER_LEN as u32;
                            *len -= l as u64;
                            Some(std::slice::from_raw_parts(page.offset(VALUE_HEADER_LEN as isize),
                                                            l as usize))
                        }
                    }
                }
            }
            &mut Value::Direct { ref mut len, ref mut p, .. } => unsafe {
                // Rust iterators can compare iterators returning slices of equal length.
                if p.is_null() {
                    None
                } else if *len <= PAGE_SIZE as u64 {
                    let s = std::slice::from_raw_parts(*p, *len as usize);
                    *p = std::ptr::null_mut();
                    Some(s)
                } else {
                    let l = PAGE_SIZE as usize - VALUE_HEADER_LEN;
                    let s = std::slice::from_raw_parts(*p, l);
                    *p = p.offset(l as isize);
                    *len -= l as u64;
                    Some(s)
                }
            },
        }
    }
}

impl UnsafeValue {
    /// Length of this unsafe value
    pub fn len(&self) -> u64 {
        match self {
            &UnsafeValue::Direct { len, .. } => len,
            &UnsafeValue::Large { len, .. } => len,
        }
    }

    /// Unsafely turn this value into a slice, if it is inlined on a page. Panics else.
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        match self {
            &UnsafeValue::Direct { p, len } => std::slice::from_raw_parts(p, len as usize),
            &UnsafeValue::Large { .. } => unimplemented!(),
        }
    }

    /// Converts a slice of bytes into an unsafe value.
    pub fn from_slice(slice: &[u8]) -> UnsafeValue {
        UnsafeValue::Direct {
            p: slice.as_ptr(),
            len: slice.len() as u64,
        }
    }

    /// Allocates a large value if needed, or else return a pointer to the given slice.
    pub fn alloc_if_needed<T>(txn: &mut MutTxn<T>,
                              value: &[u8])
                              -> Result<UnsafeValue, super::Error> {
        if value.len() > MAX_INLINE_SIZE as usize {
            Self::alloc_large(txn, value)
        } else {
            Ok(UnsafeValue::Direct {
                p: value.as_ptr(),
                len: value.len() as u64,
            })
        }
    }

    /// Allocate an unsafe large value. This function always allocate
    /// and returns `UnsafeValue::Large`, no matter what it receives
    /// as input
    fn alloc_large<T>(txn: &mut MutTxn<T>, value: &[u8]) -> Result<UnsafeValue, super::Error> {
        debug!("alloc_value");
        let mut len = value.len();
        let mut p_value = value.as_ptr();
        let mut page = try!(txn.alloc_page());
        let first_page = page.page_offset();
        unsafe {
            loop {
                if len <= PAGE_SIZE as usize {
                    std::ptr::copy_nonoverlapping(p_value, page.offset(0), len);
                    break;
                } else {
                    std::ptr::copy_nonoverlapping(p_value, page.offset(8), PAGE_SIZE as usize - 8);
                    p_value = p_value.offset((PAGE_SIZE - 8) as isize);
                    len -= PAGE_SIZE as usize - 8;
                    let next_page = try!(txn.alloc_page());
                    *(page.offset(0) as *mut u64) = next_page.page_offset().to_le();
                    page = next_page
                }
            }
        }
        debug_assert!(first_page > 0);
        debug!("/alloc_value");
        Ok(UnsafeValue::Large {
            offset: first_page,
            len: value.len() as u64,
        })
    }
}

impl<'a, T: LoadPage> Value<'a, T> {
    /// Length of this value.
    pub fn len(&self) -> u64 {
        match self {
            &Value::Direct { len, .. } => len,
            &Value::Large { len, .. } => len,
        }
    }

    /// Clone the pointers to this value. This is actually unsafe, as
    /// it duplicates a pointer, and several copies might be stored in
    /// the database without incrementing their RC.
    ///
    /// As long as it is never put back into a database, it should be safe.
    pub fn clone(&self) -> Value<'a, T> {
        match self {
            &Value::Direct { p, len, txn } => {
                Value::Direct {
                    len: len,
                    p: p,
                    txn: txn,
                }
            }
            &Value::Large { offset, len, txn } => {
                Value::Large {
                    len: len,
                    offset: offset,
                    txn: txn,
                }
            }
        }
    }

    /// Turns an `UnsafeValue` into a `Value`. The database from which
    /// that `UnsafeValue` comes must not have changed since the
    /// `UnsafeValue` was retrieved.
    pub unsafe fn from_unsafe(u: &UnsafeValue, txn: &'a T) -> Value<'a, T> {
        match *u {
            UnsafeValue::Direct { p, len } => {
                Value::Direct {
                    p: p,
                    len: len,
                    txn: PhantomData,
                }
            }
            UnsafeValue::Large { offset, len } => {
                Value::Large {
                    len: len,
                    offset: offset,
                    txn: txn,
                }
            }
        }
    }


    /// Turns a value into an unsafe value, ready to be inserted into a database.
    pub fn to_unsafe(&self) -> UnsafeValue {
        match *self {
            Value::Direct { p, len, .. } => {
                assert!(len <= MAX_INLINE_SIZE);
                UnsafeValue::Direct { p: p, len: len }
            }
            Value::Large { offset, len, .. } => {
                UnsafeValue::Large {
                    len: len,
                    offset: offset,
                }
            }
        }
    }

    /// Produces a direct value from a slice. Might exceed the maximal page size.
    pub fn from_slice(slice: &'a [u8]) -> Value<'a, T> {
        Value::Direct {
            p: slice.as_ptr(),
            len: slice.len() as u64,
            txn: PhantomData,
        }
    }

    /// Turn this value into a slice, if it is inlined on a page. Panics else.
    pub fn as_slice(&self) -> &'a [u8] {
        match self {
            &Value::Direct { p, len, .. } => unsafe { std::slice::from_raw_parts(p, len as usize) },
            &Value::Large { .. } => unimplemented!(),
        }
    }

    /// Turns this value into a `std::borrow::Cow`, allocating a `Vec<u8>` if it is too large.
    pub fn into_cow(&'a self) -> std::borrow::Cow<'a, [u8]> {
        match self {
            &Value::Direct { p, len, .. } => {
                std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(p, len as usize) })
            }
            &Value::Large { .. } => {
                let mut v = Vec::new();
                for chunk in self.clone() {
                    v.extend(chunk)
                }
                std::borrow::Cow::Owned(v)
            }
        }
    }
}
