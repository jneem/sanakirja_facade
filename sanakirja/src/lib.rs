// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Fast and reliable key-value store, under the Mozilla Public
//! License (link as you like, share modifications).
//!
//! # Features
//!
//! - ACID semantics.
//!
//! - B trees with copy-on-write.
//!
//! - Support for referential transparency: databases can be cloned in time O(1).
//!
//! - Ultimately, we'd like to have no locks. Right now, there is a
//! cross-process read write lock, that only ```commit``` takes
//! exclusively (other parts of a mutable transaction need just a read
//! access).
//!
//!
//! This version is only capable of inserting and retrieving keys in
//! the database, allowing several bindings for the same key (get will
//! retrieve the first one).
//!

#![deny(trivial_casts, trivial_numeric_casts,
        unused_import_braces, unused_qualifications)]

extern crate rand;
#[cfg(test)]
extern crate rustc_serialize;

#[macro_use]
extern crate log;
extern crate fs2;
extern crate memmap;

mod transaction;

pub use transaction::{Txn, MutTxn, Env, Commit, Page};
use transaction::{MutPage, Cow};
use skiplist::SkipCursor;
use rand::Rng;
/// A database is a skip list of (page offset, key, value).
#[derive(Clone, Copy, Debug)]
pub struct Db<K: Representable, V: Representable>(pub u64, pub std::marker::PhantomData<(K, V)>);

/// Values, which might be either inlined on the page, or stored as a reference if too large.
pub mod value;

pub use transaction::LoadPage;

/// Alignment of representables on page. The only implication is on how the `write_value` and `read_value` methods are going to be implemented.
#[repr(u16)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum Alignment {
    /// 1 byte-aligned
    B1 = 1,
    /// 2 bytes-aligned (i.e. this is a pointer to a u16 or i16)
    B2 = 2,
    /// 4 bytes-aligned (i.e. this is a pointer to a u32 or i32)
    B4 = 4,
    /// 8 bytes-aligned (i.e. this is a pointer to a u64 or i64)
    B8 = 8
}

/// Types that can be stored in a Sanakirja database, as keys or
/// values. Some care must be taken when storing things.
pub trait Representable: Copy + std::fmt::Debug {

    /// An iterator over the offsets to pages contained in this
    /// value. Only values from this crate can generate non-empty
    /// iterators, but combined values (like tuples) must chain the
    /// iterators returned by method `page_offsets`.
    type PageOffsets: Iterator<Item = u64>;

    /// Alignment of this type. Allowed values are 1, 2, 4 and 8.
    fn alignment() -> Alignment {
        Alignment::B8
    }

    /// How much space this value occupies on the page (not counting alignment padding).
    fn onpage_size(&self) -> u16;

    /// First pointer strictly after this value's pointer. The default
    /// implementation is basically `p.offset(self.onpage_size() as
    /// isize)`, but their might be more efficient implementations in
    /// some cases.
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        // debug!("skip {:?}", p);
        let r = Self::read_value(p);
        // debug!("skip, r = {:?}", r);
        p.offset(r.onpage_size() as isize)
    }

    /// Write this value to a u8 pointer, guaranteed to be a multiple of `self.alignment()`.
    unsafe fn write_value(&self, p: *mut u8);

    /// Read value from an onpage pointer, guaranteed to be a multiple of `self.alignment()`.
    unsafe fn read_value(p: *const u8) -> Self;

    /// Compare a value with an onpage value. The current transaction
    /// is sometimes helpful, for instance when the page only stores a
    /// pointer to another page.
    unsafe fn cmp_value<T: LoadPage>(&self, txn: &T, x: Self) -> std::cmp::Ordering;

    /// How to free the pages used by this value. The default
    /// implementation doesn't do anything, which is fine for types
    /// stored directly on B tree pages.
    fn drop_value<R:Rng>(&self, _: &mut MutTxn, _:&mut R) -> Result<(), Error> { Ok(()) }

    /// If this value is an offset to another page at offset `offset`,
    /// return `Some(offset)`. Return `None` else.
    fn page_offsets(&self) -> Self::PageOffsets;
}


impl<A:Representable, B:Representable> Representable for (A, B) {
    fn alignment() -> Alignment {
        std::cmp::max(A::alignment(), B::alignment())
    }
    fn onpage_size(&self) -> u16 {
        let a_size = self.0.onpage_size();
        let b_size = self.1.onpage_size();

        let b_align = B::alignment() as u16;

        // Padding needed between a and b.
        let b_padding = (b_align - (a_size % b_align)) % b_align;

        let size = a_size + b_size + b_padding;
        debug!("size: {:?}", size);
        size
    }

    unsafe fn write_value(&self, p: *mut u8) {

        self.0.write_value(p);

        let a_size = self.0.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;

        self.1.write_value(p.offset((a_size + b_padding) as isize))
    }

    unsafe fn read_value(p: *const u8) -> Self {

        let a = A::read_value(p);
        let a_size = a.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;

        let a = A::read_value(p);
        let b = B::read_value(p.offset((a_size + b_padding) as isize));
        (a, b)
    }

    unsafe fn cmp_value<T: LoadPage>(&self, txn: &T, x: Self) -> std::cmp::Ordering {
        let ord = self.0.cmp_value(txn, x.0);
        if let std::cmp::Ordering::Equal = ord {
            self.1.cmp_value(txn, x.1)
        } else {
            ord
        }
    }

    type PageOffsets = std::iter::Chain<A::PageOffsets, B::PageOffsets>;
    fn page_offsets(&self) -> Self::PageOffsets {
        self.0.page_offsets().chain(self.1.page_offsets())
    }

    fn drop_value<R:Rng>(&self, txn: &mut MutTxn, rng: &mut R) -> Result<(), Error> {
        try!(self.0.drop_value(txn, rng));
        try!(self.1.drop_value(txn, rng));
        Ok(())
    }
}

impl<A:Representable, B:Representable, C:Representable> Representable for (A, B, C) {
    fn alignment() -> Alignment {
        std::cmp::max(std::cmp::max(A::alignment(), B::alignment()), C::alignment())
    }
    fn onpage_size(&self) -> u16 {
        let a_size = self.0.onpage_size();
        let b_size = self.1.onpage_size();
        let c_size = self.2.onpage_size();

        let b_align = B::alignment() as u16;
        let c_align = C::alignment() as u16;

        // Padding needed between a and b.
        let b_padding = (b_align - (a_size % b_align)) % b_align;
        let a_b_size = a_size+b_size+b_padding;
        // Padding between b and c
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;

        let size = a_b_size + c_size + c_padding;
        debug!("size: {:?}", size);
        size
    }

    unsafe fn write_value(&self, p: *mut u8) {

        self.0.write_value(p);

        let a_size = self.0.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;

        self.1.write_value(p.offset((a_size + b_padding) as isize));

        let b_size = self.1.onpage_size();
        let a_b_size = a_size+b_size+b_padding;
        let c_align = C::alignment() as u16;
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;

        self.2.write_value(p.offset((a_b_size + c_padding) as isize))
    }

    unsafe fn read_value(p: *const u8) -> Self {

        let a = A::read_value(p);
        let a_size = a.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;
        let b = B::read_value(p.offset((a_size + b_padding) as isize));

        let b_size = b.onpage_size();
        let a_b_size = a_size+b_size+b_padding;
        let c_align = C::alignment() as u16;
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;

        let c = C::read_value(p.offset((a_b_size + c_padding) as isize));
        (a, b, c)
    }

    unsafe fn cmp_value<T: LoadPage>(&self, txn: &T, x: Self) -> std::cmp::Ordering {
        let ord = self.0.cmp_value(txn, x.0);
        if let std::cmp::Ordering::Equal = ord {
            let ord = self.1.cmp_value(txn, x.1);
            if let std::cmp::Ordering::Equal = ord {
                self.2.cmp_value(txn, x.2)
            } else {
                ord
            }
        } else {
            ord
        }
    }

    type PageOffsets = std::iter::Chain<std::iter::Chain<A::PageOffsets, B::PageOffsets>, C::PageOffsets>;
    fn page_offsets(&self) -> Self::PageOffsets {
        self.0.page_offsets().chain(self.1.page_offsets()).chain(self.2.page_offsets())
    }

    fn drop_value<R:Rng>(&self, txn: &mut MutTxn, rng: &mut R) -> Result<(), Error> {
        try!(self.0.drop_value(txn, rng));
        try!(self.1.drop_value(txn, rng));
        try!(self.2.drop_value(txn, rng));
        Ok(())
    }
}





impl<A:Representable, B:Representable, C:Representable, D:Representable> Representable for (A, B, C, D) {
    fn alignment() -> Alignment {
        std::cmp::max(
            std::cmp::max(A::alignment(), B::alignment()),
            std::cmp::max(C::alignment(), D::alignment())
        )
    }
    fn onpage_size(&self) -> u16 {
        let a_size = self.0.onpage_size();
        let b_size = self.1.onpage_size();
        let c_size = self.2.onpage_size();
        let d_size = self.3.onpage_size();

        let b_align = B::alignment() as u16;
        let c_align = C::alignment() as u16;
        let d_align = D::alignment() as u16;

        // Padding needed between a and b.
        let b_padding = (b_align - (a_size % b_align)) % b_align;
        let a_b_size = a_size+b_size+b_padding;

        // Padding between b and c
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;
        let a_b_c_size = a_b_size + c_size + c_padding;

        // Padding between c and d
        let d_padding = (d_align - (a_b_c_size % d_align)) % d_align;
        let a_b_c_d_size = a_b_c_size + d_size + d_padding;
        a_b_c_d_size
    }

    unsafe fn write_value(&self, p: *mut u8) {

        self.0.write_value(p);

        let a_size = self.0.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;

        self.1.write_value(p.offset((a_size + b_padding) as isize));

        let b_size = self.1.onpage_size();
        let a_b_size = a_size+b_size+b_padding;
        let c_align = C::alignment() as u16;
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;

        self.2.write_value(p.offset((a_b_size + c_padding) as isize));

        let c_size = self.2.onpage_size();
        let a_b_c_size = a_b_size + c_size + c_padding;
        let d_align = D::alignment() as u16;
        let d_padding = (d_align - (a_b_c_size % d_align)) % d_align;

        self.3.write_value(p.offset((a_b_c_size + d_padding) as isize));

    }

    unsafe fn read_value(p: *const u8) -> Self {

        let a = A::read_value(p);
        let a_size = a.onpage_size();
        let b_align = B::alignment() as u16;
        let b_padding = (b_align - (a_size % b_align)) % b_align;
        let b = B::read_value(p.offset((a_size + b_padding) as isize));

        let b_size = b.onpage_size();
        let a_b_size = a_size+b_size+b_padding;
        let c_align = C::alignment() as u16;
        let c_padding = (c_align - (a_b_size % c_align)) % c_align;
        let c = C::read_value(p.offset((a_b_size + c_padding) as isize));

        let c_size = c.onpage_size();
        let a_b_c_size = a_b_size + c_size + c_padding;
        let d_align = D::alignment() as u16;
        let d_padding = (d_align - (a_b_c_size % d_align)) % d_align;
        let d = D::read_value(p.offset((a_b_c_size + d_padding) as isize));

        (a, b, c, d)
    }

    unsafe fn cmp_value<T: LoadPage>(&self, txn: &T, x: Self) -> std::cmp::Ordering {
        let ord = self.0.cmp_value(txn, x.0);
        if let std::cmp::Ordering::Equal = ord {
            let ord = self.1.cmp_value(txn, x.1);
            if let std::cmp::Ordering::Equal = ord {
                let ord = self.2.cmp_value(txn, x.2);
                if let std::cmp::Ordering::Equal = ord {
                    self.3.cmp_value(txn, x.3)
                } else {
                    ord
                }
            } else {
                ord
            }
        } else {
            ord
        }
    }

    type PageOffsets = std::iter::Chain<std::iter::Chain<A::PageOffsets, B::PageOffsets>, std::iter::Chain<C::PageOffsets, D::PageOffsets>>;
    fn page_offsets(&self) -> Self::PageOffsets {
        self.0.page_offsets().chain(self.1.page_offsets())
            .chain(self.2.page_offsets().chain(self.3.page_offsets()))
    }

    fn drop_value<R:Rng>(&self, txn: &mut MutTxn, rng: &mut R) -> Result<(), Error> {
        try!(self.0.drop_value(txn, rng));
        try!(self.1.drop_value(txn, rng));
        try!(self.2.drop_value(txn, rng));
        try!(self.3.drop_value(txn, rng));
        Ok(())
    }
}








impl Representable for u64 {
    fn alignment() -> Alignment {
        Alignment::B8
    }
    fn onpage_size(&self) -> u16 {
        8
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p.offset(8)
    }
    unsafe fn write_value(&self, p: *mut u8) {
        *(p as *mut u64) = self.to_le()
    }
    unsafe fn read_value(p: *const u8) -> Self {
        u64::from_le(*(p as *const u64))
    }
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::empty()
    }
}

impl Representable for u32 {
    fn alignment() -> Alignment {
        Alignment::B4
    }
    fn onpage_size(&self) -> u16 {
        4
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p.offset(4)
    }
    unsafe fn write_value(&self, p: *mut u8) {
        *(p as *mut u32) = self.to_le()
    }
    unsafe fn read_value(p: *const u8) -> Self {
        u32::from_le(*(p as *const u32))
    }
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::empty()
    }
}


impl Representable for i64 {
    fn alignment() -> Alignment {
        Alignment::B8
    }
    fn onpage_size(&self) -> u16 {
        8
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p.offset(8)
    }
    unsafe fn write_value(&self, p: *mut u8) {
        *(p as *mut i64) = self.to_le()
    }
    unsafe fn read_value(p: *const u8) -> Self {
        i64::from_le(*(p as *const i64))
    }
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::empty()
    }
}

impl Representable for i32 {
    fn alignment() -> Alignment {
        Alignment::B4
    }
    fn onpage_size(&self) -> u16 {
        4
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p.offset(8)
    }
    unsafe fn write_value(&self, p: *mut u8) {
        debug!("write_value i64: {:?} {:?}", *self, p);
        *(p as *mut i32) = self.to_le()
    }
    unsafe fn read_value(p: *const u8) -> Self {
        i32::from_le(*(p as *const i32))
    }
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::empty()
    }
}



impl Representable for () {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        0
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p
    }
    unsafe fn write_value(&self, _: *mut u8) {}
    unsafe fn read_value(_: *const u8) -> Self {}
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, _: Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::empty()
    }
}

impl<K:Representable, V:Representable> Representable for Db<K,V> {
    fn alignment() -> Alignment {
        Alignment::B8
    }
    fn onpage_size(&self) -> u16 {
        8
    }
    unsafe fn skip(p: *mut u8) -> *mut u8 {
        p.offset(8)
    }
    unsafe fn write_value(&self, p: *mut u8) {
        debug!("write_value u64: {:?} {:?}", *self, p);
        *(p as *mut u64) = self.0.to_le()
    }
    unsafe fn read_value(p: *const u8) -> Self {
        Db(u64::from_le(*(p as *const u64)), std::marker::PhantomData)
    }
    unsafe fn cmp_value<T: LoadPage>(&self, _: &T, x: Db<K,V>) -> std::cmp::Ordering {
        self.0.cmp(&x.0)
    }
    type PageOffsets = std::iter::Once<u64>;
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::once(self.0)
    }
    fn drop_value<R:Rng>(&self, txn: &mut MutTxn, rng: &mut R) -> Result<(), Error> {
        txn.drop(rng, self)
    }
}



#[doc(hidden)]
#[derive(Debug, Copy, Clone)]
pub struct PageCursor {
    page: u64,
    cursor: SkipCursor,
}

impl std::ops::Index<usize> for PageCursor {
    type Output = u16;
    fn index(&self, i: usize) -> &u16 {
        self.cursor.levels.index(i)
    }
}

impl std::ops::IndexMut<usize> for PageCursor {
    fn index_mut(&mut self, i: usize) -> &mut u16 {
        self.cursor.levels.index_mut(i)
    }
}


impl PageCursor {
    fn new() -> Self {
        PageCursor {
            page: 0,
            cursor: SkipCursor::new(),
        }
    }
}
#[doc(hidden)]
pub const N_CURSORS: usize = 30; // should be 64, but [*;64] doesn't derive Debug.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct CursorStack {
    stack: [PageCursor; N_CURSORS],
    first_rc_level: usize,
    pointer: usize,
}

impl CursorStack {
    #[doc(hidden)]
    pub fn new() -> Self {
        CursorStack {
            stack: [PageCursor::new(); N_CURSORS],
            first_rc_level: N_CURSORS,
            pointer: 1,
        }
    }
}
impl std::ops::Index<usize> for CursorStack {
    type Output = PageCursor;
    fn index(&self, i: usize) -> &PageCursor {
        self.stack.index(i)
    }
}

impl std::ops::IndexMut<usize> for CursorStack {
    fn index_mut(&mut self, i: usize) -> &mut PageCursor {
        self.stack.index_mut(i)
    }
}


impl CursorStack {
    fn current_mut(&mut self) -> &mut PageCursor {
        &mut self.stack[self.pointer]
    }
    fn current(&self) -> &PageCursor {
        &self.stack[self.pointer]
    }
    fn set<T: skiplist::SkipList + Transaction, K: Representable, V: Representable>
        (&mut self,
         txn: &T,
         db: &Db<K, V>,
         k: Option<(K, Option<V>)>)
         -> Result<bool, Error> {

            debug!("db = {:?}", db.0);
            self.stack[1].page = db.0;
            self.pointer = 1;

            // Set the "cursor stack" by setting a skip list cursor in
            // each page from the root to the appropriate leaf.
            let mut last_matching_page = 0;
            loop {
                debug!("self.stack: {:?} {:?}",
                       &self.stack[..self.pointer+1], self.pointer);
                let page = txn.load_page(self.stack[self.pointer].page);
                if self.first_rc_level >= N_CURSORS && txn.rc(self.stack[self.pointer].page) >= 2 {
                    debug!("first_rc_level: {:?} {:?}",
                           self.pointer,
                           self.stack[self.pointer].page);
                    self.first_rc_level = self.pointer
                }
                if let Some((k, v)) = k {
                    if txn.skiplist_set_cursor(&page, &mut self.stack[self.pointer].cursor, k, v).is_some() {
                        debug!("found on page {:?}", page.page_offset());
                        if v.is_some() {
                            debug!("found v");
                            return Ok(true);
                        } else {
                            last_matching_page = self.pointer
                        }
                    }
                }
                let next_page = page.right_child(self.stack[self.pointer][0]);

                debug!("next_page = {:?}, cursors: {:?}",
                       next_page,
                       self.stack[self.pointer].cursor);
                if next_page > 0 {
                    self.pointer += 1;
                    self.stack[self.pointer].page = next_page;
                } else {
                    break
                }
            }
            if last_matching_page > 0 {
                debug!("last_matching_page: {:?}", last_matching_page);
                debug!("cursors: {:?}", &self.stack[..self.pointer+1]);
                // self.set(txn, db, k, Some(last_matching_page))
                self.pointer = last_matching_page;
                Ok(true)
            } else {
                debug!("not found");
                Ok(false)
            }
        }
}

/// An iterator over a database.
#[derive(Clone)]
pub struct Cursor<'a, T: LoadPage + 'a, K, V> {
    txn: &'a T,
    stack: CursorStack,
    marker: std::marker::PhantomData<(K, V)>,
}

// Cursor invariant: at every step, the whole stack is smaller than
// the current point. When popping the stack, the skiplist cursor
// below the top needs to be moved forward.
//
// At all other steps, either we push, or we move forward by one
// binding in a skiplist.
use skiplist::SkipListPage;
impl<'a, T: Transaction + 'a, K: Representable, V: Representable> Iterator for Cursor<'a, T, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.pointer == 0 {
            None
        } else {

            let page = self.stack[self.stack.pointer].page;
            debug!("next(), pointer: {:?}", self.stack.pointer);
            debug!("PAGE: {:?}", page);
            let page = self.txn.load_page(page);
            let pos = self.stack[self.stack.pointer][0];
            debug!("pos = {:?}", pos);
            if pos == skiplist::SKIPLIST_ROOT {

                // First element of a page (not a binding).
                let right_child = page.right_child(pos);
                // Then visit the right child (if any), i.e. push.
                if right_child != 0 {
                    // visit right child first
                    self.stack.pointer += 1;
                    let p = self.stack.pointer;
                    self.stack[p].page = right_child;
                    let page = self.txn.load_page(right_child);
                    page.skiplist_set_cursor_root(&mut self.stack[p].cursor);
                } else {
                    // No right child, move forward.
                    let p = self.stack.pointer;
                    if !page.skiplist_move_cursor_forward(&mut self.stack[p].cursor) {
                        self.stack[p][0] = skiplist::NIL
                    }
                }

                // In either case, this was not a binding (it was a
                // skiplist root), there is nothing to return. Return
                // the next binding.
                self.next()

            } else if pos == skiplist::NIL {

                // End of current skiplist, pop.
                debug!("POP!");
                // Pop.
                self.stack.pointer -= 1;
                // If we're at the end of the stack, the iterator is finished.
                // Else, return the next binding.
                if self.stack.pointer > 0 {
                    let p = self.stack.pointer;
                    let page = self.txn.load_page(self.stack[p].page);
                    if !page.skiplist_move_cursor_forward(&mut self.stack[p].cursor) {
                        self.stack[p][0] = skiplist::NIL
                    }
                    self.next()
                } else {
                    None
                }

            } else {

                // Find the current key (to be returned).
                let (cur_key, cur_value) = unsafe { page.read_key_value::<K, V>(pos) };

                let right_child = page.right_child(pos);
                if right_child != 0 {
                    // If the current binding has a right child, push
                    // the next page onto the stack.
                    self.stack.pointer += 1;
                    let p = self.stack.pointer;
                    self.stack[p].page = right_child;
                    let page = self.txn.load_page(right_child);
                    page.skiplist_set_cursor_root(&mut self.stack[p].cursor);
                } else {
                    // Else, simply move forward on this page (hint:
                    // we're at a leaf).
                    let p = self.stack.pointer;
                    if !page.skiplist_move_cursor_forward(&mut self.stack[p].cursor) {
                        self.stack[p][0] = skiplist::NIL
                    }
                }

                // Return the "current" key (well, current at the beginning of this step).
                Some((cur_key, cur_value))
            }
        }
    }
}



/// An iterator over a database.
#[derive(Clone)]
pub struct RevCursor<'a, T: Transaction + 'a, K, V> {
    txn: &'a T,
    stack: CursorStack,
    marker: std::marker::PhantomData<(K, V)>,
}

// Cursor invariant: at every step, the whole stack is smaller than
// the current point. When popping the stack, the skiplist cursor
// below the top needs to be moved forward.
//
// At all other steps, either we push, or we move forward by one
// binding in a skiplist.

impl<'a, T: Transaction + 'a, K: Representable, V: Representable> Iterator for RevCursor<'a, T, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.pointer == 0 {
            None
        } else {

            let page = self.stack[self.stack.pointer].page;
            debug!("next(), pointer: {:?}", self.stack.pointer);
            debug!("PAGE: {:?}", page);
            let page = self.txn.load_page(page);
            let pos = self.stack[self.stack.pointer][0];
            debug!("pos = {:?}", pos);
            if pos == skiplist::SKIPLIST_ROOT {
                // We've reached the beginning of this page.
                self.stack.pointer -= 1;
                // If we're at the end of the stack, the iterator is finished.
                // Else, return the next binding.
                if self.stack.pointer > 0 {
                    self.next()
                } else {
                    None
                }

            } else {

                // Find the current key (to be returned).
                let (cur_key, cur_value) = unsafe { page.read_key_value::<K, V>(pos) };

                // Is this page a leaf?
                let right_child = page.right_child(pos);
                if right_child != 0 {
                    // If the current binding has a right child, this
                    // page is not a leaf.
                    //
                    // Move the cursor backward on this page, and set
                    // the cursor to the new element's rightmost
                    // descendant.
                    let p = self.stack.pointer;
                    // No need to check here: if the current position
                    // (pos) has a key and value, we can move
                    // backward (we're not at the root).
                    page.skiplist_move_cursor_backward(&mut self.stack[p].cursor);

                    let mut right_child = page.right_child(self.stack[p][0]);
                    while right_child != 0 {

                        self.stack.pointer += 1;
                        let p = self.stack.pointer;
                        self.stack[p].page = right_child;

                        let page = self.txn.load_page(right_child);
                        page.skiplist_set_cursor_last(&mut self.stack[p].cursor);
                        right_child = page.right_child(self.stack[p][0])

                    }

                } else {
                    // Else, simply move forward on this page (hint:
                    // we're at a leaf).
                    let p = self.stack.pointer;
                    page.skiplist_move_cursor_backward(&mut self.stack[p].cursor);
                }

                // Return the "current" key (well, current at the beginning of this step).
                Some((cur_key, cur_value))
            }
        }
    }
}



mod put;
pub use put::*;

mod del;
pub use del::*;

const RC_ROOT: usize = 0;


impl<'env> MutTxn<'env> {
    /// Creates a new database, complexity O(1).
    pub fn create_db<K: Representable, V: Representable>(&mut self) -> Result<Db<K, V>, Error> {
        let mut db = try!(self.alloc_page());
        db.init_skiplist_page();
        Ok(Db(db.offset, std::marker::PhantomData))
    }

    /// Sets the specified root to the given value. At most 508 different roots are allowed.
    pub fn set_root<K: Representable, V: Representable>(&mut self, root: usize, value: Db<K, V>) {
        assert!(root <= (PAGE_SIZE as usize - transaction::ZERO_HEADER as usize) >> 3);
        debug!("set_root {:?} {:?}", root, value.0);
        self.set_root_(1 + root, value.0)
    }

    /// Fork a database, in O(log n), where n is the number of pages
    /// with reference count at least 2 (this complexity is also O(log
    /// |db|)).
    pub fn fork<R: Rng, K: Representable, V: Representable>(&mut self,
                                                                  rng: &mut R,
                                                                  db: &Db<K, V>)
                                                                  -> Result<Db<K, V>, Error> {
        debug!("FORK ! {:?}", db.0);
        try!(self.incr_rc(rng, db.0));
        Ok(Db(db.0, std::marker::PhantomData))
    }

    /// Drop a database, in O(n).
    pub fn drop<R: Rng, K: Representable, V: Representable>(&mut self,
                                                            rng: &mut R,
                                                            db: &Db<K, V>)
                                                            -> Result<(), Error> {
        debug!("DROP ! {:?}", db.0);
        // TODO(jneem): understand this. Are the pages used by the Db itself ever freed?
        let page = self.load_cow_page(db.0);
        for (_, k, r) in page.iter_all::<K,V>() {
            if let Some((k,v)) = k {
                unsafe {
                    try!(self.rc_decr_value(rng, k));
                    try!(self.rc_decr_value(rng, v));
                }
            }
            if r > 0 {
                let d = Db::<K,V>(r, std::marker::PhantomData);
                unsafe {
                    try!(self.rc_decr_value(rng, d));
                }
            }
        }
        Ok(())
    }

    /// Allocate one page, and write an iterator onto it. Returns the allocated page.
    fn spread_on_one_page<'a,
                          R: Rng,
                          K: Representable,
                          V: Representable,
                          I: Iterator<Item = (Option<(K, V)>, u64, bool, bool)>>
        (&mut self,
         rng: &mut R,
         it: I)
         -> Result<u64, Error> {
            debug!("spread_on_one_page");
        let mut new_a = try!(self.alloc_page());
            debug!("new_a: {:?}", new_a);
        new_a.init_skiplist_page();
        let mut new_a_cursor = SkipCursor::new();

        // Fill `new_a`.
        for (x, r, incr_r, incr_val) in it {
            debug!("spread_on_one_page: {:?}", new_a.occupied());
            debug!("incr_r: {:?} {:?}", incr_r, r);
            if incr_r && r != 0 {
                try!(self.incr_rc(rng, r));
            }
            if let Some((k, v)) = x {
                // debug!("k = {:?}", std::str::from_utf8(k));
                if incr_val {
                    for offset in k.page_offsets().chain(v.page_offsets()) {
                        try!(self.incr_rc(rng, offset))
                    }
                }
                new_a.skiplist_insert_after_cursor(rng, &mut new_a_cursor, k, v, r)
            } else {
                new_a.set_right_child(new_a_cursor.levels[0], r);
            }
        }
        Ok(new_a.page_offset())
    }

    /// Allocate two pages, and write an iterator onto them. Returns
    /// the two pages and the separator element.
    fn spread_on_two_pages<'a,
                           R: Rng,
                           K: Representable,
                           V: Representable,
                           I: Iterator<Item = (Option<(K, V)>, u64, bool, bool)>>
        (&mut self,
         rng: &mut R,
         mut it: I,

         // Total size occupied by iterator `it`, after all deletions and replacements.
         total_it_size: u16)
         -> Result<(u64, u64, K, V), Error> {

        let mut new_a = try!(self.alloc_page());
        new_a.init_skiplist_page();
        let mut new_b = try!(self.alloc_page());
        new_b.init_skiplist_page();
        let mut new_a_cursor = SkipCursor::new();
        let mut new_b_cursor = SkipCursor::new();

        debug!("spread on two pages");
        let mut middle = None;
        {

            // Fill `new_a`.
            for (x, r, incr_r, incr_val) in &mut it {
                debug!("spread_on_two_pages: A, {:?}", x);
                if incr_r && r != 0 {
                    try!(self.incr_rc(rng, r));
                }

                debug!("Spread A");
                if let Some((k, v)) = x {
                    if incr_val {
                        for offset in k.page_offsets().chain(v.page_offsets()) {
                            try!(self.incr_rc(rng, offset))
                        }
                    }

                    // debug!("SPREAD A {:?}", std::str::from_utf8(k));
                    let rec_size = skiplist::record_size(k, v);
                    debug!("{:?} {:?} {:?}", new_a.occupied(), total_it_size, rec_size);
                    if new_a.occupied() >= (total_it_size - rec_size) / 2 {
                        new_b.set_right_child(skiplist::SKIPLIST_ROOT, r);
                        middle = Some((k, v));
                        break;
                    } else {
                        new_a.skiplist_insert_after_cursor(rng, &mut new_a_cursor, k, v, r)
                    }

                } else {
                    new_a.set_right_child(new_a_cursor.levels[0], r)
                }

            }
            // Fill `new_b`.
            for (x, r, incr_r, incr_val) in &mut it {
                debug!("spread_on_two_pages: A, {:?}", x);
                if incr_r && r != 0 {
                    try!(self.incr_rc(rng, r));
                }
                debug!("Spread B");
                if let Some((k, v)) = x {

                    if incr_val {
                        for offset in k.page_offsets().chain(v.page_offsets()) {
                            try!(self.incr_rc(rng, offset))
                        }
                    }

                    // debug!("SPREAD B {:?}", std::str::from_utf8(k));
                    new_b.skiplist_insert_after_cursor(rng, &mut new_b_cursor, k, v, r)
                } else {
                    new_b.set_right_child(new_b_cursor.levels[0], r)
                }
            }
        }
        let (k, v) = middle.unwrap();
        Ok((new_a.page_offset(), new_b.page_offset(), k, v))
    }

    fn split_root<R: Rng, K: Representable, V: Representable>(&mut self,
                                                                    rng: &mut R,
                                                                    cursor: &mut CursorStack,
                                                                    left: u64,
                                                                    right: u64,
                                                                    key: K,
                                                                    value: V)
                                                                    -> Result<u64, Error> {
        let mut new_root = try!(self.alloc_page());
        new_root.init_skiplist_page();
        cursor.stack[0].page = new_root.offset;
        new_root.set_right_child(skiplist::SKIPLIST_ROOT, left);
        new_root.skiplist_insert_after_cursor(rng,
                                              &mut cursor.current_mut().cursor,
                                              key,
                                              value,
                                              right);
        Ok(new_root.page_offset())
    }

    /// Sets the RC of a page.
    fn set_rc<R: Rng>(&mut self, rng: &mut R, page: u64, rc: u64) -> Result<(), Error> {
        use transaction::LoadPage;

        let mut rc_root: Db<u64, u64> = Db(self.root_(RC_ROOT), std::marker::PhantomData);
        if rc_root.0 == 0 && rc > 1 {
            rc_root = try!(self.create_db())
        };
        debug!("====== BEGIN RC: RC_ROOT = {:?}", rc_root.0);

        if rc_root.0 != 0 {
            try!(self.del(rng, &mut rc_root, page, None));
            if rc > 1 {
                try!(self.put(rng, &mut rc_root, page, rc));
            }
            self.set_root_(RC_ROOT, rc_root.0);
        }
        debug!("====== END RC");
        Ok(())
    }

    /// Stop tracking a page.
    fn remove_rc<R: Rng>(&mut self, rng: &mut R, page: u64) -> Result<(), Error> {
        use transaction::LoadPage;

        let mut rc_root: Db<u64, u64> = Db(self.root_(RC_ROOT), std::marker::PhantomData);
        if rc_root.0 != 0 {
            debug!("====== BEGIN RC: RC_ROOT = {:?}", rc_root.0);

            try!(self.del(rng, &mut rc_root, page, None));
            self.set_root_(RC_ROOT, rc_root.0);
            debug!("====== END RC");
        }
        Ok(())
    }

    /// Increments the reference count of a page. The page must not be free.
    fn incr_rc<R: Rng>(&mut self, rng: &mut R, page: u64) -> Result<(), Error> {
        if page > 0 {
            let rc = std::cmp::max(1, self.rc(page));
            self.set_rc(rng, page, rc + 1)
        } else {
            Ok(())
        }
    }



    unsafe fn rc_decr_value<R:Rng, V: Representable>(&mut self,
                                              rng: &mut R,
                                              v: V)
                                              -> Result<(), Error> {

        for page in v.page_offsets() {
            if page > 0 {
                let rc = self.rc(page);
                debug!("rc_decr_del: {:?} {:?}", page, rc);
                if rc == 1 {
                    try!(self.remove_rc(rng, page));
                }

                if rc <= 1 {
                    try!(v.drop_value(self, rng));
                } else {
                    try!(self.set_rc(rng, page, rc - 1))
                }
            }
        }
        Ok(())
    }




}

/// Trait for operations common to mutable and immutable transactions.
pub trait Transaction: skiplist::SkipList {
    /// Iterate over a database, starting at the first value larger
    /// than or equal to `(key, value)` (and at the smallest key, or
    /// smallest value if one or both of them is `None`).
    fn iter<'a, K: Representable, V: Representable>(&'a self,
                                                    db: Db<K, V>,
                                                    key: Option<(K, Option<V>)>)
                                                    -> Cursor<'a, Self, K, V> {
        let (stack, _) = self.set_cursors(&db, key);
        let mut c = Cursor {
            txn: self,
            stack: stack,
            marker: std::marker::PhantomData,
        };
        // The next element on the top page is larger than or equal to
        // (key, value), and the current one is strictly smaller than
        // (key, value).
        //
        // Since .next() returns the current element, we have to skip
        // the current one.
        //
        // Special case: (key, value) is smaller than or equal to the
        // first element in the database.

        if key.is_some() {

            let p = c.stack.pointer;
            let page = self.load_page(c.stack[p].page);
            let pos = c.stack[p][0];
            c.stack[p][0] = page.next_at_level(pos, 0);
        }
        c
    }

    /// Iterate over a database in the reverse order, starting from the last binding strictly before `k` (or from the last binding in the table if `k.is_none()`).
    fn rev_iter<'a, K: Representable, V: Representable>(&'a self,
                                                        db: Db<K, V>,
                                                        key: Option<(K, Option<V>)>)
                                                        -> RevCursor<'a, Self, K, V> {
        let (stack, _) = self.set_cursors(&db, key);
        let mut c = RevCursor {
            txn: self,
            stack: stack,
            marker: std::marker::PhantomData,
        };

        // If no key was given, start at the end of the database.
        if key.is_none() {
            c.stack.pointer = 1;
            let p = c.stack.pointer;
            let page = self.load_page(c.stack[p].page);
            page.skiplist_set_cursor_last(&mut c.stack[p].cursor);
            let mut right_child = page.right_child(c.stack[p][0]);
            debug!("rev_iter: {:?} {:?}", c.stack[p].page, right_child);

            while right_child != 0 {
                debug!("right_child: {:?}", right_child);
                // Push
                c.stack.pointer += 1;
                let p = c.stack.pointer;
                c.stack[p].page = right_child;

                // Load next page.
                let page = self.load_page(c.stack[p].page);
                page.skiplist_set_cursor_last(&mut c.stack[p].cursor);
                right_child = page.right_child(c.stack[p][0])

            }
        }
        c
    }

    #[doc(hidden)]
    fn set_cursors<K: Representable, V: Representable>(&self,
                                                       root: &Db<K, V>,
                                                       key: Option<(K, Option<V>)>)
                                                       -> (CursorStack, bool) {
        // Set the "cursor stack" by setting a skip list cursor in
        // each page from the root to the appropriate leaf.
        let mut stack = CursorStack::new();
        let present = stack.set(self, &root, key).unwrap();
        (stack, present)
    }

    /// Gets the specified root. At most 508 different roots are allowed.
    fn root<K: Representable, V: Representable>(&self, root: usize) -> Option<Db<K, V>> {
        debug!("root {:?}", root);
        let db = self.root_(1 + root);
        if db > 0 {
            Some(Db(db, std::marker::PhantomData))
        } else {
            None
        }
    }

    /// Get the smallest value associated to `key`, or returns the
    /// given binding if `value` is `Some(..)` and is associated to
    /// `key` in the given database.
    fn get<'a, K: Representable, V: Representable>(&'a self,
                                                   root: &Db<K, V>,
                                                   key: K,
                                                   value: Option<V>)
                                                   -> Option<V> {

        let mut page_off = root.0;
        loop {
            let page = self.load_page(page_off);
            let mut cursor = SkipCursor::new();
            if let Some(val) = self.skiplist_set_cursor(&page, &mut cursor, key, value) {

                return Some(val);

            } else {
                let next_page = page.right_child(cursor.levels[0]);
                if next_page == 0 {
                    // Not found.
                    return None;
                } else {
                    page_off = next_page
                }
            }
        }
    }

    #[doc(hidden)]
    fn rc(&self, page: u64) -> u64 {
        let rc_root = self.root_(RC_ROOT);
        debug!("rc_root = {:?}", rc_root);
        if rc_root == 0 {
            0
        } else {
            let db: Db<u64, u64> = Db(rc_root, std::marker::PhantomData);
            self.get(&db, page, None).unwrap_or(0)
        }
    }
}

impl<'env> Transaction for Txn<'env> {}
impl<'env> Transaction for MutTxn<'env> {}

pub use transaction::Error;

pub mod skiplist;

const PAGE_SIZE: u32 = 4096;
const PAGE_SIZE_U16: u16 = 4096;

#[doc(hidden)]
pub trait PageT: Sized {
    /// offset of the page in the file.
    fn page_offset(&self) -> u64;

    /// pointer to the first word of the page.
    fn data(&self) -> *mut u8;

    fn offset(&self, off: isize) -> *mut u8 {
        unsafe {
            assert!(off < 4096);
            let p: *mut u8 = self.data();
            p.offset(off)
        }
    }
}

#[doc(hidden)]
pub const INDIRECT_BIT: u16 = 0x8000;



impl PageT for MutPage {
    fn page_offset(&self) -> u64 {
        self.offset
    }
    fn data(&self) -> *mut u8 {
        self.data
    }
}

impl PageT for Page {
    fn page_offset(&self) -> u64 {
        self.offset
    }
    fn data(&self) -> *mut u8 {
        self.data as *mut u8
    }
}

impl PageT for Cow {
    fn page_offset(&self) -> u64 {
        match self {
            &Cow::Page(ref p) => p.page_offset(),
            &Cow::MutPage(ref p) => p.page_offset(),
        }
    }
    fn data(&self) -> *mut u8 {
        match self {
            &Cow::Page(ref p) => p.data(),
            &Cow::MutPage(ref p) => p.data(),
        }
    }
}

#[doc(hidden)]
pub const MAX_RECORD_SIZE: u16 = ((PAGE_SIZE as u16 - BINDING_HEADER_SIZE) >> 2);
#[doc(hidden)]
pub const BINDING_HEADER_SIZE: u16 = 16;

#[cfg(test)]
mod tests;

use std::io::{Write, BufWriter};
use std::collections::HashSet;
use std::path::Path;
use std::fs::File;
/// Dumps a number of databases into a dot file.
pub fn debug<P: AsRef<Path>, T: LoadPage + Transaction, K: Representable, V: Representable>
    (t: &T,
     db: &[&Db<K, V>],
     p: P) {
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage + Transaction,K:Representable, V:Representable>(txn: &T,
                                                          pages: &mut HashSet<u64>,
                                                          buf: &mut BufWriter<File>,
                                                          p: &Page,
                                                          print_children: bool) {
        if !pages.contains(&p.offset) {
            pages.insert(p.offset);
            if print_children {

                writeln!(buf,
                         "subgraph cluster{} {{\nlabel=\"Page {}, first_free {}, occupied {}, rc \
                          {}\";\ncolor=black;",
                         p.offset,
                         p.offset,
                         p.first_free(),
                         p.occupied(),
                         txn.rc(p.page_offset()))
                    .unwrap();
            }
            // debug!("print_page: page {:?}", p.offset);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            skiplist::print_skiplist::<T, K, V>(txn, &mut hh, buf, &mut edges, &mut h, p);
            if print_children {
                writeln!(buf, "}}").unwrap();
            }
            for p in edges.iter() {
                writeln!(buf, "{}", p).unwrap()
            }
            if print_children {
                for p in h.iter() {
                    print_page::<T, K, V>(txn, pages, buf, p, true)
                }
            }
        }
    }

    for db in db {
        let page = t.load_page(db.0);
        print_page::<T, K, V>(t, &mut h, &mut buf, &page, true /* print children */);
    }
    writeln!(&mut buf, "}}").unwrap();
}
