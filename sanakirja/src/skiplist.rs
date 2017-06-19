// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use transaction::{MutPage, Page, MutTxn, Txn, LoadPage, Cow};
use {PageT, Representable, MAX_RECORD_SIZE, PAGE_SIZE, BINDING_HEADER_SIZE};
use std::cmp::Ordering;
use std;
use rand::Rng;
pub const NIL: u16 = 0;
const N_LEVELS: usize = 7;
pub const SKIPLIST_ROOT: u16 = 8;
const RIGHT_CHILD_OFFSET_U64: isize = 1;
pub const FIRST_BINDING_SIZE: u16 = 24;

#[derive(Debug, Copy, Clone)]
pub struct SkipCursor {
    pub levels: [u16; N_LEVELS],
    pub started: bool,
}

impl SkipCursor {
    pub fn new() -> Self {
        SkipCursor {
            levels: [SKIPLIST_ROOT; N_LEVELS],
            started: false,
        }
    }
    pub fn reset(&mut self) {
        for i in self.levels.iter_mut() {
            *i = SKIPLIST_ROOT
        }
        self.started = false
    }
}

impl<'env, T> MutTxn<'env, T> {
    #[doc(hidden)]
    pub fn skiplist_insert<R: Rng, K: Representable, V: Representable>
        (&mut self,
         r: &mut R,
         page: &mut MutPage,
         key: K,
         value: V,
         right_child: u64)
         -> Result<bool, super::Error> {
        // Now, insert it in the list: set the cursor to the position
        // just before where (key, value) should be.
        let mut cursor = SkipCursor::new();
        if self.skiplist_set_cursor(page, &mut cursor, key, Some(value)).is_some() {
            return Ok(false);
        }
        page.skiplist_insert_after_cursor(r, &mut cursor, key, value, right_child);
        Ok(true)
    }
}

pub trait SkipList: LoadPage + Sized {
    /// Sets the cursor to the last position strictly smaller than
    /// (key, value). If the key (and possibly value) was found, the
    /// corresponding value is returned.
    fn skiplist_set_cursor<P: SkipListPage, K: Representable, V: Representable>
        (&self,
         page: &P,
         curs: &mut SkipCursor,
         key: K,
         value: Option<V>)
         -> Option<V> {
        let mut level = N_LEVELS - 1;
        loop {
            let result =
                self.skiplist_set_cursor_at_level(page, &mut curs.levels[level], level, key, value);
            if level == 0 {
                return result;
            }
            level -= 1;
            curs.levels[level] = curs.levels[level + 1]
        }
    }

    /// Moves the cursor at level `level`, until the final position is
    /// either immediately before NIL, or the largest position
    /// strictly smaller than (key, value). If (key, value) was found,
    /// it is returned.
    fn skiplist_set_cursor_at_level<P: SkipListPage, K: Representable, V: Representable>
        (&self,
         page: &P,
         position: &mut u16,
         level: usize,
         key: K,
         value: Option<V>)
         -> Option<V> {
        loop {
            debug!("set_cursor_at_level page = {:?}, position = {:?} level = {:?}",
                   page.page_offset(),
                   *position,
                   level);
            let next_position = page.next_at_level(*position, level);
            debug!("next_position: {:?}", next_position);
            if next_position == 0 {
                return None;
            }
            debug!("set_cursor_at_level {:?}", next_position);
            let (k, v) = unsafe { page.read_key_value::<K, V>(next_position) };

            match unsafe { key.cmp_value(self, k) } {
                Ordering::Less => return None,
                Ordering::Greater => {
                    debug!("greater");
                    debug_assert!(*position != next_position);
                    *position = next_position
                }
                Ordering::Equal => unsafe {
                    if let Some(value) = value {
                        match value.cmp_value(self, v) {
                            Ordering::Equal => return Some(v),
                            Ordering::Less => return None,
                            Ordering::Greater => *position = next_position,
                        }
                    } else {
                        return Some(v);
                    }
                },
            }
        }
    }
}

unsafe fn set_level(p: *mut u8, level: usize, value: u16) {
    debug!("set_level {:?} {:?}", level, value);
    debug_assert!(level < N_LEVELS);
    debug_assert_eq!(value & 7, 0); // Check that value is multiple of 8.
    debug_assert_eq!((p as usize) & 7, 0); // Check that the conversion to *mut u64 is valid.
    let p = p as *mut u64;
    let mut levels = u64::from_le(*p);

    // All values are 8-bytes aligned, we don't need to store the 3 extra 0s.
    let value = (value >> 3) as u64;

    // Set the value at that level to 0.
    levels &= !(0x1ff << (9 * level));

    // Replace with current value.
    levels |= value << (9 * level);

    debug!("levels = {:?}", levels);
    *p = levels.to_le()
}


impl MutPage {
    /// This function panics if there's enough space on the page.
    pub fn skiplist_insert_after_cursor<R: Rng, K: Representable, V: Representable>(&mut self,
                                                r: &mut R,
                                                cursor: &mut SkipCursor,
                                                key: K,
                                                value: V,
                                                right_child: u64) {
        debug!("insert_after_cursor: {:?} {:?}", self, cursor);
        // First allocate the key and value.
        let size = record_size(key, value);
        debug!("record_size: {:?}", size);
        let off = self.can_alloc_size(size);
        if off == 0 {
            panic!("size = {:?}, occupied {:?}", size, self.occupied())
        }
        debug_assert_ne!(off, 0);

        self.reset_pointers(off);
        self.alloc_key_value(off, size, key, value);
        self.set_right_child(off, right_child);

        // Now insert in the base list.
        for l in 0..N_LEVELS {
            debug!("next: {:?}", self.next_at_level(cursor.levels[l], l));
            unsafe {
                debug_assert_ne!(off, self.next_at_level(cursor.levels[l], l));
                debug_assert_ne!(off, cursor.levels[l]);
                set_level(self.offset(off as isize),
                          l,
                          self.next_at_level(cursor.levels[l], l));
                set_level(self.offset(cursor.levels[l] as isize), l, off);
            }
            cursor.levels[l] = off;
            if r.gen() {
                break;
            }
        }
    }



    /// Deletes the element immediately after the cursor.
    pub fn skiplist_cursor_delete_next<K: Representable, V: Representable>(&mut self,
                                                                           cursor: &SkipCursor) {

        let next_0 = self.next_at_level(cursor.levels[0], 0);

        let size = {
            let (k, v) = unsafe { self.read_key_value::<K, V>(next_0) };
            record_size(k, v)
        };

        let next_next_0 = self.next_at_level(next_0, 0);
        debug!("next: {:?} -> {:?}", next_0, next_next_0);

        unsafe {
            set_level(self.offset(cursor.levels[0] as isize), 0, next_next_0);
        }


        let occupied = self.occupied();
        self.set_occupied(occupied - size);

        // The reasoning here is that cursor contains the pointers to
        // the next element for all levels in which that element is,
        // because that's how we looked the next element up.
        for l in 1..N_LEVELS {

            let next = self.next_at_level(cursor.levels[l], l);
            if next == next_0 {
                let next_next = self.next_at_level(next, l);
                debug!("next {:?}, next_0 {:?}, next_next {:?}",
                       next,
                       next_0,
                       next_next);
                unsafe {
                    debug_assert_ne!(cursor.levels[l], next_next);
                    set_level(self.offset(cursor.levels[l] as isize), l, next_next);
                }
            } else {
                break;
            }
        }
    }

    fn reset_pointers(&mut self, offset: u16) {
        // Initialize the list pointers.
        unsafe {
            debug!("reset_pointers {:?}", self.data().offset(offset as isize));
            let p:*mut u8 = self.data().offset(offset as isize);
            std::ptr::write_bytes(p, 0, 8);
        }
    }
    pub fn set_right_child(&mut self, off: u16, r: u64) {
        if off >= 4096 {
            panic!("{} < 4096", off);
        }
        unsafe {
            *((self.offset(off as isize) as *mut u64).offset(RIGHT_CHILD_OFFSET_U64)) = r.to_le()
        }
    }

    /// allocate and write key, value, left and right neighbors.
    fn alloc_key_value<K: Representable, V: Representable>(&mut self,
                                                           off_ptr: u16,
                                                           size: u16,
                                                           key: K,
                                                           value: V) {
        debug!("alloc_key_value off {:?} {:?}", off_ptr, size);
        debug_assert!(size > 0);
        let occupied = self.occupied();
        self.set_occupied(occupied + size);
        let first_free = self.first_free();
        self.set_first_free(first_free + size);

        assert!(off_ptr < 4096);
        let key_ptr = self.offset(off_ptr as isize + BINDING_HEADER_SIZE as isize);
        unsafe {
            key.write_value(key_ptr);
        }

        let value_ptr = unsafe { K::skip(key_ptr) };

        let va = V::alignment() as u16;
        let mask = va - 1;
        let value_padding = (va - ((value_ptr as usize as u16) & mask)) & mask;
        unsafe {
            debug!("value_ptr {:?}", value_ptr.offset(value_padding as isize));
            value.write_value(value_ptr.offset(value_padding as isize));
        }
    }
}


pub trait SkipListPage: PageT + Sized {
    /// 0 if cannot alloc, valid offset else (offset in bytes from the
    /// start of the page)
    // First free spot in this page (head of the linked list, number
    // of |u32| from the last glue.
    fn first_free(&self) -> u16 {
        unsafe { *((self.data() as *mut u16).offset(2)) }
    }
    fn occupied(&self) -> u16 {
        unsafe { *((self.data() as *mut u16).offset(3)) }
    }

    fn set_first_free(&mut self, x: u16) {
        debug!("set_first_free");
        unsafe {
            let p = self.data().offset(4) as *mut u16;
            *p = x.to_le()
        }
    }

    fn set_occupied(&mut self, x: u16) {
        debug!("set_occupied");
        unsafe {
            let p = self.data().offset(6) as *mut u16;
            *p = x.to_le()
        }
    }

    fn right_child(&self, off: u16) -> u64 {
        if off >= 4096 {
            panic!("{} < 4096", off);
        }
        unsafe {
            u64::from_le(*((self.offset(off as isize) as *const u64)
                .offset(RIGHT_CHILD_OFFSET_U64)))
        }
    }

    fn can_alloc_size(&self, size: u16) -> u16 {
        debug_assert!(size & 7 == 0); // 64 bits aligned.
        debug!("self.occupied = {:?}", self.occupied());
        if self.occupied() + size <= PAGE_SIZE as u16 {
            self.first_free()
        } else {
            0
        }
    }

    fn can_alloc<K: Representable, V: Representable>(&self, key: K, value: V) -> u16 {
        let size = record_size(key, value);
        self.can_alloc_size(size)
    }

    // All bindings are 8-bytes aligned. There are 7 layers, each
    // encoding 9 bits, which are then multiplied by 8 to get the
    // position on the page.
    fn next_at_level(&self, off: u16, level: usize) -> u16 {
        unsafe {
            debug_assert!(level < N_LEVELS);
            let levels = u64::from_le(*(self.offset(off as isize) as *const u64));
            (((levels >> (9 * level)) & 0x1ff) as u16) << 3
        }
    }

    /// Moves the cursor by one step forward, and returns whether this
    /// was possible.
    fn skiplist_move_cursor_forward(&self, cursor: &mut SkipCursor) -> bool {
        // One tricky point is, we might have to move upper levels if
        // the lower levels were moved past the next entry at the
        // upper levels.
        let next = self.next_at_level(cursor.levels[0], 0);
        debug!("move_forward, next: {:?}", next);
        if next == NIL {
            return false;
        }
        cursor.levels[0] = next;
        for l in 1..N_LEVELS {
            let next = self.next_at_level(cursor.levels[l], l);
            if cursor.levels[l - 1] == next {
                // If the lower level was moved past this level's next
                // entry, move this level's next entry.
                cursor.levels[l] = next
            } else {
                break;
            }
        }
        true
    }

    /// Moves the cursor by one step backwards. Returns a boolean
    /// indicating whether the cursor was moved.
    fn skiplist_move_cursor_backward(&self, cursor: &mut SkipCursor) -> bool {
        debug!("move_cursor_backward: cursor: {:?}", cursor);
        if cursor.levels[0] == SKIPLIST_ROOT {
            return false;
        }
        let until = cursor.levels[0];

        let mut l = 1;
        while l < N_LEVELS && cursor.levels[l] == cursor.levels[0] {
            l += 1
        }
        if l >= N_LEVELS {
            // This cursor is set at an entry present in all the lists.
            // Start again from the top list, at the beginning of the page.
            l = N_LEVELS - 1;
            cursor.levels[l] = SKIPLIST_ROOT;
        } else {
            l -= 1;
            cursor.levels[l] = cursor.levels[l + 1];
        }

        loop {
            assert!(cursor.levels[l] != until);
            loop {
                let next = self.next_at_level(cursor.levels[l], l);
                debug_assert!(next != 0);
                if next == until {
                    break;
                } else {
                    cursor.levels[l] = next
                }
            }

            if l == 0 {
                break;
            }
            l -= 1;
            cursor.levels[l] = cursor.levels[l + 1]
        }
        true
    }

    /// Creates an iterator starting at the element immediately after `position`.
    fn iter<K: Representable, V: Representable>(&self, position: u16) -> Iter<Self, K, V> {
        Iter {
            page: self,
            position: position,
            marker: std::marker::PhantomData,
        }
    }

    /// Creates an iterator starting at the skiplist root.
    fn iter_all<K: Representable, V: Representable>(&self) -> Iter<Self, K, V> {
        Iter {
            page: self,
            position: SKIPLIST_ROOT,
            marker: std::marker::PhantomData,
        }
    }

    /// Creates an iterator starting at the element at `position`.
    fn rev_iter_all<K: Representable, V: Representable>(&self) -> ReverseIter<Self, K, V> {
        let mut iter = ReverseIter {
            page: self,
            cursor: SkipCursor::new(),
            marker: std::marker::PhantomData,
        };
        self.skiplist_set_cursor_last(&mut iter.cursor);
        iter
    }

    /// Sets the cursor to the last position strictly smaller than
    /// (key, value). If the key (and possibly value) was found, the
    /// corresponding value is returned.
    fn skiplist_set_cursor_last(&self, curs: &mut SkipCursor) {
        let mut level = N_LEVELS - 1;
        curs.levels[level] = SKIPLIST_ROOT;
        loop {
            loop {
                let next = self.next_at_level(curs.levels[level], level);
                if next == NIL {
                    break
                } else {
                    curs.levels[level] = next
                }
            }
            if level == 0 {
                break
            }
            level -= 1;
            curs.levels[level] = curs.levels[level + 1]
        }
    }

    /// Sets the cursor to the last position strictly smaller than
    /// (key, value). If the key (and possibly value) was found, the
    /// corresponding value is returned.
    fn skiplist_set_cursor_root(&self, curs: &mut SkipCursor) {
        for st in curs.levels.iter_mut() {
            *st = SKIPLIST_ROOT
        }
    }

    unsafe fn key_ptr(&self, off: u16) -> *mut u8 {
        debug!("key_ptr: off {:?}", off);
        debug_assert!(off >= FIRST_BINDING_SIZE);
        self.offset(off as isize + BINDING_HEADER_SIZE as isize)
    }


    unsafe fn read_key_value<K: Representable, V: Representable>(&self, off: u16) -> (K, V) {
        assert!(off < 4096);
        let key_ptr = self.offset(off as isize + BINDING_HEADER_SIZE as isize);
        let value_ptr = K::skip(key_ptr);

        let va = V::alignment() as u16;
        let mask = va - 1;
        let value_padding = (va - ((value_ptr as usize) & (mask as usize)) as u16) & mask;

        // debug!("read_key_value {:?} {:?} {:?}", key_ptr, value_ptr, value_padding);
        (K::read_value(key_ptr),
         V::read_value(value_ptr.offset(value_padding as isize)))
    }
}

impl MutPage {
    pub fn init_skiplist_page(&mut self) {
        debug!("init_skiplist_page");
        self.reset_pointers(SKIPLIST_ROOT);
        // Initialize the page metadata.
        debug!("init: set_first_free");
        self.set_first_free(FIRST_BINDING_SIZE); // first free spot.
        debug!("init: set_occupied");
        self.set_occupied(FIRST_BINDING_SIZE); // occupied size on page.
        debug!("init: right_child");
        self.set_right_child(SKIPLIST_ROOT, 0);
    }
}

impl<'env, T> SkipList for MutTxn<'env, T> {}
impl<'env> SkipList for Txn<'env> {}

impl SkipListPage for MutPage {}
impl SkipListPage for Page {}
impl SkipListPage for Cow {}

pub fn record_size<K: Representable, V: Representable>(key: K, value: V) -> u16 {
    // debug!("key: {:?}", key);

    let kl = key.onpage_size();

    let vl = value.onpage_size();
    let va = V::alignment() as u16;

    // Padding before value.
    let mask = va - 1;
    let vp = (va - (kl & mask)) & mask;

    debug!("record_size, onpage: {:?} {:?} {:?}", kl, vl, vp);
    let size_onpage = BINDING_HEADER_SIZE + kl + vl + vp;
    assert!(size_onpage <= MAX_RECORD_SIZE);
    // keep the pointers 8-bytes aligned.
    let size = size_onpage + ((8 - (size_onpage & 7)) & 7);
    debug!("record_size {:?}", size);
    debug_assert!(size >= BINDING_HEADER_SIZE);
    size
}


#[derive(Clone, Copy)]
pub struct Iter<'a, P: 'a, K, V> {
    page: &'a P,
    position: u16,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, P: SkipListPage, K: Representable, V: Representable> Iterator for Iter<'a, P, K, V> {
    type Item = (*const u8, Option<(K, V)>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == NIL {
            None
        } else {
            debug!("self.position: {:?} {:?}",
                   self.page.page_offset(),
                   self.position);
            let pos = self.position;
            let r = self.page.right_child(pos);
            self.position = self.page.next_at_level(pos, 0);
            debug!("next position: {:?}", self.position);
            if pos == SKIPLIST_ROOT {
                Some((self.page.offset(pos as isize), None, r))
            } else {
                let (k, v) = unsafe { self.page.read_key_value(pos) };
                Some((self.page.offset(pos as isize), Some((k, v)), r))
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct ReverseIter<'a, P: 'a, K, V> {
    page: &'a P,
    cursor: SkipCursor,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, P: SkipListPage, K: Representable, V: Representable> Iterator for ReverseIter<'a, P, K, V> {
    type Item = (*const u8, Option<(K, V)>, u64);

    fn next(&mut self) -> Option<Self::Item> {

        if self.cursor.levels[0] == NIL {
            None
        } else {
            let pos = self.cursor.levels[0];
            let kv:Option<(K, V)> = if pos == SKIPLIST_ROOT {
                Some(unsafe { self.page.read_key_value(pos) })
            } else {
                None
            };
            let r = self.page.right_child(pos);

            // If we cannot move backward, mark the iterator finished
            // (i.e. set the cursor's lowest level to NIL).
            if ! self.page.skiplist_move_cursor_backward(&mut self.cursor) {
                self.cursor.levels[0] = NIL;
            }
            Some((self.page.offset(pos as isize), kv, r))
        }
    }
}








#[test]
fn test_iterate() {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use value::UnsafeValue;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    use rustc_serialize::hex::ToHex;

    let mut rng = rand::thread_rng();
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();


    let mut p = txn.alloc_page().unwrap();
    p.init_skiplist_page();
    unsafe {
        let test = std::slice::from_raw_parts(p.data, 10);
        debug!("test = {:?}", test.to_hex());
    }
    let n = 10;
    for _ in 0..n {
        let k0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();
        let v0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();


        let key = UnsafeValue::from_slice(k0.as_bytes());
        let value = UnsafeValue::from_slice(v0.as_bytes());
        txn.skiplist_insert(&mut rng, &mut p, key, value, 0).unwrap();
    }

    // Is the iterator sorted?
    let mut x: Vec<_> = Vec::new();
    for (_, pp, _) in p.iter::<UnsafeValue, UnsafeValue>(SKIPLIST_ROOT) {
        if let Some((k, v)) = pp {
            x.push((k, v))
        }
    }
    let mut i = 0;
    while i < x.len() - 1 {
        let x0 = unsafe { x[i].0.as_slice() };
        let x1 = unsafe { x[i + 1].0.as_slice() };
        assert!(x0 <= x1);
        i += 1
    }

    txn.commit().unwrap()
}



#[test]
fn test_skiplist_del() {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use value::UnsafeValue;
    extern crate env_logger;
    env_logger::init().unwrap_or(());

    use rustc_serialize::hex::ToHex;

    let mut rng = rand::thread_rng();
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();


    let mut p = txn.alloc_page().unwrap();
    p.init_skiplist_page();
    unsafe {
        let test = std::slice::from_raw_parts(p.data, 10);
        debug!("test = {:?}", test.to_hex());
    }
    let n = 10;
    let mut v = Vec::new();
    for _ in 0..n {
        let k0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();
        let v0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();

        let key = UnsafeValue::from_slice(k0.as_bytes());
        let value = UnsafeValue::from_slice(v0.as_bytes());
        txn.skiplist_insert(&mut rng, &mut p, key, value, 0).unwrap();

        v.push((k0, v0));
    }

    let (ref k0, ref v0) = v[n / 2];
    let key = UnsafeValue::from_slice(k0.as_bytes());
    let value = UnsafeValue::from_slice(v0.as_bytes());
    {
        let mut cursor = SkipCursor::new();
        assert!(txn.skiplist_set_cursor(&p, &mut cursor, key, Some(value)).is_some());
        p.skiplist_cursor_delete_next::<UnsafeValue, UnsafeValue>(&mut cursor);
    }
    let mut cursor = SkipCursor::new();
    assert!(txn.skiplist_set_cursor(&p, &mut cursor, key, Some(value)).is_none());

    let mut m = 0;
    for (_, pp, _) in p.iter::<UnsafeValue, UnsafeValue>(SKIPLIST_ROOT) {
        debug!("{:?}", pp);
        if pp.is_some() {
            m += 1;
        }
    }
    assert_eq!(m, n - 1);
    txn.commit().unwrap()
}



#[test]
fn test_move_cursor() {
    extern crate tempdir;
    extern crate rand;
    use transaction::{Env, Commit};
    use rand::Rng;
    use value;
    extern crate env_logger;
    env_logger::init().unwrap_or(());
    use rustc_serialize::hex::ToHex;
    let mut rng = rand::thread_rng();
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 40960).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();


    let mut p = txn.alloc_page().unwrap();
    p.init_skiplist_page();
    unsafe {
        debug!("{:?}", p.data);
        let test = std::slice::from_raw_parts(p.data, 30);
        debug!("test = {:?}", test.to_hex());
    }

    let n = 10;

    for _ in 0..n {
        let k0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();
        let v0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect();


        let key = value::UnsafeValue::from_slice(k0.as_bytes());
        let value = value::UnsafeValue::from_slice(v0.as_bytes());
        txn.skiplist_insert(&mut rng, &mut p, key, value, 0).unwrap();
        unsafe {
            let test = std::slice::from_raw_parts(p.data, 4096);
            let mut i = 0;
            while i < 100 {
                print!("{:?} ", &test[i..i + 8].to_hex());
                i += 8;
            }
            print!("\n")
        }
    }

    let mut cursor = SkipCursor::new();
    let mut i = 0;
    for _ in 0..10000 {
        debug!("i = {:?}", i);
        let forward = rng.gen();
        debug!("cursor = {:?}, moving forward: {:?}", cursor, forward);
        if forward {
            let m = p.skiplist_move_cursor_forward(&mut cursor);
            if i >= n { assert!(!m) } else { i += 1 }
        } else {
            let m = p.skiplist_move_cursor_backward(&mut cursor);
            if i == 0 { assert!(!m) } else { i -= 1 }
        }
    }

    txn.commit().unwrap()
}


use std::collections::HashSet;
use std::io::Write;

pub fn print_skiplist<T: LoadPage, K: Representable, V: Representable>(txn: &T,
                                                                       nodes: &mut HashSet<u16>,
                                                                       buf: &mut Write,
                                                                       edges: &mut Vec<String>,
                                                                       pages: &mut Vec<Page>,
                                                                       p: &Page) {
    let mut off = SKIPLIST_ROOT;
    while off != 0 {
        // debug!("print tree:{:?}, off={:?}", p, off);
        // let ptr = p.offset(off as isize) as *const u32;
        let (key, value) = {
            if off == SKIPLIST_ROOT {
                ("root".to_string(), "".to_string())
            } else {
                let (key, value) = unsafe { p.read_key_value::<K, V>(off) };
                (format!("{:?}", key), format!("{:?}", value))
            }
        };
        // debug!("key,value={:?},{:?}",key,value);
        writeln!(buf,
                 "n_{}_{}[label=\"{}: '{}'->'{}'\"];",
                 p.offset,
                 off,
                 off,
                 key,
                 value)
            .unwrap();
        if !nodes.contains(&off) {
            let next_page = p.right_child(off);
            if next_page > 0 {
                // debug!("print_tree, page = {:?}, next_page = {:?}", p.offset, next_page);
                pages.push(txn.load_page(next_page));
                edges.push(format!("n_{}_{}->n_{}_{}[color=\"red\"];",
                                   p.offset,
                                   off,
                                   next_page,
                                   SKIPLIST_ROOT))
            };
            nodes.insert(off);
            for i in 0..N_LEVELS {
                let next = p.next_at_level(off, i);
                // debug!("{:?}",((ptr as *const u16).offset(i)));
                if next != 0 {
                    writeln!(buf,
                             "n_{}_{}->n_{}_{}[color=\"blue\", label=\"{}\"];",
                             p.offset,
                             off,
                             p.offset,
                             next,
                             i)
                        .unwrap();
                }
            }
            off = p.next_at_level(off, 0)
        }
        // debug!("/print tree:{:?}",p);
    }
}
