// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use {MutTxn, Transaction, Representable, PageCursor, CursorStack, Db, Error, PageT,
     BINDING_HEADER_SIZE, PAGE_SIZE_U16, MAX_RECORD_SIZE};

use skiplist::{SkipListPage, NIL, SKIPLIST_ROOT, FIRST_BINDING_SIZE, record_size};
use transaction::{Cow, MutPage};

use std;
use rand::Rng;

#[derive(Debug)]
enum Deleted<K, V> {
    Merged {
        page: u64,
        left_free: u64,
        right_free: u64,
    },
    Rebalanced {
        key: K,
        value: V,
        left: u64,
        right: u64,
        left_free: u64,
        right_free: u64,
    },
    Split {
        after: *const u8, // key whose right child has split.
        key: K,
        value: V,
        left: u64,
        right: u64,
        free: u64,
    },
    None {
        /// the page on which the last operation (possibly a deletion) was performed.
        page: u64,
        /// the page that should replace it, if from another transaction.
        replaced_by: u64,
    },
}

#[derive(Debug)]
struct Delete<K, V> {
    cursor: CursorStack,

    /// This is a pointer to the key in the page below the one pointed
    /// to by `cursor_pointer`. It is never dereferenced, and is used
    /// only to recognize the key that should be deleted.
    forgotten_ptr: *const u8,

    /// The record length corresponding to that key, in order to
    /// compute merges and rotations.
    forgotten_len: u16,

    /// The last merge or rotation performed on the page below, or
    /// `Deleted::None` if it was just a deletion or update.
    last_operation: Deleted<K, V>,

    /// Whenever we are deleting a binding in an internal node, we
    /// need to copy a binding from a leaf to the node. This means
    /// that the leaf must not be freed until the copy is finished.
    protected_leaf: u64,
    protected_leaf_was_freed: bool,

    /// First page on the stack for which the reference count is at least 2.
    rc_level: usize,
}

impl<K, V> std::ops::Index<usize> for Delete<K, V> {
    type Output = PageCursor;
    fn index(&self, i: usize) -> &PageCursor {
        self.cursor.stack.index(i)
    }
}

impl<K, V> std::ops::IndexMut<usize> for Delete<K, V> {
    fn index_mut(&mut self, i: usize) -> &mut PageCursor {
        self.cursor.stack.index_mut(i)
    }
}

impl<K, V> Delete<K, V> {
    fn new(cursors: CursorStack) -> Self {
        Delete {
            cursor: cursors,
            forgotten_ptr: std::ptr::null(),
            forgotten_len: 0,
            last_operation: Deleted::None {
                page: 0,
                replaced_by: 0,
            },
            protected_leaf: 0,
            protected_leaf_was_freed: false,
            rc_level: 200, // anything larger than 64 would do.
        }
    }
    fn current(&mut self) -> &mut PageCursor {
        &mut self.cursor.stack[self.cursor.pointer]
    }
}

#[derive(Copy, Clone, Debug)]
struct Replacement<K, V> {
    k: K,
    v: V,
    deleted_ptr: *const u8,
    deleted_len: u16,
}

impl<'env, T> MutTxn<'env, T> {
    /// Find the smallest right descendant of the element at the given cursor.
    fn find_smallest_descendant<K, V>(&mut self, del: &mut Delete<K, V>) {
        // We need to look for the smallest element in the right
        // subtree (if there's a right subtree).
        let mut page = self.load_cow_page(del.current().page);

        // Position of the deleted element.
        let next = page.next_at_level(del.current()[0], 0);
        // Right child of the deleted element.
        let right_page = page.right_child(next);
        if right_page > 0 {
            // If there's a right child, take its smallest element.
            del.cursor.pointer += 1;
            del.current().page = right_page;
            del.current().cursor.reset();
            loop {
                page = self.load_cow_page(del.current().page);

                if del.cursor.first_rc_level >= super::N_CURSORS &&
                   self.rc(del[del.cursor.pointer].page) >= 2 {
                    debug!("first_rc_level: {:?} {:?}",
                           del.cursor.pointer,
                           del.cursor.stack[del.cursor.pointer].page);
                    del.cursor.first_rc_level = del.cursor.pointer
                }

                let right_page = page.right_child(SKIPLIST_ROOT);
                if right_page == 0 {
                    break;
                } else {
                    del.cursor.pointer += 1;
                    del.current().page = right_page;
                    del.current().cursor.reset()
                }
            }
        }
    }

    /// If `value` is `None`, delete one of the bindings associated to
    /// `key` from the database (without any specific
    /// preference). Else, delete the specified binding if present.
    pub fn del<R: Rng, K: Representable, V: Representable>(&mut self,
                                                           rng: &mut R,
                                                           db: &mut Db<K, V>,
                                                           key: K,
                                                           value: Option<V>)
                                                           -> Result<bool, Error> {

        // debug!("del key: {:?}", &key[..std::cmp::min(8, key.len())]);
        // debug!("del key: {:?}", std::str::from_utf8(&key[..std::cmp::min(8, key.len())]));
        // let unsafe_value = value.map(|v| UnsafeValue::from_slice(v));
        let mut del = {
            let (stack, found) = self.set_cursors(db, Some((key, value)));
            if !found {
                // debug!("element not present: {:?}", key);
                return Ok(false);
            }
            Delete::new(stack)
        };

        let cursor_pointer0 = del.cursor.pointer;

        self.find_smallest_descendant(&mut del);

        // Load the page of the smallest descendant of the deleted entry.
        let page = self.load_cow_page(del.current().page);

        debug!("cursor_pointer: {:?} {:?}",
               del.cursor.pointer,
               cursor_pointer0);

        // Now, two cases:
        if del.cursor.pointer == cursor_pointer0 {
            // We are deleting at a leaf (which might also be the
            // root).
            let next = page.next_at_level(del.current()[0], 0);
            let (key, value) = unsafe { page.read_key_value::<K, V>(next) };
            // debug!("leaf delete {:?}", value);
            del.forgotten_ptr = page.offset(next as isize);
            del.forgotten_len = record_size(key, value);
            del.cursor.pointer -= 1;

            if cursor_pointer0 < del.cursor.first_rc_level {
                // This page will be copied, and hence will increase
                // the RC of the replacement value, if indirect.
                unsafe {
                    try!(self.rc_decr_value(rng, key));
                    try!(self.rc_decr_value(rng, value))
                }
            }
            // Update (i.e. merge, rebalance or update) the pages
            // above (if any).
            while del.cursor.pointer >= 1 {
                try!(self.merge_or_rebalance(rng, &mut del, None));
                del.cursor.pointer -= 1;
            }
            try!(self.update_root(rng, db, &mut del, None));
        } else {
            // We're deleting at an internal node, at the position
            // pointed to by cursor_pointer0.  We want to replace the
            // deleted entry with the smallest right descendant of
            // that entry, i.e. the element pointed to by
            // del.cursor_pointer.

            // Load the replacement.
            let first = page.next_at_level(SKIPLIST_ROOT, 0);
            let (key, value) = unsafe { page.read_key_value::<K, V>(first) };
            // debug!("Internal, replacement {:?}", std::str::from_utf8(key));

            // Delete<K,V> the replacement on the leaf (actually, schedule
            // it for deletion from a page above).
            del.forgotten_ptr = page.offset(first as isize);
            del.forgotten_len = record_size(key, value);
            // Protect this page so that merges and rotations do not
            // free it even if needed.
            del.protected_leaf = page.page_offset();


            if del.cursor.pointer >= del.cursor.first_rc_level {
                // This page will be copied, and hence will increase
                // the RC of the replacement value, if indirect.
                for offset in key.page_offsets() {
                    try!(self.incr_rc(rng, offset))
                }
                for offset in value.page_offsets() {
                    try!(self.incr_rc(rng, offset))
                }
            }

            // merge_or_rebalance everything from the leaf to
            // cursor_pointer0.
            del.cursor.pointer -= 1;

            let repl = {
                // k0, v0: deleted key/value.
                let page0 = self.load_cow_page(del[cursor_pointer0].page);
                debug!("reading at {:?}", del[cursor_pointer0][0]);
                let ptr0 = page0.next_at_level(del[cursor_pointer0][0], 0);
                let (k0, v0) = {
                    unsafe { page0.read_key_value::<K, V>(ptr0) }
                };
                if cursor_pointer0 < del.cursor.first_rc_level {
                    // This page will be copied, and hence will increase
                    // the RC of the replacement value, if indirect.
                    unsafe {
                        try!(self.rc_decr_value(rng, k0));
                        try!(self.rc_decr_value(rng, v0))
                    }
                }
                // debug!("deleting {:?}", std::str::from_utf8(k0));
                Replacement {
                    k: key,
                    v: value,
                    deleted_ptr: page0.offset(ptr0 as isize),
                    deleted_len: record_size(k0, v0),
                }
            };
            debug!("Internal, updating {:?}", &del.cursor.stack[..del.cursor.pointer + 1]);
            while del.cursor.pointer >= 1 {
                debug!("LOOP, del.cursor.pointer = {:?}", del.cursor.pointer);
                try!(self.merge_or_rebalance(rng, &mut del, Some(repl)));
                del.cursor.pointer -= 1;
            }
            debug!("Internal, done updating");
            try!(self.update_root(rng, db, &mut del, Some(repl)));
        }
        // debug!("last {:?}", del.last_operation);
        debug!("stack {:?}", del.cursor.stack[1]);

        // Now change the root if split or merged.
        match del.last_operation {
            Deleted::Split { key, value, left, right, free, .. } => {
                db.0 = try!(self.split_root(rng, &mut del.cursor, left, right, key, value));
                debug!("Split {:?}", db.0);
                unsafe { try!(self.rc_decr_del(rng, &mut del, free)) }
            }
            Deleted::None { page, replaced_by } => {
                if page != 0 {
                    unsafe { try!(self.rc_decr_del(rng, &mut del, page)) }
                }
                db.0 = replaced_by
            }
            _ => {}
        }

        if del.protected_leaf_was_freed {
            unsafe { self.free_page(del.protected_leaf) }
        }
        Ok(true)
    }

    /// From a cursor, a (possibly null) pointer to a key which needs
    /// to be forgotten, a pointer to the last operation
    /// (merge/rebalance/nothing), decide what to do (merge, rebalance
    /// or nothing) at the point indicated by `cursor`, and prepare the
    /// children in consequence.
    ///
    /// This function does not update the page pointed to by the
    /// cursor, because these operations might cascade, and the update
    /// will be done by another call to this function.
    fn merge_or_rebalance<R: Rng, K: Representable, V: Representable>
        (&mut self,
         rng: &mut R,
         del: &mut Delete<K, V>,
         replacement: Option<Replacement<K, V>>)
         -> Result<(), Error> {

        // In order to update the page pointers appropriately, record
        // whether the cursor was moved backwards (for instance if
        // we're at the end of a page and need to merge).
        let mut cursor_was_moved_backwards = false;

        let page = self.load_cow_page(del.current().page);

        // If there is something to delete on the pages below the current one.
        if !del.forgotten_ptr.is_null() {

            // If a key in the page below needs to be deleted, try to
            // merge or rebalance the pages below as needed, and
            // return.

            let mut next = page.next_at_level(del.current()[0], 0);
            if next == NIL {
                // If the current element is the last element in the
                // skiplist, move one element backwards.
                cursor_was_moved_backwards = true;
                debug!("moving backwards in page {:?}", page);
                let backwards = page.skiplist_move_cursor_backward(&mut del.current().cursor);
                assert!(backwards);
                next = page.next_at_level(del.current()[0], 0)
            }

            debug!("page = {:?}", page);
            let page_a = page.right_child(del.current()[0]);
            let page_a = self.load_cow_page(page_a);
            let page_b = page.right_child(next);
            let page_b = self.load_cow_page(page_b);


            // build the iterator to evaluate its size.
            let next_ptr: *const u8 = page.offset(next as isize);
            let (middle_key, middle_value) = unsafe { page.read_key_value(next) };
            let middle_size = record_size(middle_key, middle_value);
            let total = {
                let it = {
                    // Fill the two new pages (`new_a` and `new_b`), write the
                    // new separator into `middle`.
                    let it_a = page_a.iter_all().map(|(a, b, c)| (a, b, c, true));
                    let it_b = page_b.iter_all().map(|(a, b, c)| (a, b, c, true));
                    let it = it_a.chain(std::iter::once((next_ptr,
                                                Some((middle_key, middle_value)),
                                                0,
                                                false)))
                        .chain(it_b);
                    WithChanges::new(del, it, replacement)
                };
                let mut total = FIRST_BINDING_SIZE;
                for (x, _, _, _) in it {
                    if let Some((k, v)) = x {
                        total += record_size(k, v)
                    }
                }
                total
            };

            if total <= PAGE_SIZE_U16 {
                debug!("can merge {:?} {:?}", page_a, page_b);
                // If we can fit everything into a single page, merge!
                try!(self.merge(rng,
                                page_a,
                                page_b,
                                next_ptr,
                                middle_key,
                                middle_value,
                                del,
                                replacement));

                del.forgotten_ptr = page.offset(next as isize);
                del.forgotten_len = middle_size;
                return Ok(());

            } else {

                // Else, consider rebalancing. Rebalance if and only
                // if the modified page is really small (at most one
                // maximal binding size).  This makes sur that pages
                // have at least two bindings.
                debug!("should_rebalance? {:?} {:?} {:?} {:?}",
                       cursor_was_moved_backwards,
                       page_a.occupied(),
                       page_b.occupied(),
                       del.forgotten_len);

                let total_a = {
                    let it_a = page_a.iter_all().map(|(a, b, c)| (a, b, c, true));
                    let it = WithChanges::new(del, it_a, replacement);
                    let mut total = FIRST_BINDING_SIZE;
                    for (x, _, _, _) in it {
                        if let Some((k, v)) = x {
                            total += record_size(k, v)
                        }
                    }
                    total
                };
                let total_b = {
                    let it_b = page_b.iter_all().map(|(a, b, c)| (a, b, c, true));
                    let it = WithChanges::new(del, it_b, replacement);
                    let mut total = FIRST_BINDING_SIZE;
                    for (x, _, _, _) in it {
                        if let Some((k, v)) = x {
                            total += record_size(k, v)
                        }
                    }
                    total
                };
                debug!("total_a: {:?}, total_b: {:?}, moved {:?}",
                       total_a,
                       total_b,
                       cursor_was_moved_backwards);

                let should_rebalance = total_a <= (MAX_RECORD_SIZE + FIRST_BINDING_SIZE) ||
                                       total_b <= (MAX_RECORD_SIZE + FIRST_BINDING_SIZE);

                if should_rebalance {

                    debug!("cannot merge, rebalancing");
                    try!(self.rebalance(rng,
                                        page_a,
                                        page_b,
                                        next_ptr,
                                        middle_key,
                                        middle_value,
                                        total,
                                        del,
                                        replacement));
                    del.forgotten_ptr = page.offset(next as isize);
                    del.forgotten_len = record_size(middle_key, middle_value);
                    return Ok(());
                }
            }
        }

        // If nothing happened at the level below, or no merge or
        // rebalance was needed, we simply need to update the child
        // (in all other cases, this function has already returned).
        //
        // First move the cursor back in place.
        debug!("not merge, not rebalance");
        if cursor_was_moved_backwards {
            let forward = page.skiplist_move_cursor_forward(&mut del.current().cursor);
            assert!(forward)
        }
        // Then update
        self.update_del(rng, del, replacement)
    }

    /// Updates the root, taking care of merges in pages below.
    fn update_root<R: Rng, K: Representable, V: Representable>(&mut self,
                                                               rng: &mut R,
                                                               db: &mut Db<K, V>,
                                                               del: &mut Delete<K, V>,
                                                               repl: Option<Replacement<K, V>>)
                                                               -> Result<(), Error> {
        // debug!("UPDATE ROOT: {:?}", del.last_operation);
        match del.last_operation {
            Deleted::Merged { page, left_free, right_free, .. } => {
                let root = self.load_cow_page(del[1].page);
                // If there was only one binding here.
                let next = root.next_at_level(SKIPLIST_ROOT, 0);
                debug!("next = {:?} {:?}", root, root.next_at_level(next, 0));
                if root.next_at_level(next, 0) == NIL {
                    debug!("freeing");
                    unsafe {
                        let rc = self.rc(del[1].page);
                        if rc > 1 {
                            try!(self.set_rc(rng, del[1].page, rc - 1));
                        } else {
                            self.free_page_del(del, root.page_offset());
                            try!(self.rc_decr_del(rng, del, left_free));
                            try!(self.rc_decr_del(rng, del, right_free))
                        }
                    }
                    db.0 = page
                } else {
                    try!(self.update_del(rng, del, repl))
                }
            }
            _ => try!(self.update_del(rng, del, repl)),
        }
        Ok(())
    }


    /// Update the page below the current one with the event that
    /// happened there (this implies in particular that the page below
    /// has not been merged or rebalanced).
    fn update_del<R: Rng, K: Representable, V: Representable>(&mut self,
                                                              rng: &mut R,
                                                              del: &mut Delete<K, V>,
                                                              replacement: Option<Replacement<K,
                                                                                              V>>)
                                                              -> Result<(), Error> {

        let mut page = self.load_cow_page(del[del.cursor.pointer + 1].page);
        debug!("pointers/rc: {:?} {:?}",
               del.cursor.pointer,
               del.cursor.first_rc_level);
        match page {
            Cow::MutPage(ref mut p) if del.cursor.pointer + 1 < del.cursor.first_rc_level => {
                try!(self.update_mut(rng, del, replacement, p))
            }
            Cow::MutPage(ref p) => try!(self.update_const(rng, del, replacement, p)),
            Cow::Page(ref p) => try!(self.update_const(rng, del, replacement, p)),
        }

        del.forgotten_ptr = std::ptr::null();
        del.forgotten_len = 0;
        Ok(())
    }

    /// Update in the case of a mutable page (i.e. allocated in this
    /// transaction).
    fn update_mut<R: Rng, K: Representable, V: Representable>(&mut self,
                                                              rng: &mut R,
                                                              del: &mut Delete<K, V>,
                                                              replacement: Option<Replacement<K,
                                                                                              V>>,
                                                              p: &mut MutPage)
                                                              -> Result<(), Error> {
        // Delete<K,V> del.forgotten_key, possibly reinserting a replacement
        // debug!("update_mut, p {:?}, last_operation: {:?}",
        // p.page_offset(),
        // del.last_operation);

        match del.last_operation {
            Deleted::Merged { page, .. } => {
                // If the current key needs to be replaced, but its
                // left and right child have been merged, the
                // replacement is already in the merged child.
                p.skiplist_cursor_delete_next::<K, V>(&del[del.cursor.pointer + 1].cursor);
                // debug!("merged, last: {:?}", del.last_operation);
                p.set_right_child(del[del.cursor.pointer + 1][0], page);
                try!(self.free_last_operation(rng, del));
                del.last_operation = Deleted::None {
                    page: 0,
                    replaced_by: p.page_offset(),
                }
            }
            Deleted::None { replaced_by, .. } => {

                // debug!("deleted::none {:?} {:?}", next_k, del.forgotten_key);
                // debug!("del.forgotten_key = {:?}", del.forgotten_key);
                let next = p.next_at_level(del[del.cursor.pointer + 1][0], 0);
                let next_k = if next != 0 && next != SKIPLIST_ROOT {
                    p.offset(next as isize)
                } else {
                    std::ptr::null()
                };
                // debug!("deleting forgotten key, replacement {:?}", replacement);
                match replacement {
                    Some(repl) if next_k == repl.deleted_ptr => {
                        let size = record_size(repl.k, repl.v);
                        // assert!(k.as_ptr() != next_k);
                        // debug!("inserting replacement {:?}", std::str::from_utf8(repl.k));
                        debug!("occupied: {:?} {:?} {:?}",
                               p.occupied(),
                               repl.deleted_len,
                               size);
                        if p.occupied() - repl.deleted_len + size <= PAGE_SIZE_U16 {

                            if p.first_free() + size < PAGE_SIZE_U16 {
                                {
                                    // Delete<K,V> next.
                                    let pcurs = del.cursor.pointer;
                                    let ref mut skip_cursor = del[pcurs + 1].cursor;
                                    p.skiplist_cursor_delete_next::<K, V>(&skip_cursor);

                                    // Insert replacement
                                    p.skiplist_insert_after_cursor(rng,
                                                                   skip_cursor,
                                                                   repl.k,
                                                                   repl.v,
                                                                   replaced_by);
                                }
                                try!(self.free_last_operation(rng, del));
                                del.last_operation = Deleted::None {
                                    page: 0,
                                    replaced_by: p.page_offset(),
                                }
                            } else {
                                // compact + delete next.
                                try!(self.update_const(rng, del, replacement, p))
                            };
                        } else {
                            // Split
                            try!(self.split(rng, p, del, replacement));
                        }
                    }
                    _ => {
                        let cur = del[del.cursor.pointer + 1][0];
                        p.set_right_child(cur, replaced_by);

                        if next_k == del.forgotten_ptr && !next_k.is_null() {
                            p.skiplist_cursor_delete_next::<K, V>(&del[del.cursor.pointer + 1]
                                .cursor);
                        }
                        try!(self.free_last_operation(rng, del));
                        del.last_operation = Deleted::None {
                            page: 0,
                            replaced_by: p.page_offset(),
                        }
                    }
                }
            }
            Deleted::Rebalanced { key, value, right, left, .. } => {

                // Similarly to Deleted::Merged, if there's a
                // replacement, it is already included in the
                // rebalanced pages.

                let size = record_size(key, value);
                if p.occupied() + size - del.forgotten_len < PAGE_SIZE_U16 {
                    if p.first_free() + size < PAGE_SIZE_U16 {

                        p.skiplist_cursor_delete_next::<K, V>(&del[del.cursor.pointer + 1].cursor);
                        // debug!("rebalanced, last: {:?}", del.last_operation);
                        p.set_right_child(del[del.cursor.pointer + 1][0], left);
                        {
                            let ref mut skip_cursor = del.cursor.stack[del.cursor.pointer + 1]
                                .cursor;
                            p.skiplist_insert_after_cursor(rng, skip_cursor, key, value, right);
                        }
                        try!(self.free_last_operation(rng, del));
                        del.last_operation = Deleted::None {
                            page: 0,
                            replaced_by: p.page_offset(),
                        }
                    } else {
                        try!(self.update_const(rng, del, replacement, p))
                    }
                } else {
                    // Split
                    try!(self.split(rng, p, del, replacement));
                }
            }
            Deleted::Split { key, value, right, left, .. } => {
                // Similarly to Deleted::Merged, if there's a
                // replacement, it is already included in the
                // rebalanced pages.

                // debug!("split, last: {:?}", del.last_operation);
                let size = record_size(key, value);
                if p.occupied() + size - del.forgotten_len < PAGE_SIZE_U16 {
                    if p.first_free() + size < PAGE_SIZE_U16 {

                        p.set_right_child(del[del.cursor.pointer + 1][0], left);
                        {
                            let ref mut skip_cursor = del.cursor.stack[del.cursor.pointer + 1]
                                .cursor;
                            p.skiplist_insert_after_cursor(rng, skip_cursor, key, value, right);
                        }
                        try!(self.free_last_operation(rng, del));
                        del.last_operation = Deleted::None {
                            page: p.page_offset(),
                            replaced_by: p.page_offset(),
                        }
                    } else {
                        try!(self.update_const(rng, del, replacement, p))
                    }
                } else {
                    // Split
                    try!(self.split(rng, p, del, replacement));
                }
            }
        }
        Ok(())
    }

    /// Update in the case of an immutable page (i.e. allocated in a
    /// previous transaction).
    fn update_const<R: Rng, P: SkipListPage, K: Representable, V: Representable>
        (&mut self,
         rng: &mut R,
         del: &mut Delete<K, V>,
         replacement: Option<Replacement<K, V>>,
         p: &P)
         -> Result<(), Error> {
        // Build an iterator, and spread it on one or two freshly
        // allocated pages, depending on the total size of the
        // iterator.
        // Delete<K,V> del.forgotten_key, possibly reinserting a replacement

        // debug!("update_const, last_operation: {:?}", del.last_operation);

        // What's added (0 if things are only removed).
        let mut it_size = BINDING_HEADER_SIZE;
        {
            let p_it = p.iter_all().map(|(a, b, c)| (a, b, c, false));
            let it = WithChanges::new(del, p_it, replacement);
            for (x, _, _, _) in it {
                if let Some((k, v)) = x {
                    it_size += record_size(k, v)
                }
            }
        }

        if it_size <= PAGE_SIZE_U16 {
            let new_page = {
                debug!("it on page {:?}", p.page_offset());
                let incr_children_rc = del.cursor.pointer + 1 >= del.cursor.first_rc_level;
                debug!("{:?} {:?}", del.cursor.pointer, del.cursor.first_rc_level);
                let it = p.iter_all().map(|(a, b, c)| (a, b, c, incr_children_rc));
                let it = WithChanges::new(del, it, replacement);
                try!(self.spread_on_one_page(rng, it))
            };
            debug!("new_page: {:?}", new_page);
            let curs = del.cursor.pointer;
            del[curs + 1].page = new_page;
            debug!("curs: {:?}", &del.cursor.stack[..10]);

            try!(self.free_last_operation(rng, del));

            del.last_operation = Deleted::None {
                page: p.page_offset(),
                replaced_by: new_page,
            };
            Ok(())
        } else {
            self.split(rng, p, del, replacement)
        }
    }

    /// Special function to free a page in the context of a deletion:
    /// if the page is protected (i.e. contains a key which has not
    /// yet been copied), don't actually free it.
    unsafe fn free_page_del<K, V>(&mut self, del: &mut Delete<K, V>, page: u64) {

        assert!(page != 0);
        if page == del.protected_leaf {
            del.protected_leaf_was_freed = true
        } else {
            debug!("freeing page {:?}", page);
            self.free_page(page);
        }
    }

    unsafe fn rc_decr_del<R: Rng, K, V>(&mut self,
                                        rng: &mut R,
                                        del: &mut Delete<K, V>,
                                        page: u64)
                                        -> Result<(), Error> {

        let rc = self.rc(page);
        debug!("rc_decr_del: {:?} {:?}", page, rc);
        if rc == 1 {
            try!(self.remove_rc(rng, page));
            self.free_page_del(del, page)
        } else if rc == 0 {
            self.free_page_del(del, page)
        } else {
            try!(self.set_rc(rng, page, rc - 1))
        }
        Ok(())
    }

    /// Split the page, i.e. create an iterator including the
    /// replacement, and spread it over two pages. The iterator length
    /// must obviously be strictly more than one full page.
    fn split<R: Rng, P: SkipListPage, K: Representable, V: Representable>
        (&mut self,
         rng: &mut R,
         p: &P,
         del: &mut Delete<K, V>,
         replacement: Option<Replacement<K, V>>)
         -> Result<(), Error> {
        let after = {
            let page = del[del.cursor.pointer].page;
            let page = self.load_cow_page(page);
            page.offset(del.current()[0] as isize)
        };
        let (left, right, key, value) = {
            let incr_children_rc = del.cursor.pointer + 1 >= del.cursor.first_rc_level;
            let it = p.iter_all().map(|(a, b, c)| (a, b, c, incr_children_rc));
            let it = WithChanges::new(del, it, replacement);
            try!(self.spread_on_two_pages(rng, it, p.occupied()))
        };
        try!(self.free_last_operation(rng, del));
        del.last_operation = Deleted::Split {
            after: after,
            key: key,
            value: value,
            left: left,
            right: right,
            free: p.page_offset(),
        };
        Ok(())
    }


    /// Merge `page_a` and `page_b`, add `(middle_key, middle_value)`
    /// into the resulting page, and free `page_a` and `page_b`.
    fn merge<R: Rng, K: Representable, V: Representable>(&mut self,
                                                         rng: &mut R,
                                                         page_a: Cow,
                                                         page_b: Cow,
                                                         middle_ptr: *const u8,
                                                         middle_key: K,
                                                         middle_value: V,
                                                         del: &mut Delete<K, V>,
                                                         replacement: Option<Replacement<K, V>>)
                                                         -> Result<(), Error> {

        let incr_middle = del.cursor.pointer >= del.cursor.first_rc_level;
        let incr_a_children = incr_middle || self.rc(page_a.page_offset()) >= 2;
        let incr_b_children = incr_middle || self.rc(page_b.page_offset()) >= 2;
        let it_a = page_a.iter_all().map(|(a, b, c)| (a, b, c, incr_a_children));
        let it_b = page_b.iter_all().map(|(a, b, c)| (a, b, c, incr_b_children));

        debug!("MERGE {:?} {:?}",
               del.cursor.pointer,
               del.cursor.first_rc_level);
        let new_page = {
            let it = it_a.chain(std::iter::once((middle_ptr,
                                        Some((middle_key, middle_value)),
                                        0,
                                        incr_middle)))
                .chain(it_b);
            let it = WithChanges::new(del, it, replacement);
            try!(self.spread_on_one_page(rng, it))
        };
        debug!("finished");
        try!(self.free_last_operation(rng, del));
        del.last_operation = Deleted::Merged {
            page: new_page,
            left_free: page_a.page_offset(),
            right_free: page_b.page_offset(),
        };
        Ok(())
    }

    /// Free all pages used in the last operation.
    fn free_last_operation<R: Rng, K, V>(&mut self,
                                         rng: &mut R,
                                         del: &mut Delete<K, V>)
                                         -> Result<(), Error> {
        debug!(">> free_last_operation {:?} {:?}",
               del.cursor.pointer,
               del.cursor.first_rc_level);
        {
            use transaction::LoadPage;
            let rc_root = self.root_(super::RC_ROOT);
            debug!("rc_root = {:?}", rc_root);
        }
        match del.last_operation {
            Deleted::Rebalanced { left_free, right_free, .. } |
            Deleted::Merged { left_free, right_free, .. } => {
                if del.cursor.pointer + 1 < del.cursor.first_rc_level {
                    unsafe {
                        try!(self.rc_decr_del(rng, del, left_free));
                        try!(self.rc_decr_del(rng, del, right_free));
                    }
                }
            }
            Deleted::Split { free, .. } => unsafe {
                if del.cursor.pointer + 1 < del.cursor.first_rc_level {
                    try!(self.rc_decr_del(rng, del, free));
                }
            },
            Deleted::None { page, .. } if page != 0 => unsafe {
                if del.cursor.pointer + 1 < del.cursor.first_rc_level {
                    try!(self.rc_decr_del(rng, del, page));
                }
            },
            _ => {}
        }
        {
            use transaction::LoadPage;
            let rc_root = self.root_(super::RC_ROOT);
            debug!("rc_root = {:?}", rc_root);
        }
        debug!("<< free_last_operation");
        Ok(())
    }

    /// rebalance, i.e. allocate two pages, spread the contents of
    /// `page_a` and `page_b` on the newly allocated pages, and
    /// schedule `page_a` and `page_b` for freeing (the
    /// separator key of the two new pages is on one of
    /// {`page_a`,`page_b'}, so we cannot free these now).
    fn rebalance<R: Rng, K: Representable, V: Representable>(&mut self,
                                                             rng: &mut R,
                                                             page_a: Cow,
                                                             page_b: Cow,
                                                             middle_ptr: *const u8,
                                                             middle_key: K,
                                                             middle_value: V,
                                                             total_child_size: u16,
                                                             del: &mut Delete<K, V>,
                                                             replacement: Option<Replacement<K,
                                                                                             V>>)
                                                             -> Result<(), Error> {
        let incr_middle = del.cursor.pointer >= del.cursor.first_rc_level;
        let incr_a_children = incr_middle || self.rc(page_a.page_offset()) >= 2;
        let incr_b_children = incr_middle || self.rc(page_b.page_offset()) >= 2;
        let it_a = page_a.iter_all().map(|(a, b, c)| (a, b, c, incr_a_children));
        let it_b = page_b.iter_all().map(|(a, b, c)| (a, b, c, incr_b_children));
        debug!("REBALANCE {:?} {:?}",
               del.cursor.pointer,
               del.cursor.first_rc_level);
        let spread = {
            // Fill the two new pages (`new_a` and `new_b`), write the
            // new separator into `middle`.
            let it = it_a.chain(std::iter::once((middle_ptr,
                                        Some((middle_key, middle_value)),
                                        0,
                                        incr_middle)))
                .chain(it_b);
            let it = WithChanges::new(del, it, replacement);
            // Write the new separator for the next round.
            try!(self.spread_on_two_pages(rng, it, total_child_size))
        };
        try!(self.free_last_operation(rng, del));
        let (new_a, new_b, k, v) = spread;
        del.last_operation = Deleted::Rebalanced {
            key: k,
            value: v,
            left: new_a,
            right: new_b,
            left_free: page_a.page_offset(),
            right_free: page_b.page_offset(),
        };
        Ok(())
    }
}

/// A page iterator taking care of replacements
struct WithChanges<'b, I: Iterator, K: 'b, V: 'b> {
    /// The current `Delete<K,V>` record.
    del: &'b mut Delete<K, V>,

    /// Whether the key scheduled for deletion in `self.del` has
    /// already been deleted.
    forgotten: bool,

    /// Will `self.next()` return the center key from a split?
    next_is_split: bool,

    /// The initial iterator (without the replacement)
    it: std::iter::Peekable<I>,

    /// Replacement
    replacement: Option<Replacement<K, V>>,
}

impl<'b, K, V, I: Iterator> WithChanges<'b, I, K, V> {
    fn new(del: &'b mut Delete<K, V>,
           it: I,
           replacement: Option<Replacement<K, V>>)
           -> WithChanges<'b, I, K, V> {
        WithChanges {
            del: del,
            forgotten: false,
            next_is_split: false,
            it: it.peekable(),
            replacement: replacement,
        }
    }
}

impl<'b, K:Representable, V:Representable,
     P: Iterator<Item = (*const u8, Option<(K, V)>, u64, bool)>>
    Iterator for WithChanges<'b, P, K, V> {

    type Item = (Option<(K, V)>, u64, bool, bool);

    fn next(&mut self) -> Option<Self::Item> {

        if self.next_is_split {
// First, if the last key is the one before the extra key
// from a split, return the extra key at its correct
// position.
            match self.del.last_operation {
                Deleted::Split { key, value, right, .. } => {
                    self.next_is_split = false;
                    return Some((Some((key, value)), right, false, false));
                }
                _ => {}
            }
        }
// debug!("last_op: {:?}", self.del.last_operation);
        match (self.it.next(), self.replacement) {

            (None,_) => {
                debug!("it: none");
                None
            },
            (Some((ptr, Some(_), r, incr_rc)), repl)
                if ptr == self.del.forgotten_ptr && !self.forgotten => {
// If the iterator is not yet finished.  If the
// current key is to be forgotten, and has not yet
// been.

                    debug!("forgotten");
 // mark the current key as forgotten.
                    match self.del.last_operation {

                        Deleted::Rebalanced { key, value, right, .. } => {
// The page below has been rebalanced.
                            Some((Some((key, value)), right, false, incr_rc))
                        }
                        Deleted::Split { key, value, right, .. } => {
// The page below has split. We insert the new
// element after the deleted one.
                            Some((Some((key, value)), right, false, incr_rc))
                        }
                        Deleted::Merged { .. } => {
// If there's a replacement, it's been included in
// the merged page already.
                            self.next()
                        },
                        Deleted::None { page, replaced_by } => {
                            let next = match repl {
                                Some(ref repl) if repl.deleted_ptr == ptr => {
                                    Some((repl.k, repl.v))
                                },
                                _ => None
                            };
                            if r == page {
                                Some((next, replaced_by, false, incr_rc))
                            } else {
                                Some((next, r, incr_rc, incr_rc))
                            }
                        }
                    }
                }
            (Some((ptr, Some(_), r, incr_rc)), Some(ref repl))
                if ptr == repl.deleted_ptr => {
// The next key is in an internal node, and the
// replacement replaces it.
                    match self.del.last_operation {
                        Deleted::None { replaced_by, .. } =>
                            Some((Some((repl.k, repl.v)), replaced_by, false, incr_rc)),
                        _ => Some((Some((repl.k, repl.v)), r, incr_rc, incr_rc))
                    }
                }
            (Some((ptr, entry, r, incr_rc)), _) => {

                debug!("not forgotten forgotten = {:?} != {:?}", self.del.forgotten_ptr, ptr);

// If the last operation was a split, whose separator
// is immediately before k, return the separator
// binding.
                if let Some((k,v)) = entry {
// debug!("k = {:?}", std::str::from_utf8(&k[..std::cmp::min(10, k.len())]));
                    match self.del.last_operation {
                        Deleted::Split { after, left, .. } if after == ptr => {
                            self.next_is_split = true;
                            return Some((Some((k, v)), left, false, incr_rc));
                        }
                        _ => {}
                    }
                }
                let next_is_forgotten =
                    if let Some(&(next_ptr, Some(_), _, _)) = self.it.peek() {
                        next_ptr == self.del.forgotten_ptr
                    } else {
                        false
                    };
                debug!("next_is_forgotten {:?}", next_is_forgotten);
// If the current key is not to be forgotten, maybe
// the next one is, in which case we might need to
// replace r (the current key's right child).

                match self.del.last_operation {
                    Deleted::Split { left, .. } |
                    Deleted::Rebalanced { left, .. } if next_is_forgotten => {
// If the pages below have been
// rebalanced, the replacement comes
// with a left child. (and the current
// key's right child will be freed in
// the next iteration of this
// iterator, by the unsafe block
// above).
                        Some((entry, left, false, incr_rc))
                    }

                    Deleted::Merged { page, .. } if next_is_forgotten => {
// If the pages below have been
// merged, they have been freed, and
// we need to replace `r` by the
// merged page.
                        debug!("replacing {:?} with {:?}", r, page);
                        Some((entry, page, false, incr_rc))
                    }

                    Deleted::None { page, replaced_by } => {
                        if r == page {
                            Some((entry, replaced_by, false, incr_rc))
                        } else {
                            Some((entry, r, incr_rc, incr_rc))
                        }
                    }
                    _ => Some((entry, r, incr_rc, incr_rc))
                }
            }
        }
    }
    }
