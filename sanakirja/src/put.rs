// Copyright 2015 Pierre-Étienne Meunier and Florent Becker. See the
// COPYRIGHT file at the top-level directory of this distribution and
// at http://pijul.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use {Transaction, Representable, PageT, MutTxn, Db, Error, CursorStack, PAGE_SIZE_U16};
use transaction::Cow;
use skiplist::{SkipListPage, FIRST_BINDING_SIZE, record_size};
use skiplist;
use std;
use rand::Rng;

impl<'env> MutTxn<'env> {
    /// Insert a binding to a database, returning `false` if and only
    /// if the exact same binding (key *and* value) was already in the database.
    pub fn put<R: Rng, K: Representable, V: Representable>(&mut self,
                                                           rng: &mut R,
                                                           db: &mut Db<K, V>,
                                                           key: K,
                                                           value: V)
                                                           -> Result<bool, Error> {

        let mut cursor = CursorStack::new();
        if cursor.set(self, db, Some((key, Some(value))))? {
            return Ok(false);
        }
        // Now insert in the leaf and split all pages that need to be split.

        let leaf_pointer = cursor.pointer;

        // Split all pages that need it.
        //
        // This loop uses unsafe slices, but the memory management is
        // simple: all pointers used in the loop are valid until we
        // explicitely call free, after the loop.
        let last_allocated;
        {
            let mut inserted_right_child = 0;
            let mut inserted_key = key;
            let mut inserted_value = value;
            let mut inserted_left_child = 0u64;

            loop {

                let mut page = self.load_cow_page(cursor.current().page);

                debug!("page: {:?}", page);

                // If we're "CoWing" the first double-pointed page on the stack, decrease its RC.
                if cursor.pointer == cursor.first_rc_level {
                    let page = cursor.current().page;
                    let rc = self.rc(page);
                    if rc >= 2 {
                        try!(self.set_rc(rng, page, rc - 1));
                    }
                }

                if page.can_alloc(inserted_key, inserted_value) != 0 {
                    debug!("can alloc");
                    // We can allocate. Do we need to duplicate this
                    // page (because of RC or first_free)?
                    let needs_dup = page.first_free() + record_size(inserted_key, inserted_value) >
                                    PAGE_SIZE_U16;

                    match page {
                        Cow::MutPage(ref mut page) if cursor.pointer < cursor.first_rc_level &&
                                                      !needs_dup => {
                            debug!("does not need dup");

                            // No, there will be no other references to
                            // this page after this method (`put`)
                            // returns. Here, simply insert.

                            // Update the right child from the previous split
                            // (if any, `inserted_left_child` is 0 else).
                            debug!("page right child: {:?}", cursor.current()[0]);
                            page.set_right_child(cursor.current()[0], inserted_left_child);
                            page.skiplist_insert_after_cursor(rng,
                                                              &mut cursor.current_mut().cursor,
                                                              inserted_key,
                                                              inserted_value,
                                                              inserted_right_child);
                            use PageT;
                            last_allocated = page.page_offset();
                        }
                        _ => {
                            debug!("needs dup {:?} {:?} {:?}",
                                   cursor.pointer,
                                   cursor.first_rc_level,
                                   needs_dup);
                            // Yes, we do need to duplicate this
                            // page. Propagate RC to pages
                            // below.
                            let page = self.load_cow_page(cursor.current().page);

                            // Iterate over the page, incorporating the new element.
                            use PageT;
                            let it = Iter {
                                iter: page.iter_all().peekable(),
                                off: page.offset(cursor.current()[0] as isize),
                                next_is_extra: false,
                                extra_left: inserted_left_child,
                                extra: (Some((inserted_key, inserted_value)), inserted_right_child),
                            };

                            // Increase the RC of everyone, but the left
                            // side of the split (which was just
                            // allocated now).
                            let incr_rc = cursor.pointer >= cursor.first_rc_level;
                            last_allocated = try!(self.spread_on_one_page(rng,
                                                                          it.map(|(b, c)| {
                                let fresh = c == inserted_left_child || c == inserted_right_child;
                                (b, c, incr_rc && !fresh, incr_rc)
                            })));

                            if cursor.pointer < cursor.first_rc_level {
                                unsafe {
                                    // free of the current page.
                                    self.free_page(cursor.current().page)
                                }
                            }
                        }
                    }
                    break;

                } else {
                    // We cannot allocate, we need to split this page

                    debug!("cannot alloc");
                    let (left, right, sep_key, sep_value) = {
                        let it = Iter {
                            iter: page.iter_all().peekable(),
                            off: page.offset(cursor.current()[0] as isize),
                            next_is_extra: false,
                            extra_left: inserted_left_child,
                            extra: (Some((inserted_key, inserted_value)), inserted_right_child),
                        };
                        // We're going to add a page, hence
                        // FIRST_BINDING_SIZE more bytes will be
                        // needed.
                        let total = page.occupied() + record_size(key, value) + FIRST_BINDING_SIZE;
                        if cursor.pointer >= cursor.first_rc_level {
                            let it = it.map(|(b, c)| {
                                if c == inserted_left_child || c == inserted_right_child {
                                    (b, c, false, true)
                                } else {
                                    (b, c, true, true)
                                }
                            });
                            try!(self.spread_on_two_pages(rng, it, total))
                        } else {
                            let it = it.map(|(b, c)| (b, c, false, false));
                            try!(self.spread_on_two_pages(rng, it, total))
                        }
                    };

                    // Extend the lifetime of "inserted_key".
                    inserted_key = sep_key;
                    inserted_left_child = left;
                    inserted_right_child = right;
                    inserted_value = sep_value;

                    cursor.pointer -= 1;
                    if cursor.pointer == 0 {

                        // We've just split the root! we need to
                        // allocate a page to collect the two sides of
                        // the split.
                        last_allocated = try!(self.split_root(rng,
                                                              &mut cursor,
                                                              inserted_left_child,
                                                              inserted_right_child,
                                                              inserted_key,
                                                              inserted_value));
                        break;
                    }
                }
            }
        }
        debug!("freeing pages from {:?} to {:?} {:?} (inclusive)",
               cursor.pointer + 1,
               leaf_pointer,
               cursor.first_rc_level);
        // Now, cursor_pointer is the reference to a page that doesn't
        // need to split anymore (possibly the new root).

        // Free all split pages.
        let last_free = std::cmp::min(leaf_pointer + 1, cursor.first_rc_level);
        let first_free = std::cmp::min(cursor.pointer + 1, last_free);
        for page_cursor in &cursor.stack[first_free..last_free] {
            // free the page that was split.
            let rc = self.rc(page_cursor.page);
            if rc <= 1 {
                debug!("Freeing {:?}", page_cursor.page);
                try!(self.remove_rc(rng, page_cursor.page));
                unsafe { self.free_page(page_cursor.page) }
            } else {
                debug!("Decrease RC for page {:?}, new rc: {:?}",
                       page_cursor.page,
                       rc - 1);
                try!(self.set_rc(rng, page_cursor.page, rc - 1));
            }
        }

        // In the above loop, `break` only occur after an insertion of
        // a (key, value) with right child `inserted_right_child`. For
        // the loop below to update the reference properly in the
        // pages above, we need to update that child here (it might be
        // 0 if we're still at the leaf).

        // Has the root split?
        if cursor.pointer > 0 {
            cursor.stack[cursor.pointer].page = last_allocated;
            cursor.pointer -= 1;
            // The root has not split, the splits stopped earlier. There are remaining pages.
            while cursor.pointer > 0 {

                if cursor.pointer == cursor.first_rc_level {
                    let page = cursor.current().page;
                    let rc = self.rc(page);
                    if rc >= 2 {
                        try!(self.set_rc(rng, page, rc - 1));
                    }
                }
                try!(self.update_put::<R, K, V>(rng, &mut cursor));
                cursor.pointer -= 1;
            }

            db.0 = cursor[1].page

        } else {
            // The root has split, no need to do anything.
            debug!("the root has split");
            db.0 = last_allocated
        }
        Ok(true)
    }


    fn update_put<R: Rng, K: Representable, V: Representable>(&mut self,
                                                              rng: &mut R,
                                                              cursor: &mut CursorStack)
                                                              -> Result<(), Error> {
        use PageT;
        let mut page = self.load_cow_page(cursor.current().page);
        match page {

            Cow::MutPage(ref mut p) if cursor.pointer < cursor.first_rc_level => {

                p.set_right_child(cursor.current()[0], cursor[cursor.pointer + 1].page);
                cursor.current_mut().page = p.offset;
            }
            Cow::MutPage(_) | Cow::Page(_) => {
                // Here, the page needs to be CoWed.
                let incr_rc = cursor.pointer >= cursor.first_rc_level;
                let current_ptr = page.offset(cursor.current()[0] as isize);
                let new_page =
                    try!(self.spread_on_one_page::<_, K, V, _>(rng,
                                                               page.iter_all()
                                                                   .map(|(off, b, c)| {
                            // Both pages will survive, increase reference count.
                            if off != current_ptr {
                                (b, c, incr_rc, incr_rc)
                            } else {
                                let c = cursor[cursor.pointer + 1].page;
                                (b, c, false, incr_rc)
                            }
                        })));
                // If nothing points to the page anymore, free.
                if cursor.pointer < cursor.first_rc_level {
                    unsafe {
                        use PageT;
                        debug!("line {:?}, freeing {:?}", line!(), page.page_offset());
                        self.free_page(page.page_offset())
                    }
                }
                debug!("NEW_PAGE = {:?}", new_page);
                cursor.current_mut().page = new_page;
            }
        }
        Ok(())
    }
}

/// Iterator on a page, plus a middle element immediately after `off`,
/// with a fixed left and right child.
pub struct Iter<'a, P: SkipListPage + 'a, K: Representable, V: Representable> {
    iter: std::iter::Peekable<skiplist::Iter<'a, P, K, V>>,
    off: *const u8,
    next_is_extra: bool,
    extra_left: u64,
    extra: (Option<(K, V)>, u64),
}

impl<'a, P: SkipListPage + 'a, K: Representable, V: Representable> Iterator for Iter<'a, P, K, V> {
    type Item = (Option<(K, V)>, u64);

    fn next(&mut self) -> Option<Self::Item> {

        if self.next_is_extra {
            debug!("next_is_extra");
            self.next_is_extra = false;
            self.off = std::ptr::null();
            Some(self.extra)

        } else {
            debug!("not next_is_extra");

            if let Some((a, b, d)) = self.iter.next() {

                // if a == self.off, the extra element is to be
                // inserted immediately after this element.  Set
                // next_is_extra, and replace the current element's
                // right child with extra's left child.
                debug!("{:?}", a);
                if a == self.off {
                    self.next_is_extra = true;
                    Some((b, self.extra_left))
                } else {
                    Some((b, d))
                }

            } else {
                debug!("finished, extra = {:?}", self.off);
                None

            }
        }
    }
}
