use byteorder::{ByteOrder, LittleEndian};
use std;
use std::convert::TryInto;

use {Alloc, MutPage, Result, Storable, Storage, Stored, StoredHeader};
use PAGE_SIZE;

#[derive(Clone, Copy, Debug)]
pub struct LargeBufHeader {
    first_page: usize,
}

impl StoredHeader for LargeBufHeader {
    type PageOffsets = std::iter::Once<u64>;
    fn onpage_size(&self) -> u16 { 16 }
    fn page_offsets(&self) -> Self::PageOffsets {
        std::iter::once(self.first_page as u64)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LargeBuf<'sto> {
    storage: Storage<'sto>,
    first_page: usize,
    len: usize,
}

struct LargeBufIter<'sto> {
    storage: Storage<'sto>,
    next_page: usize,
    remaining_len: usize,
}

impl<'sto> Iterator for LargeBufIter<'sto> {
    type Item = &'sto [u8];
    fn next(&mut self) -> Option<&'sto [u8]> {
        if self.remaining_len == 0 {
            None
        } else {
            let chunk_len = ::std::cmp::min(self.remaining_len, PAGE_SIZE as usize - 8);
            let buf = self.storage.get_page(self.next_page);
            self.next_page = LittleEndian::read_u64(buf).try_into().unwrap();
            self.remaining_len -= chunk_len;
            Some(&buf[8..])
        }
    }
}

impl<'sto> LargeBuf<'sto> {
    pub fn iter(&self) -> impl Iterator<Item=&'sto [u8]> {
        LargeBufIter {
            storage: self.storage,
            next_page: self.first_page,
            remaining_len: self.len,
        }
    }
}

impl<'sto> PartialEq for LargeBuf<'sto> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'sto> PartialOrd for LargeBuf<'sto> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        Some(self.iter().cmp(other.iter()))
    }
}

impl<'sto> Stored<'sto> for LargeBuf<'sto> {
    type Header = LargeBufHeader;

    fn header(&self) -> LargeBufHeader {
        LargeBufHeader { first_page: self.first_page }
    }

    fn read_header(buf: &[u8]) -> LargeBufHeader {
        LargeBufHeader {
            first_page: LittleEndian::read_u64(buf).try_into().unwrap(),
        }
    }

    fn drop_value(&self, alloc: &mut Alloc) -> Result<()> {
        let mut next_page = self.first_page;
        let mut remaining_len = self.len;
        while remaining_len > 0 {
            let buf = self.storage.get_page(next_page);
            remaining_len -= PAGE_SIZE as usize - 8;
            alloc.free_page(next_page);
            next_page = LittleEndian::read_u64(buf).try_into().unwrap();
        }
        Ok(())
    }

    fn alignment() -> ::Alignment {
        ::Alignment::B8
    }

    fn read_value(buf: &[u8], storage: &Storage<'sto>) -> Self {
        LargeBuf {
            first_page: LittleEndian::read_u64(buf).try_into().unwrap(),
            storage: storage.clone(),
            len: LittleEndian::read_u64(&buf[8..]).try_into().unwrap(),
        }
    }

    fn write_value(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.first_page as u64);
        LittleEndian::write_u64(&mut buf[8..], self.len as u64);
    }
}

impl<'sto> Storable<'sto, LargeBuf<'sto>> for LargeBuf<'sto> {
    fn store<'a>(&self, _: &mut Alloc<'a, 'sto>) -> Result<Self> { Ok(*self) }
}

impl<'sto> PartialEq<LargeBuf<'sto>> for [u8] {
    fn eq(&self, other: &LargeBuf<'sto>) -> bool {
        self.chunks(PAGE_SIZE as usize - 8).eq(other.iter())
    }
}

impl<'sto> PartialOrd<LargeBuf<'sto>> for [u8] {
    fn partial_cmp(&self, other: &LargeBuf<'sto>) -> Option<std::cmp::Ordering> {
        self.chunks(PAGE_SIZE as usize - 8).partial_cmp(other.iter())
    }
}

impl<'sto> Storable<'sto, LargeBuf<'sto>> for [u8] {
    fn store<'a>(&self, alloc: &mut Alloc<'a, 'sto>) -> Result<LargeBuf<'sto>> {
        let chunk_len = PAGE_SIZE as usize - 8;
        let mut me = self;
        let mut prev_page: Option<MutPage> = None;
        let mut first_offset: Option<usize> = None;
        while !me.is_empty() {
            let mut page = alloc.alloc_page()?;
            page.buf()[8..].copy_from_slice(&me[..chunk_len]);
            me = &me[chunk_len..];

            // Build the linked list of pages by writing the offset of the new page at the
            // beginning of the last page.
            if let Some(mut prev) = prev_page {
                LittleEndian::write_u64(prev.buf(), page.offset() as u64);
            }
            if first_offset.is_none() {
                first_offset = Some(page.offset());
            }
            prev_page = Some(page);
        }
        Ok(LargeBuf {
            first_page: first_offset.unwrap_or(0),
            storage: alloc.storage(),
            len: self.len(),
        })
    }
}
