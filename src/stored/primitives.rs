use byteorder::{ByteOrder, LittleEndian};
use std;

use {Alignment, Alloc, Result, Storable, Storage, Stored, StoredHeader};

macro_rules! store_int {
    ($int:ty, $align:expr, $header:ident, $read_fn:expr, $write_fn:expr) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $header {}

        impl StoredHeader for $header {
            type PageOffsets = std::iter::Empty<u64>;
            fn onpage_size(&self) -> u16 { ::std::mem::size_of::<$int>() as u16 }
            fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
        }

        impl<'sto> Storable<'sto, $int> for $int {
            fn store(&self, _: &mut Alloc) -> Result<$int> { Ok(*self) }
        }

        impl<'sto> Stored<'sto> for $int {
            type Header = $header;
            fn header(&self) -> $header { $header {} }
            fn read_header(_: &[u8]) -> $header { $header {} }
            fn drop_value(&self, _: &mut Alloc) -> Result<()> { Ok(()) }
            fn alignment() -> Alignment { $align }
            fn read_value(buf: &[u8], _: &Storage<'sto>) -> $int {
                $read_fn(buf)
            }
            fn write_value(&self, buf: &mut [u8]) {
                $write_fn(buf, *self)
            }
        }
    };
}

fn read_u8(buf: &[u8]) -> u8 { buf[0] }
fn write_u8(buf: &mut [u8], val: u8) { buf[0] = val }
fn read_i8(buf: &[u8]) -> i8 { buf[0] as i8 }
fn write_i8(buf: &mut [u8], val: i8) { buf[0] = val as u8 }

store_int!(u64, Alignment::B8, U64StoredHeader, LittleEndian::read_u64, LittleEndian::write_u64);
store_int!(u32, Alignment::B4, U32StoredHeader, LittleEndian::read_u32, LittleEndian::write_u32);
store_int!(u16, Alignment::B2, U16StoredHeader, LittleEndian::read_u16, LittleEndian::write_u16);
store_int!(u8, Alignment::B1, U8StoredHeader, read_u8, write_u8);

store_int!(i64, Alignment::B8, I64StoredHeader, LittleEndian::read_i64, LittleEndian::write_i64);
store_int!(i32, Alignment::B4, I32StoredHeader, LittleEndian::read_i32, LittleEndian::write_i32);
store_int!(i16, Alignment::B2, I16StoredHeader, LittleEndian::read_i16, LittleEndian::write_i16);
store_int!(i8, Alignment::B1, I8StoredHeader, read_i8, write_i8);

