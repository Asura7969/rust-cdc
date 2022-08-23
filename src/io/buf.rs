use std::str::from_utf8;
use bit_set::BitSet;

use bytes::{Buf, Bytes};
use memchr::memchr;
use crate::err_protocol;

use crate::error::Error;

pub trait BufExt: Buf {
    // Read a nul-terminated byte sequence
    fn get_bytes_nul(&mut self) -> Result<Bytes, Error>;

    // Read a byte sequence of the exact length
    fn get_bytes(&mut self, len: usize) -> Bytes;

    // Read a nul-terminated string
    fn get_str_nul(&mut self) -> Result<String, Error>;

    // Read a string of the exact length
    fn get_str(&mut self, len: usize) -> Result<String, Error>;

    fn get_str_until(&mut self, pause: u8) -> Result<String, Error>;

    fn get_packet_num(&mut self) -> Result<i32, Error>;

    fn read_bitset(&mut self, len: usize, big_endian: bool) -> Result<BitSet, Error>;
}

impl BufExt for Bytes {
    fn get_bytes_nul(&mut self) -> Result<Bytes, Error> {
        let nul =
            memchr(b'\0', &self).ok_or_else(|| err_protocol!("expected NUL in byte sequence"))?;

        let v = self.slice(0..nul);

        self.advance(nul + 1);

        Ok(v)
    }

    fn get_bytes(&mut self, len: usize) -> Bytes {
        let v = self.slice(..len);
        self.advance(len);

        v
    }

    fn get_str_nul(&mut self) -> Result<String, Error> {
        self.get_bytes_nul().and_then(|bytes| {
            from_utf8(&*bytes)
                .map(ToOwned::to_owned)
                .map_err(|err| err_protocol!("{}", err))
        })
    }

    fn get_str(&mut self, len: usize) -> Result<String, Error> {
        let v = from_utf8(&self[..len])
            .map_err(|err| err_protocol!("{}", err))
            .map(ToOwned::to_owned)?;

        self.advance(len);

        Ok(v)
    }

    fn get_str_until(&mut self, pause: u8) -> Result<String, Error> {
        let mut slice = vec![];
        loop {
            let e = self.get_u8();
            if e != pause {
                slice.push(e);
            } else {
                break
            }
        }
        let v = from_utf8(slice.as_slice())
            .map_err(|err| err_protocol!("{}", err))
            .map(ToOwned::to_owned)?;
        Ok(v)

    }

    fn get_packet_num(&mut self) -> Result<i32, Error> {
        let b = self.get_u8();
        if b < 251 {
            Ok(b as i32)
        } else if b == 251 {
            Err(err_protocol!("Unexpected NULL where int should have been"))
        } else if b == 252 {
            Ok(self.get_u16_le() as i32)
        } else if b == 253 {
            let i = self.get_bytes(3).get_u64_le();
            if i > i32::MAX as u64 {
                Err(err_protocol!("Stumbled upon long even though int expected"))
            } else {
                Ok(i as i32)
            }
        } else if b == 254 {
            let i = self.get_u64_le();
            if i > i32::MAX as u64 {
                Err(err_protocol!("Stumbled upon long even though int expected"))
            } else {
                Ok(i as i32)
            }
        } else {
            Err(err_protocol!("Unexpected packed number byte {}", b))
        }
    }

    fn read_bitset(&mut self, len: usize, big_endian: bool) -> Result<BitSet, Error> {
        // according to MySQL internals the amount of storage required for N columns is INT((N+7)/8) bytes
        let mut bytes = self.get_bytes((len + 7) >> 3).to_vec();
        if !big_endian {
            bytes.reverse();
        }
        let mut result = BitSet::new();
        for i in 0..bytes.len() {
            if (bytes[i >> 3] & (1 << (i % 8))) != 0 {
                result.insert(i);
            }
        }
        Ok(result)
    }
}
