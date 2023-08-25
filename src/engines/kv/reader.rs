use crate::thread_pool::ThreadPool;
use crate::{RayonThreadPool, Result};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

pub struct LogReader<R: Read + Seek> {
    reader: BufReaderWithPos<R>,
}

impl<R: Read + Seek> LogReader<R> {
    pub fn new(inner: R) -> Result<LogReader<R>> {
        Ok(LogReader {
            reader: BufReaderWithPos::new(inner, 0)?,
        })
    }
}

pub struct VLogReader<R: Read + Seek> {
    reader: BufReaderWithPos<R>,
    pool: RayonThreadPool,
}

impl<R: Read + Seek> VLogReader<R> {
    pub fn new(inner: R) -> Result<VLogReader<R>> {
        Ok(VLogReader {
            reader: BufReaderWithPos::new(inner, 0)?,
            pool: RayonThreadPool::new(8)?,
        })
    }

    pub fn get_value(&self, pos: u64, size: usize) -> Result<String> {
        // simple implementation
        // TODO: use thread pool to concurrently read
        self.reader.seek(SeekFrom::Start(pos));
        let mut buf = self.reader.take(size as u64);
        let mut value = String::new();
        buf.read_to_string(&mut value)?;
        Ok(value)
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(inner: R, pos: u64) -> Result<BufReaderWithPos<R>> {
        let mut reader = BufReader::new(inner);
        let pos = reader.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos { reader, pos })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.seek(SeekFrom::Start(self.pos))?;
        let n = self.reader.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
