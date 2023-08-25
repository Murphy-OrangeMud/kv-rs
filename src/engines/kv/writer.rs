use crate::Result;
use std::io::{BufWriter, Error, ErrorKind, Seek, SeekFrom, Write};

pub struct LogWriter<W: Write + Seek> {
    writer: BufWriterWithPos<W>,
}

impl<W: Write + Seek> LogWriter<W> {
    pub fn new(inner: W) -> Result<LogWriter<W>> {
        Ok(LogWriter {
            writer: BufWriterWithPos::new(inner)?,
        })
    }

    pub fn add_record(&mut self, buf: &[u8]) -> Result<()> {
        // TODO: mvcc
        // for now it's simple implementation
        let n = self.writer.write(buf)?;
        self.writer.flush()?;
        if n != buf.len() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Not written enough bytes and corrupted file",
            ));
        }
        Ok(())
    }

    fn emit_physical_record(&mut self) {}
}

pub struct VLogWriter<W: Write + Seek> {
    writer: BufWriterWithPos<W>,
}

impl<W: Write + Seek> VLogWriter<W> {
    pub fn new(inner: W) -> Result<VLogWriter<W>> {
        Ok(VLogWriter {
            writer: BufWriterWithPos::new(inner)?,
        })
    }

    pub fn add_record(&mut self, buf: &[u8]) -> Result<()> {
        // TODO: mvcc
        // for now it's simple implementation
        let n = self.writer.write(buf)?;
        self.writer.flush()?;
        if n != buf.len() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Not written enough bytes and corrupted file",
            ));
        }
        Ok(())
    }

    pub fn emit_physical_record(&mut self) {}
}

#[derive(Debug)]
pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(inner: W) -> Result<BufWriterWithPos<W>> {
        let mut writer = BufWriter::new(inner);
        let pos = writer.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: writer,
            pos: pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // not safe for concurrency
        let n = self.writer.write(buf)?;
        self.pos = self.writer.seek(SeekFrom::Current(0))?;
        Ok(n)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
