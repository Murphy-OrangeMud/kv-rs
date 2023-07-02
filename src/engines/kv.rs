use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{ErrorKind, Write, Read, Seek, BufReader, BufWriter, SeekFrom, BufRead};
use crate::{KvsEngine, Result};


#[derive(Debug)]
pub enum KVSError {
    NoSuchKey,
    WriteLogFail,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Command {
    Set,
    Remove,
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    cmd: Command,
    key: String,
    value: String,
}

pub struct KvStore {
    kv: HashMap<String, u64>,
    path: PathBuf,
    log_writer: BufWriterWithPos<File>,
    reader: BufReaderWithPos<File>,
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = serde_json::to_string(&Record { cmd: Command::Set, key: key.clone(), value: value })? + "\n";
        let n = self.log_writer.write(record.as_bytes())?;
        self.log_writer.flush()?;
        if n != record.as_bytes().len() {
            return Err(std::io::Error::new(ErrorKind::Other, "Not written enough bytes and corrupted file"));
        }
        self.kv.insert(key.clone(), self.log_writer.pos - n as u64);
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.kv.get(&key) {
            None => Ok(None),
            Some (pos) => {
                let mut value = String::new();
                self.reader.seek(SeekFrom::Start(*pos))?;
                self.reader.read_line(&mut value)?;
                let record: Record = serde_json::from_str(&value)?;
                if record.cmd == Command::Remove {
                    Ok(None)
                } else {
                    Ok(Some(record.value))
                }
            }
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.kv.get(&key) {
            None => Err(std::io::Error::new(ErrorKind::Other, "Non existent key")),
            Some(_) => {
                let record = serde_json::to_string(&Record { cmd: Command::Remove, key: key.clone(), value: "".to_owned()})? + "\n";
                let n = self.log_writer.write(record.as_bytes())?;
                if n != record.as_bytes().len() {
                    return Err(std::io::Error::new(ErrorKind::Other, "Not written enough bytes and corrupted file"));
                }
                self.kv.remove(&key);
                Ok(())
            }
        }
    }

    
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let p: PathBuf = path.into().join("log");
        let mut kv = HashMap::<String, u64>::new();
        let f = std::fs::OpenOptions::new().read(true).append(true).create(true).truncate(false).open(&p)?;
        let writer = BufWriterWithPos::new(f)?;
        let mut reader = BufReaderWithPos::new(File::open(&p)?, 0)?;
        let mut pos: u64 = 0;
        let end = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(pos))?;
        while pos < end {
            let mut cmd = String::new();
            let x = reader.read_line(&mut cmd)?;
            let record: Record = serde_json::from_str(&cmd)?;
            match record.cmd {
                Command::Remove => {
                    kv.remove(&record.key);
                }
                Command::Set => {
                    kv.insert(record.key, pos);
                }
            }
            pos += x as u64;
        }
        Ok(KvStore { kv: kv, path: p, log_writer: writer, reader: reader })
    }
    
    fn compact(&mut self) -> Result<()> {
        let p: PathBuf = self.path.parent().unwrap().join("log.temp");
        let nf = std::fs::OpenOptions::new().append(true).create(true).open(&p)?;
        let mut writer = BufWriterWithPos::new(nf)?;
        let mut reader = BufReaderWithPos::new(File::open(&p)?, 0)?;
        let mut kv = HashMap::<String, u64>::new();
        for (k, pos) in self.kv.iter_mut() {
            let mut value = String::new();
            reader.seek(SeekFrom::Start(*pos))?;
            reader.read_line(&mut value)?;
            let record: Record = serde_json::from_str(&value)?;
            if record.cmd == Command::Set {
                let n = writer.write(value.as_bytes())?;
                kv.insert(k.to_owned(), writer.pos - n as u64);
                writer.flush()?;
            }
        }
        std::fs::rename(p, &self.path).expect("Error");
        self.kv = kv;
        Ok(())
    }
}

//impl Drop for KvStore {
//    fn drop(&mut self) {
//        self.compact().expect("Error compaction");
//    }
//}


#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl <R: Read + Seek> BufReaderWithPos<R> {
    fn new(inner: R, pos: u64) -> Result<BufReaderWithPos<R>> {
        let mut reader = BufReader::new(inner);
        let pos = reader.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos { reader, pos })
    }
}

impl <R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.seek(SeekFrom::Start(self.pos))?;
        let n = self.reader.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl <R: Read + Seek> BufRead for BufReaderWithPos<R> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt);
        self.pos += amt as u64;
    }

    fn read_line(&mut self, buf: &mut String) -> Result<usize> {
        self.reader.seek(SeekFrom::Start(self.pos))?;
        let n = self.reader.read_line(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl <R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl <W: Write + Seek> BufWriterWithPos<W> {
    fn new(inner: W) -> Result<BufWriterWithPos<W>> {
        let mut writer = BufWriter::new(inner);
        let pos = writer.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos { writer: writer, pos: pos })
    }
}

impl <W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // not safe for concurrency
        let n = self.writer.write(buf)?;
        self.pos = self.writer.seek(SeekFrom::Current(0))?;
        Ok(n)
    }

    fn flush(&mut self) -> Result<()>{
        self.writer.flush()
    }
}

impl <W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
