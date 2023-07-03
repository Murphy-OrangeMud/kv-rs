use crate::{KvsEngine, Result};
use dashmap::DashMap;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread;

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

#[derive(Clone)]
pub struct KvStore {
    kv: Arc<DashMap<String, u64>>,
    path: Arc<PathBuf>,
    log_writer: Arc<Mutex<BufWriterWithPos<File>>>,
    reader: Arc<Mutex<BufReaderWithPos<File>>>,
    // compact_daemon: Arc<Mutex<thread::JoinHandle<()>>>,
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let record = serde_json::to_string(&Record {
            cmd: Command::Set,
            key: key.clone(),
            value: value,
        })? + "\n";
        let mut guard = self.log_writer.lock().unwrap();
        let n = guard.write(record.as_bytes())?;
        let pos = guard.pos - n as u64;
        guard.flush()?;
        drop(guard);
        if n != record.as_bytes().len() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Not written enough bytes and corrupted file",
            ));
        }
        if self.kv.contains_key(&key) {
            self.kv.remove(&key);
        }
        self.kv.insert(key.clone(), pos);
        debug!("Inserted: key: {key}, value: {pos}");
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.kv.get(&key).as_deref() {
            None => Ok(None),
            Some(pos) => {
                let mut value = String::new();
                let mut guard = self.reader.lock().unwrap();
                guard.seek(SeekFrom::Start(*pos))?;
                guard.read_line(&mut value)?;
                drop(guard);
                let record: Record = serde_json::from_str(&value)?;
                if record.cmd == Command::Remove {
                    Ok(None)
                } else {
                    Ok(Some(record.value))
                }
            }
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if self.kv.contains_key(&key) {
            let record = serde_json::to_string(&Record {
                cmd: Command::Remove,
                key: key.clone(),
                value: "".to_owned(),
            })? + "\n";
            let mut guard = self.log_writer.lock().unwrap();
            let n = guard.write(record.as_bytes())?;
            guard.flush()?;
            drop(guard);
            if n != record.as_bytes().len() {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Not written enough bytes and corrupted file",
                ));
            }
            self.kv.remove(&key);
            Ok(())
        } else {
            Err(std::io::Error::new(ErrorKind::Other, "Non existent key"))
        }
    }
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let p: PathBuf = path.into().join("log");
        let mut kv = DashMap::<String, u64>::new();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .open(&p)?;
        let writer = BufWriterWithPos::new(f)?;
        let mut reader = BufReader::new(File::open(&p)?);
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


        
        Ok(KvStore {
            kv: Arc::new(kv),
            path: Arc::new(p),
            log_writer: Arc::new(Mutex::new(writer)),
            reader: Arc::new(Mutex::new(BufReaderWithPos {
                reader: reader,
                pos: 0,
            })),
            // compact_daemon: Arc::new(Mutex::new(thread::spawn(move||{})))
        })
    }

    fn compact(&mut self) {
        let p: PathBuf = self.path.parent().unwrap().join("log.temp");
        let nf = std::fs::OpenOptions::new().append(true).create(true).open(&p).unwrap();
        let mut writer = BufWriterWithPos::new(nf).unwrap();
        let mut reader = BufReaderWithPos::new(File::open(&p).unwrap(), 0).unwrap();
        let mut kv = DashMap::<String, u64>::new();
        for tuple in self.kv.iter_mut() {
            let k = tuple.key();
            let pos = tuple.value();
            let mut value = String::new();
            reader.seek(SeekFrom::Start(*pos)).unwrap();
            reader.read_line(&mut value);
            let record: Record = serde_json::from_str(&value).unwrap();
            if record.cmd == Command::Set {
                let n = writer.write(value.as_bytes()).unwrap();
                kv.insert(k.to_owned(), writer.pos - n as u64);
                writer.flush();
            }
        }
        std::fs::rename(p, self.path.as_ref()).expect("Error");
        self.kv = Arc::new(kv);
    }
}

/* impl Drop for KvStore {
    fn drop(&mut self) {

    }
} */

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

impl<R: Read + Seek> BufRead for BufReaderWithPos<R> {
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

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
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

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(inner: W) -> Result<BufWriterWithPos<W>> {
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
