pub mod memtable;
pub mod reader;
pub mod version;
pub mod writer;

use assert_cmd::prelude::OutputAssertExt;
use memtable::MemTable;
use rand::AsByteSliceMut;
use reader::{LogReader, VLogReader};
use version::{Version, VersionEdit, VersionSet};
use writer::{LogWriter, VLogWriter};

use crate::engines::kv::version::{DBIterator, FileMetaData};
use crate::{engines::KVSError, KvsEngine, Result};
use self::version::Compaction;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;

const compact_threshold: u64 = 1 << 63;
const compact_memtable_threshold: u64 = 1024 * 1024;
const kL0_StopWritesTrigger: u64 = 12;
const kL0_CompactionTrigger: u64 = 4;

pub const NUM_LEVELS: i32 = 7;
pub const MAX_SEQUENCE_NUM: u64 = (1 << 56) - 1;
pub const MAX_MEM_COMPACT_LEVEL: i32 = 2;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct InternalKey {
    sequence_num: u64,
    user_key: String,
    command: Command,
}

impl InternalKey {
    pub fn new(user_key: &String, sequence_num: u64, command: Command) -> InternalKey {
        InternalKey {
            sequence_num,
            user_key: user_key.to_owned(),
            command,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.user_key == other.user_key {
            if self.sequence_num > other.sequence_num {
                Some(Ordering::Less)
            } else if self.sequence_num < other.sequence_num {
                Some(Ordering::Greater)
            } else {
                Some(Ordering::Equal)
            }
        } else if self.user_key < other.user_key {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Greater)
        }
    }
}

pub struct Options {
    max_file_size: usize,
    write_buffer_size: usize,
    max_open_files: i32,
    block_size: usize,
    block_restart_interval: i32,
    // block_cache: Option<Cache>,
    // create_if_missing: bool,
    // error_if_exists: bool,
    // paranoid_checks: bool,
    // compression: CompressionType

    // verify_checksums: bool,
    // fill_cache: bool,
    // snapshot: Option<Arc<Snapshot>>,

    // sync: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            max_file_size: 2 * 1024 * 1024,
            write_buffer_size: 4 * 1024 * 1024,
            max_open_files: 1000,
            block_size: 4 * 1024,
            block_restart_interval: 16,
        }
    }
}

const DEFAULT_OPTIONS: Options = Options {
    max_file_size: 2 * 1024 * 1024,
    write_buffer_size: 4 * 1024 * 1024,
    max_open_files: 1000,
    block_size: 4 * 1024,
    block_restart_interval: 16,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
enum Command {
    Set,
    Remove,
    Seek, // not sure
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    cmd: Command,
    key: String,
    value: String,
}

// for this point we don't run it concurrently but for convenience we still use Arc
// need to come up with a strategy of adding locks
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    // compact_daemon: Option<Arc<Mutex<thread::JoinHandle<()>>>>,
    mem: Arc<MemTable>,
    imm: Arc<Option<MemTable>>, // immutable

    log: Arc<LogWriter<File>>,
    vlog_writer: Arc<VLogWriter<File>>,
    vlog_reader: Arc<VLogReader<File>>,

    versions: Arc<VersionSet>,

    pending_outputs: HashSet<u64>,

    log_file_number: u64,
    // compation_scheduler: Arc<Mutex<CompactionScheduler>>,

    // background_work_finished_signal: Arc<Mutex<Condvar>>, // TODO: validate the implementation
}

// TODO: version control of log file
// TODO: consideration of lock poisoning
// TODO: make it concurrent
impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        // record construction
        let record = serde_json::to_string(&Record {
            cmd: Command::Set,
            key: key.clone(),
            value: value,
        })?;
        let buffer = record.as_bytes();

        self.write(buffer)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        // For now we don't consider snapshots
        // For concurrency: add Arc ref to self.mem and self.imm
        let ikey = InternalKey::new(&key, self.versions.last_sequence(), Command::Seek);
        if let Some((pos, n)) = self.mem.get(ikey).unwrap() {
            if pos == -1 {
                // deletion
                return Ok(None);
            } else {
                let value = self.vlog_reader.get_value(pos as u64, n)?;
                Ok(Some(value))
            }
        } else if self.imm.is_some()
            && self.imm.unwrap().get(ikey).unwrap().is_some()
        {
            let (pos, n) = self.imm.unwrap().get(ikey).unwrap().unwrap();
            if pos == -1 {
                // deletion
                return Ok(None);
            } else {
                let value = self.vlog_reader.get_value(pos as u64, n)?;
                Ok(Some(value))
            }
        } else {
            let current = self.versions.current;
            self.versions.last_sequence = 0;
            if let Some((pos, n)) = current.get(ikey).unwrap() {
                let value = self.vlog_reader.get_value(pos as u64, n)?;
                // Here we have stats update
                self.schedule_compaction();
                Ok(Some(value))
            } else {
                Ok(None)
            }
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let record = serde_json::to_string(&Record {
            cmd: Command::Remove,
            key: key.clone(),
            value: "".to_owned(),
        })?;
        let buffer = record.as_bytes();

        self.write(buffer)
    }
}

impl KvStore {
    fn _open(path: impl Into<PathBuf>) -> Result<KvStore> {
        unimplemented!()
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut store = Self::_open(path)?;
        Ok(store)
    }

    fn write(&self, buf: &[u8]) -> Result<()> {
        // For now it's simple implementation
        self.make_room_for_write(false);

        // 

        // write to log
        self.log.add_record(buf);

        // write to values
        self.vlog_writer.add_record(buf);

        // insert to memtable
        // self.mem.read().unwrap().insert(key.clone(), pos, n);

        Ok(())
    }
}

// Compaction
impl KvStore {
    fn compact(&self) {
        unimplemented!();
    }

    fn write_level0_table(
        &self,
        mem: &MemTable,
        edit: &mut VersionEdit,
        base: Option<&mut Version>,
    ) -> Result<()> {
        // mutex held
        let number = self.versions.new_file_number();
        self.pending_outputs.insert(number); // This is for concurrency
        debug!("Level 0 table {} compaction started", number);

        // build table
        let file_path = std::env::current_dir()?.join(make_file_name(number, "dbt"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_path)?;

        let mut iter = mem.iter();
        let mut first = true;
        let mut smallest: InternalKey;
        let mut largest: InternalKey;
        let mut key: InternalKey;
        // let mut buffer = Vec::<u8>::new();
        let mut writer = BufWriter::new(file);
        while let Some((k, v)) = iter.next() {
            if first {
                smallest = k.to_owned();
            }
            // We temporarily don't consider blocks and output buffer here
            let mut kstr = serde_json::to_string(k)?;
            let alen = kstr.as_bytes().len() + 8 + 8;
            writer.write(alen.to_le_bytes().as_slice());
            writer.write(kstr.as_bytes());
            writer.write(v.0.to_le_bytes().as_slice());
            writer.write(v.1.to_le_bytes().as_slice());
            largest = k.to_owned();
        }
        let file_size = writer.buffer().len() as u64;
        writer.flush()?;

        debug!(
            "Level 0 table {}: {file_size} bytes",
            file_path.to_str().unwrap()
        );
        self.pending_outputs.remove(&number);

        let mut level = 0;
        if file_size > 0 {
            if base.is_some() {
                level = base
                    .unwrap()
                    .pick_level_for_memtable_output(&smallest.user_key, &largest.user_key);
            }
            edit.add_file(level, number, file_size, smallest, largest)
        }

        // We don't consider stats here
        Ok(())
    }

    fn compact_memtable(&self) {
        // mutex held
        let mut edit = VersionEdit::new();
        let mut base = self.versions.current;

        // write_level0_table
        assert!(self.imm.is_some());
        // TODO: validate the implementation here
        let mut res = self.write_level0_table(
            &self.imm.unwrap(),
            &mut edit,
            Some(base.borrow_mut()),
        );

        if res.is_ok() {
            // Deal with logs
            edit.prev_log_number = Some(0); // TODO: why?
            edit.next_log_number = Some(self.log_file_number);
            res = self.versions.log_and_apply(edit);
        }

        if res.is_ok() {
            *self.imm = None;
            // TODO: store false in has_imm
            // TODO: remove obsolete files
        } else {
            // TODO: record background error
        }
    }

    fn schedule_compaction(&self) {
        self.background_compaction();
    }

    fn make_room_for_write(&self, force: bool) -> Result<()> {
        // We don't consider allow_latency sort of things
        loop {
            if !force && self.mem.size() <= compact_memtable_threshold {
                break Ok(());
            } else if self.imm.is_some() {
                info!("Current memtable full, waiting...");
            } else if self.versions.current_num_level_files(0) >= kL0_StopWritesTrigger {
                // TODO: validate this implementation
                info!("Too many files in level 0 files, waiting...");
            }
            let new_log_number = self.versions.new_file_number();
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(log_file_path(new_log_number));
            match file {
                Err(e) => {
                    warn!("{e}");
                    break Err(e);
                }
                Ok(file) => {
                    // TODO: validate this implementation
                    self.schedule_compaction();
                }
            }
            // }
        }
    }

    fn open_compaction_output_file(compact_state: &mut CompactionState) -> Result<()> {
        let mut file_number;
    }

    fn finish_compaction_output_file() -> Result<()> {

    }

    fn background_compaction(&self) {
        // mutex held

        if self.imm.is_some() {
            self.compact_memtable();
            return;
        }

        // We don't consider manual compaction at this moment
        // TODO: change c into Arc<Compaction>
        let mut c = self.versions.pick_compaction();

        match c {
            None => {}
            Some(mut c) => {
                if c.is_trivial_move() {
                    let f = c.input(0, 0);
                    c.edit.remove_file(c.level, f.num as u64);
                    c.edit.add_file(c.level + 1, f);
                    match self.versions.log_and_apply(c.edit) {
                        _ => {} // TODO: record background error
                    }
                    debug!("Moved {} to level {} {} byutes", f.num, c.level + 1, f.size);
                } else {
                    // do compaction work
                    debug!(
                        "Compacting {} in {} + {} in {} files",
                        c.num_input_files(0).unwrap(),
                        c.level,
                        c.num_input_files(1).unwrap(),
                        c.level + 1
                    );
                    // TODO: update smallest snapshot (snapshot system)

                    // release mutex when actually doing the compaction work
                    // TODO: iterator
                    let mut iterator: DBIterator;
                    let mut compact_state = CompactionState::new(&mut c);
                    let mut have_current_user_key = false;
                    let mut current_user_key: String;
                    let mut last_sequence_for_key = MAX_SEQUENCE_NUM;
                    while let Some((key, value)) = iterator.next() {
                        if self.imm.is_some() {
                            self.compact_memtable();
                        }

                        // TODO: builder isn't null (what is the builder?)
                        if compact_state.compaction.should_stop_before(key) {
                            // finish compaction output file
                            if self
                                .finish_compaction_output_file(compact_state, &mut iterator)
                                .is_err()
                            {
                                break;
                            }
                        }

                        let mut drop = false;
                        // TODO: We don't consider serde json error atm
                        if !have_current_user_key || key.user_key != current_user_key {
                            current_user_key = key.user_key.clone();
                            have_current_user_key = true;
                            last_sequence_for_key = MAX_SEQUENCE_NUM;
                        }

                        // if last_sequence_for_key <= compact_state.smallest snapshot then drop
                        if key.command == Command::Remove /* && key.sequence_num <= compact_state.smallest_snapshot */ && compact_state.compaction.is_base_level_for_key(key.user_key)
                        {
                            drop = true;
                        }

                        last_sequence_for_key = key.sequence_num;

                        if !drop {
                            // if compact_state.builder == nullptr
                        }
                    }

                    // clean up compaction
                    // release inputs
                    // remove obsolete files
                }
            }
        }
    }
}

// for concurrency
pub struct CompactionScheduler {
    background_compaction_scheduled: bool,
    background_work_finished_signal: Condvar,
}

impl CompactionScheduler {
    pub fn new() -> CompactionScheduler {
        CompactionScheduler {
            background_compaction_scheduled: false,
            background_work_finished_signal: Condvar::new(),
        }
    }

    pub fn maybe_schedule_compaction(&self) {}
}

fn make_file_name(number: u64, label: &str) -> String {
    format!("{number}.{label}")
}

struct CompactionState {
    compaction: &'static mut Compaction,
    // smallest_snapshot: u64, // will never serve a snapshot below smallest_snapshot
    outputs: Vec<FileMetaData>,
    outfile: Option<File>,
    total_bytes: u64,
}

impl CompactionState {
    pub fn new(compaction: &mut Compaction) -> CompactionState {
        CompactionState {
            compaction,
            outputs: Vec::new(),
            outfile: None,
            total_bytes: 0,
        }
    }
}
