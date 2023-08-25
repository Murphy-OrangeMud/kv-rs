use core::iter::Iterator;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek};
use std::ops::Index;
use std::rc::Rc;
use std::sync::Arc;
use std::{default, string};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::engines::kv::{MAX_MEM_COMPACT_LEVEL, MAX_SEQUENCE_NUM, NUM_LEVELS};
use crate::Result;

use super::{
    default_options, kL0_CompactionTrigger, make_file_name, InternalKey, KvStore, Options,
};

#[derive(Serialize, Deserialize, Eq)]
pub struct FileMetaData {
    pub num: i32,
    pub size: u64,
    pub refs: u64,
    pub smallest_key: InternalKey,
    pub largest_key: InternalKey,
}

impl FileMetaData {
    fn cmp_by_smallest(&self, other: &FileMetaData) -> Option<Ordering> {
        if self.smallest_key == other.smallest_key {
            return self.num.partial_cmp(&other.num);
        }
        return self.smallest_key.partial_cmp(&other.smallest_key);
    }
}

impl PartialEq for FileMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.num == other.num
    }
}

impl PartialOrd for FileMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // default comparator
        self.cmp_by_smallest(other)
    }
}

impl Ord for FileMetaData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone)]
pub struct VersionSet {
    db: &'static KvStore,
    pub current: Arc<Version>,

    pub last_sequence: u64,
    pub versions: Vec<Arc<Version>>,

    log_number: u64,
    prev_log_number: u64,
    next_file_number: u64,

    compact_pointer: [InternalKey; NUM_LEVELS as usize],
}

impl VersionSet {
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn log_and_apply(&self, mut edit: VersionEdit) -> Result<()> {
        if edit.log_number.is_none() {
            edit.log_number = Some(self.log_number);
        }

        if edit.prev_log_number.is_none() {
            edit.prev_log_number = Some(self.prev_log_number);
        }

        edit.next_file_number = Some(self.next_file_number);
        edit.last_sequence = Some(self.last_sequence);

        // apply edit to self
        // update compaction pointers
        for i in 0..edit.compact_pointers.len() {
            self.compact_pointer[edit.compact_pointers[i].0 as usize] = edit.compact_pointers[i].1;
        }

        let mut level_added_files: [BTreeSet<FileMetaData>; NUM_LEVELS as usize] =
            Default::default();
        let mut level_deleted_files: [HashSet<i32>; NUM_LEVELS as usize] = Default::default();
        // delete files
        for deleted_file_set_kvp in edit.deleted_files {
            level_deleted_files[deleted_file_set_kvp.0 as usize]
                .insert(deleted_file_set_kvp.1 as i32);
        }
        // add new files
        for added_file_set_kvp in edit.new_files {
            if !level_deleted_files[added_file_set_kvp.0 as usize]
                .remove(&(added_file_set_kvp.1.num as i32))
            {
                level_added_files[added_file_set_kvp.0 as usize].insert(FileMetaData {
                    num: added_file_set_kvp.1.num,
                    size: added_file_set_kvp.1.size,
                    refs: added_file_set_kvp.1.refs,
                    smallest_key: added_file_set_kvp.1.smallest_key,
                    largest_key: added_file_set_kvp.1.largest_key,
                });
            }
        }

        // let mut base = self.current.as_ref();
        let mut v = Version {
            files: Default::default(),
            vset: Arc::new(self.to_owned()),
            compaction_score: -1.0,
            compaction_level: -1,
            file_to_compact: None,
        };

        for level in 0..NUM_LEVELS {
            let base_files = self.current.files[level as usize];
            let idx = 0 as usize;
            for added_file in level_added_files[level as usize] {
                // add all smaller files listed in base_
                let bpos = base_files
                    .binary_search_by(|element| {
                        match element.as_ref().cmp_by_smallest(&added_file).unwrap() {
                            Ordering::Equal => Ordering::Less,
                            ord => ord,
                        }
                    })
                    .unwrap_err();
                while idx < bpos {
                    if !level_deleted_files[level as usize].contains(&base_files[idx].num) {
                        let mut files = &mut v.files[level as usize];
                        if level > 0 && !files.is_empty() {
                            // Must not overlap
                            assert!(files[files.len() - 1].largest_key < base_files[idx].smallest_key)
                        }
                        files.push(base_files[idx]);
                    }
                }
                if !level_deleted_files[level as usize].contains(&added_file.num) {
                    let mut files = &mut v.files[level as usize];
                    if level > 0 && !files.is_empty() {
                        // Must not overlap
                        assert!(files[files.len() - 1].largest_key < added_file.smallest_key)
                    }
                    files.push(Arc::new(added_file));
                }
            }
        }

        // Why calculating in this way?
        let mut best_level = -1;
        let mut best_score: f64 = -1.0;
        for level in 0..NUM_LEVELS - 1 {
            let mut score: f64;
            if level == 0 {
                score = v.files[level as usize].len() as f64 / kL0_CompactionTrigger as f64;
            } else {
                score = total_file_size(&v.files[level as usize]) as f64
                    / max_bytes_for_level(level) as f64;
            }

            if score > best_score {
                best_level = level;
                best_score = score;
            }
        }

        v.compaction_level = best_level;
        v.compaction_score = best_score;

        Ok(())
    }

    fn append_version(&mut self, v: Version) {
        self.current = Arc::new(v);
    }

    pub fn current_num_level_files(&self) -> u64 {
        unimplemented!();
    }

    pub fn new_file_number(&self) -> u64 {
        unimplemented!();
    }

    pub fn pick_compaction(&self) -> Option<Compaction> {
        // let mut c = Compaction::new();
        let size_compaction = self.current.compaction_score >= 1.0;
        let seek_compaction = self.current.file_to_compact.is_some();
        let mut level: i32;
        let mut c: Compaction;

        if size_compaction {
            level = self.current.compaction_level;
            c = Compaction::new(level);

            // TODO: What is compact pointer?
            // TODO: What is c.inputs
            for i in 0..self.current.files[level as usize].len() {
                if self.compact_pointer[level as usize] == Default::default()
                    || self.compact_pointer[level as usize]
                        < self.current.files[level as usize][i].largest_key
                {
                    c.inputs[0].push(self.current.files[level as usize][i]);
                    break;
                }
            }
            if c.inputs[0].is_empty() {
                c.inputs[0].push(self.current.files[level as usize][0]);
            }
        } else if seek_compaction {
            // TODO: validate this implementation
            level = self.current.file_to_compact_level;
            c = Compaction::new(level);
            c.inputs[0].push(Arc::new(self.current.file_to_compact.unwrap()));
        }
        unimplemented!()
    }

    pub fn get(&self, meta: &FileMetaData, key: &InternalKey) -> Result<Option<(i64, usize)>> {
        let file_name = make_file_name(meta.num, "dbt");

        let file = File::open(self.db.path.join(file_name))?;
        let mut reader = BufReader::new(&file);
        let mut buffer = Vec::<u8>::new();
        let resn = reader.read_to_end(&mut buffer)?;
        if resn < file.metadata().unwrap().len() as usize {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "File corrupted, not read enough bytes",
            ));
        }

        let mut pos = 0;
        loop {
            let key_len = usize::from_le_bytes(buffer[pos..8 + pos].try_into().unwrap());
            let key_buf = &buffer[8 + pos..8 + pos + key_len];
            let key_str = std::str::from_utf8(key_buf).unwrap().to_string();
            let ikey: InternalKey = serde_json::from_str(&key_str)?;

            if &ikey < key {
                pos += (8 + key_len + 8 + 8) as usize;
            } else if &ikey == key {
                let vpos = i64::from_le_bytes(
                    buffer[8 + pos + key_len..8 + pos + key_len + 8]
                        .try_into()
                        .unwrap(),
                );
                let vlen = usize::from_le_bytes(
                    buffer[8 + pos + key_len + 8..8 + pos + key_len + 16]
                        .try_into()
                        .unwrap(),
                );
                return Ok(Some((vpos, vlen)));
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct Version {
    files: [Vec<Arc<FileMetaData>>; NUM_LEVELS as usize],
    vset: Arc<VersionSet>,

    compaction_score: f64,
    compaction_level: i32,
    file_to_compact: Option<File>,
}

impl Version {
    pub fn get(&self, key: InternalKey) -> Result<Option<(i64, usize)>> {
        // search level 0
        let mut tmp = Vec::<Arc<FileMetaData>>::new();
        for i in 0..self.files[0].len() {
            if key.user_key >= self.files[0][i].smallest_key.user_key
                && key.user_key <= self.files[0][i].largest_key.user_key
            {
                tmp.push(Arc::clone(&self.files[0][i]));
            }
        }
        if !tmp.is_empty() {
            tmp.sort_by(|a, b| {
                if a.num > b.num {
                    Ordering::Less
                } else if a.num == b.num {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            });
            // We don't consider table cache at this moment
            // if record not match, return
            for i in 0..tmp.len() {
                match self.vset.get(&tmp[i], &key) {
                    Ok(Some(tuple)) => return Ok(Some(tuple)),
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        for level in 1..NUM_LEVELS {
            if self.files[level as usize].is_empty() {
                continue;
            }
            let idx = find_file(&self.files[level as usize], &key);
            if idx < self.files[level as usize].len() {
                if key.user_key >= self.files[level as usize][idx].smallest_key.user_key {
                    match self.vset.get(&self.files[level as usize][idx], &key) {
                        Ok(Some(tuple)) => return Ok(Some(tuple)),
                        Ok(None) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn overlap_in_level(
        &self,
        level: i32,
        smallest_user_key: &String,
        largest_user_key: &String,
    ) -> bool {
        if level == 0 {
            for i in 0..self.files[level as usize].len() {
                if !(smallest_user_key > &self.files[level as usize][i].largest_key.user_key
                    || largest_user_key < &self.files[level as usize][i].smallest_key.user_key)
                {
                    return true;
                }
            }
            false
        } else {
            let idx = find_file(
                &self.files[level as usize],
                &InternalKey {
                    sequence_num: MAX_SEQUENCE_NUM,
                    user_key: smallest_user_key.to_owned(),
                },
            );
            if idx >= self.files[level as usize].len() {
                false
            } else {
                largest_user_key >= &self.files[level as usize][idx].smallest_key.user_key
            }
        }
    }

    pub fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &String,
        largest_user_key: &String,
    ) -> i32 {
        let mut level = 0;
        if !self.overlap_in_level(0, smallest_user_key, largest_user_key) {
            let start = InternalKey::new(smallest_user_key, MAX_SEQUENCE_NUM);
            let limit = InternalKey::new(largest_user_key, 0);
            let mut overlaps = Vec::<Arc<FileMetaData>>::new();
            while level < MAX_MEM_COMPACT_LEVEL {
                if self.overlap_in_level(level + 1, smallest_user_key, largest_user_key) {
                    break;
                }
                if level + 2 < NUM_LEVELS {
                    self.get_overlap_inputs(level + 2, &start, &limit, &mut overlaps);
                    if total_file_size(&overlaps) > max_grandparent_overlap_bytes(default_options) {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    pub fn get_overlap_inputs(
        &self,
        level: i32,
        begin: &InternalKey,
        end: &InternalKey,
        inputs: &mut Vec<Arc<FileMetaData>>,
    ) {
        inputs.clear();
        let mut user_begin = &begin.user_key;
        let mut user_end = &end.user_key;
        let mut i = 0;
        while i < self.files[level as usize].len() {
            let file_begin = &self.files[level as usize][i].smallest_key.user_key;
            let file_end = &self.files[level as usize][i].largest_key.user_key;
            if !(file_end < user_begin || file_begin > user_end) {
                inputs.push(self.files[level as usize][i].clone());
                if level == 0 {
                    if file_begin < user_begin {
                        user_begin = file_begin;
                        inputs.clear();
                        i = 0;
                    } else if file_end > user_end {
                        user_end = file_end;
                        inputs.clear();
                        i = 0;
                    }
                }
            }
            i += 1;
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct VersionEdit {
    new_files: Vec<(i32, Arc<FileMetaData>)>,
    deleted_files: BTreeSet<(i32, u64)>, // file nums
    compact_pointers: Vec<(i32, InternalKey)>,

    log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_log_number: Option<u64>,
    last_sequence: Option<u64>,

    next_file_number: Option<u64>,
}

impl VersionEdit {
    pub fn new() -> VersionEdit {
        VersionEdit {
            new_files: Vec::new(),
            deleted_files: BTreeSet::new(),
            compact_pointers: Vec::new(),
            log_number: None,
            prev_log_number: None,
            next_log_number: None,
            last_sequence: None,
            next_file_number: None,
        }
    }

    pub fn add_file(&mut self, level: i32, f: Arc<FileMetaData>) {
        self.new_files.push((level, Arc::clone(&f)));
    }

    pub fn remove_file(&mut self, level: i32, num: u64) {
        self.deleted_files.insert((level, num));
    }
}

pub struct Compaction {
    pub level: i32,        // the level that is being compacted
    pub edit: VersionEdit, // the object that holds the edits to the descriptor done by this compaction
    version: Option<Arc<Version>>,

    // this: level; parent: level + 1; grandparent: level + 2;
    inputs: [Vec<Arc<FileMetaData>>; 3],
    grandparent_idx: usize,
    seen_key: bool,
    overlapped_bytes: i64,
    max_output_file_size: u64,

    level_ptr: [usize; NUM_LEVELS as usize],
}

impl Compaction {
    pub fn new(level: i32) -> Compaction {
        let inputs = [
            Vec::<Arc<FileMetaData>>::new(),
            Vec::<Arc<FileMetaData>>::new(),
            Vec::<Arc<FileMetaData>>::new(),
        ];
        let level_ptr = [0; NUM_LEVELS as usize];

        Compaction {
            level,
            edit: VersionEdit::new(),
            version: None,
            inputs,
            grandparent_idx: 0,
            seen_key: false,
            overlapped_bytes: 0,
            max_output_file_size: default_options.max_file_size as u64,
            level_ptr: [0; NUM_LEVELS as usize],
        }
    }

    pub fn num_input_Files(&self, which: i32) -> Result<usize> {
        if which >= 3 {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Which must be 0 or 1",
            ))
        } else {
            Ok(self.inputs[which as usize].len())
        }
    }

    pub fn input(&self, which: i32, i: i32) -> Arc<FileMetaData> {
        Arc::clone(&self.inputs[which as usize][i as usize])
    }

    pub fn max_output_file_size(&self) -> u64 {
        self.max_output_file_size
    }

    pub fn is_trivial_move(&self) -> bool {
        // let vset = self.version.as_ref().unwrap().vset.as_ref();
        self.num_input_Files(0).unwrap() == 1
            && self.num_input_Files(1).unwrap() == 0
            && total_file_size(&self.inputs[2]) <= max_grandparent_overlap_bytes(default_options)
    }

    pub fn add_input_deletions(&mut self) {
        for which in 0..2 {
            for i in 0..self.inputs[which as usize].len() {
                self.edit
                    .remove_file(self.level + which, self.inputs[which as usize][i].num);
            }
        }
    }

    pub fn is_base_level_for_key(&mut self, key: &String) -> bool {
        // TODO adapt it to binary search
        for lvl in (self.level + 2)..NUM_LEVELS {
            while self.level_ptr[lvl as usize]
                < self.version.as_ref().unwrap().files[lvl as usize].len()
            {
                if key.to_owned()
                    < self.version.as_ref().unwrap().files[lvl as usize]
                        [self.level_ptr[lvl as usize]]
                        .largest_key
                        .user_key
                {
                    if key.to_owned()
                        >= self.version.as_ref().unwrap().files[lvl as usize]
                            [self.level_ptr[lvl as usize]]
                            .smallest_key
                            .user_key
                    {
                        return false;
                    }
                }
            }
            self.level_ptr[lvl as usize] += 1;
        }
        true
    }

    pub fn should_stop_before(&mut self, key: &InternalKey) -> bool {
        while self.grandparent_idx < self.inputs[2].len()
            && key > &self.inputs[2][self.grandparent_idx].largest_key
        {
            if self.seen_key {
                self.overlapped_bytes += self.inputs[2][self.grandparent_idx].size as i64;
            }
            self.grandparent_idx += 1;
        }
        self.seen_key = true;

        if self.overlapped_bytes > max_grandparent_overlap_bytes(default_options) {
            self.overlapped_bytes = 0;
            true
        } else {
            false
        }
    }

    pub fn release_version(&mut self) {
        self.version = None;
    }
}

fn find_file(files: &Vec<Arc<FileMetaData>>, key: &InternalKey) -> usize {
    let mut l = 0 as usize;
    let mut r = files.len();
    while l < r {
        let mid = (l + r) / 2;
        if key > &files[mid].largest_key {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    r
}

fn total_file_size(files: &Vec<Arc<FileMetaData>>) -> i64 {
    let mut sum: i64 = 0;
    for file in files {
        sum += file.size as i64;
    }
    sum
}

fn max_grandparent_overlap_bytes(options: Options) -> i64 {
    options.max_file_size as i64 * 10
}

fn max_bytes_for_level(level: i32) -> u64 {
    let mut result = 10.0 * 1048576.0;
    let mut level = level;
    while level > 1 {
        result *= 10.0;
        level -= 1;
    }
    result as u64
}

pub struct DBIterator {}

impl Iterator for DBIterator {
    type Item = string;
    fn next(&mut self) -> Option<Self::Item> {}
}
