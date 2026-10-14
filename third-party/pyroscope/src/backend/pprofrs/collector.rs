// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::backend::pprofrs::frames::UnresolvedFrames;

use aligned_vec::AVec;
use tempfile::NamedTempFile;

pub const BUCKETS: usize = 1 << 12;
pub const BUCKETS_ASSOCIATIVITY: usize = 4;
pub const BUFFER_LENGTH: usize = (1 << 18) / std::mem::size_of::<Entry<UnresolvedFrames>>();

#[derive(Debug)]
pub struct Entry<T> {
    pub item: T,
    pub count: isize,
}

impl<T: Default> Default for Entry<T> {
    fn default() -> Self {
        Entry {
            item: Default::default(),
            count: 0,
        }
    }
}

#[derive(Debug)]
pub struct Bucket<T: 'static> {
    pub length: usize,
    entries: Box<[Entry<T>; BUCKETS_ASSOCIATIVITY]>,
}

impl<T: Eq + Default> Default for Bucket<T> {
    fn default() -> Bucket<T> {
        let entries = Box::default();

        Self { length: 0, entries }
    }
}

impl<T: Eq> Bucket<T> {
    pub fn clear(&mut self) {
        self.length = 0;
    }

    pub fn add(&mut self, key: T, count: isize) -> Option<Entry<T>> {
        let mut done = false;
        self.entries[0..self.length].iter_mut().for_each(|ele| {
            if ele.item == key {
                ele.count += count;
                done = true;
            }
        });

        if done {
            None
        } else if self.length < BUCKETS_ASSOCIATIVITY {
            let ele = &mut self.entries[self.length];
            ele.item = key;
            ele.count = count;

            self.length += 1;
            None
        } else {
            let mut min_index = 0;
            let mut min_count = self.entries[0].count;
            for index in 0..self.length {
                let count = self.entries[index].count;
                if count < min_count {
                    min_index = index;
                    min_count = count;
                }
            }

            let mut new_entry = Entry { item: key, count };
            std::mem::swap(&mut self.entries[min_index], &mut new_entry);
            Some(new_entry)
        }
    }

    pub fn iter(&self) -> BucketIterator<'_, T> {
        BucketIterator::<T> {
            related_bucket: self,
            index: 0,
        }
    }
}

pub struct BucketIterator<'a, T: 'static> {
    related_bucket: &'a Bucket<T>,
    index: usize,
}

impl<'a, T> Iterator for BucketIterator<'a, T> {
    type Item = &'a Entry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.related_bucket.length {
            self.index += 1;
            Some(&self.related_bucket.entries[self.index - 1])
        } else {
            None
        }
    }
}

pub struct HashCounter<T: Hash + Eq + 'static> {
    buckets: Box<[Bucket<T>; BUCKETS]>,
}

impl<T: Hash + Eq + Default + Debug> Default for HashCounter<T> {
    fn default() -> Self {
        let mut v: Vec<Bucket<T>> = Vec::with_capacity(BUCKETS);
        v.resize_with(BUCKETS, Default::default);
        let buckets = v.into_boxed_slice().try_into().unwrap();

        Self { buckets }
    }
}

impl<T: Hash + Eq> HashCounter<T> {
    pub fn clear(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.clear();
        }
    }

    fn hash(key: &T) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    pub fn add(&mut self, key: T, count: isize) -> Option<Entry<T>> {
        let hash_value = Self::hash(&key);
        let bucket = &mut self.buckets[(hash_value % BUCKETS as u64) as usize];

        bucket.add(key, count)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry<T>> {
        self.buckets.iter().flat_map(|bucket| bucket.iter())
    }
}

pub struct TempFdArray<T: 'static> {
    file: NamedTempFile,
    buffer: Box<[T; BUFFER_LENGTH]>,
    buffer_index: usize,
    flush_n: usize,
}

impl<T: Default + Debug> TempFdArray<T> {
    fn new() -> std::io::Result<TempFdArray<T>> {
        let file = NamedTempFile::new()?;

        let mut v: Vec<T> = Vec::with_capacity(BUFFER_LENGTH);
        v.resize_with(BUFFER_LENGTH, Default::default);
        let buffer = v.into_boxed_slice().try_into().unwrap();

        Ok(Self {
            file,
            buffer,
            buffer_index: 0,
            flush_n: 0,
        })
    }
}

impl<T> TempFdArray<T> {
    fn clear(&mut self) -> std::io::Result<()> {
        self.buffer_index = 0;
        self.flush_n = 0;
        self.file.as_file().set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    fn flush_buffer(&mut self) -> std::io::Result<()> {
        self.buffer_index = 0;
        let buf = unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const u8,
                BUFFER_LENGTH * std::mem::size_of::<T>(),
            )
        };
        self.flush_n += 1;
        self.file.write_all(buf)?;

        Ok(())
    }

    fn push(&mut self, entry: T) -> std::io::Result<()> {
        if self.buffer_index >= BUFFER_LENGTH {
            self.flush_buffer()?;
        }

        self.buffer[self.buffer_index] = entry;
        self.buffer_index += 1;

        Ok(())
    }

    fn try_iter(&self) -> std::io::Result<impl Iterator<Item = &T>> {
        let size = BUFFER_LENGTH * self.flush_n * std::mem::size_of::<T>();

        let mut file_vec = AVec::with_capacity(std::mem::align_of::<T>(), size);
        let mut file = self.file.reopen()?;

        unsafe {
            // it's safe as the capacity is initialized to `size`, and it'll be filled with `size` bytes
            file_vec.set_len(size);
        }
        file.read_exact(&mut file_vec[0..size])?;
        file.seek(SeekFrom::End(0))?;

        Ok(TempFdArrayIterator {
            buffer: &self.buffer[0..self.buffer_index],
            file_vec,
            index: 0,
        })
    }
}

pub struct TempFdArrayIterator<'a, T> {
    pub buffer: &'a [T],
    pub file_vec: AVec<u8>,
    pub index: usize,
}

impl<'a, T> Iterator for TempFdArrayIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.buffer.len() {
            self.index += 1;
            Some(&self.buffer[self.index - 1])
        } else {
            let length = self.file_vec.len() / std::mem::size_of::<T>();
            let ts =
                unsafe { std::slice::from_raw_parts(self.file_vec.as_ptr() as *const T, length) };
            if self.index - self.buffer.len() < ts.len() {
                self.index += 1;
                Some(&ts[self.index - self.buffer.len() - 1])
            } else {
                None
            }
        }
    }
}

pub struct Collector<T: Hash + Eq + 'static> {
    map: HashCounter<T>,
    temp_array: TempFdArray<Entry<T>>,
}

impl<T: Hash + Eq + Default + Debug + 'static> Collector<T> {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            map: HashCounter::<T>::default(),
            temp_array: TempFdArray::<Entry<T>>::new()?,
        })
    }
}

impl<T: Hash + Eq + 'static> Collector<T> {
    pub fn clear(&mut self) -> std::io::Result<()> {
        self.map.clear();
        self.temp_array.clear()?;
        Ok(())
    }

    pub fn add(&mut self, key: T, count: isize) -> std::io::Result<()> {
        if let Some(evict) = self.map.add(key, count) {
            self.temp_array.push(evict)?;
        }

        Ok(())
    }

    pub fn try_iter(&self) -> std::io::Result<impl Iterator<Item = &Entry<T>>> {
        Ok(self.map.iter().chain(self.temp_array.try_iter()?))
    }

    #[cfg(test)]
    pub fn flushed_to_disk(&self) -> usize {
        self.temp_array.flush_n
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use std::collections::BTreeMap;

    pub fn add_map<T: std::cmp::Ord + Copy>(hashmap: &mut BTreeMap<T, isize>, entry: &Entry<T>) {
        match hashmap.get_mut(&entry.item) {
            None => {
                hashmap.insert(entry.item, entry.count);
            }
            Some(count) => *count += entry.count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn stack_hash_counter() {
        let mut stack_hash_counter = HashCounter::<usize>::default();
        stack_hash_counter.add(0, 1);
        stack_hash_counter.add(1, 1);
        stack_hash_counter.add(1, 1);

        stack_hash_counter.iter().for_each(|item| {
            if item.item == 0 {
                assert_eq!(item.count, 1);
            } else if item.item == 1 {
                assert_eq!(item.count, 2);
            } else {
                unreachable!();
            }
        });
    }

    #[test]
    fn evict_test() {
        let mut stack_hash_counter = HashCounter::<usize>::default();
        let mut real_map = BTreeMap::new();

        for item in 0..(1 << 10) * 4 {
            for _ in 0..(item % 4) {
                match stack_hash_counter.add(item, 1) {
                    None => {}
                    Some(evict) => {
                        test_utils::add_map(&mut real_map, &evict);
                    }
                }
            }
        }

        stack_hash_counter.iter().for_each(|entry| {
            test_utils::add_map(&mut real_map, entry);
        });

        for item in 0..(1 << 10) * 4 {
            let count = (item % 4) as isize;
            match real_map.get(&item) {
                Some(item) => {
                    assert_eq!(*item, count);
                }
                None => {
                    assert_eq!(count, 0);
                }
            }
        }
    }

    #[test]
    fn collector_test() {
        let mut collector = Collector::new().unwrap();
        let mut real_map = BTreeMap::new();

        for item in 0..(1 << 12) * 4 {
            for _ in 0..(item % 4) {
                collector.add(item, 1).unwrap();
            }
        }

        collector.try_iter().unwrap().for_each(|entry| {
            test_utils::add_map(&mut real_map, entry);
        });

        for item in 0..(1 << 12) * 4 {
            let count = (item % 4) as isize;
            match real_map.get(&item) {
                Some(value) => {
                    assert_eq!(count, *value);
                }
                None => {
                    assert_eq!(count, 0);
                }
            }
        }
    }

    #[derive(Debug, Hash, Eq, PartialEq, PartialOrd, Ord, Default, Clone, Copy)]
    struct AlignTest {
        a: u16,
        b: u32,
        c: u64,
        d: u64,
    }

    // collector_align_test uses a bigger item to test the alignment of the collector
    #[test]
    fn collector_align_test() {
        let mut collector = Collector::new().unwrap();
        let mut real_map = BTreeMap::new();

        for item in 0..(1 << 12) * 4 {
            for _ in 0..(item % 4) {
                collector
                    .add(
                        AlignTest {
                            a: item as u16,
                            b: item as u32,
                            c: item as u64,
                            d: item as u64,
                        },
                        1,
                    )
                    .unwrap();
            }
        }

        collector.try_iter().unwrap().for_each(|entry| {
            test_utils::add_map(&mut real_map, entry);
        });

        for item in 0..(1 << 12) * 4 {
            let count = (item % 4) as isize;
            let align_item = AlignTest {
                a: item as u16,
                b: item as u32,
                c: item as u64,
                d: item as u64,
            };
            match real_map.get(&align_item) {
                Some(value) => {
                    assert_eq!(count, *value);
                }
                None => {
                    assert_eq!(count, 0);
                }
            }
        }
    }

    #[cfg(test)]
    fn assert_entries<'a, T: Ord + Copy + Debug + 'a>(
        iter: std::io::Result<impl Iterator<Item = &'a Entry<T>>>,
        expected: Vec<Entry<T>>,
    ) {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct EntryCmp<T> {
            item: T,
            count: isize,
        }

        let to_cmp = |e: &Entry<T>| EntryCmp {
            item: e.item,
            count: e.count,
        };

        assert!(iter.is_ok(), "iterator error: {:?}", iter.err());
        let Ok(iter) = iter else { return };
        let mut actual: Vec<EntryCmp<T>> = iter.map(to_cmp).collect();
        let mut expected: Vec<EntryCmp<T>> = expected.iter().map(to_cmp).collect();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_counter_clear() {
        let mut counter = HashCounter::<usize>::default();
        counter.add(1, 1);
        counter.add(2, 3);

        assert_entries(
            Ok(counter.iter()),
            vec![
                Entry {
                    item: 1usize,
                    count: 1,
                },
                Entry { item: 2, count: 3 },
            ],
        );

        counter.clear();

        assert_entries(Ok(counter.iter()), vec![]);

        counter.add(42, 7);
        assert_entries(
            Ok(counter.iter()),
            vec![Entry {
                item: 42usize,
                count: 7,
            }],
        );
    }

    #[test]
    fn temp_fd_array_clear() {
        let mut arr = TempFdArray::<Entry<usize>>::new().unwrap();
        let mut expected: Vec<Entry<usize>> = Vec::new();

        for i in 0..=(BUFFER_LENGTH + 10) {
            arr.push(Entry { item: i, count: 1 }).unwrap();
            expected.push(Entry { item: i, count: 1 });
        }
        assert!(arr.flush_n > 0);

        assert_entries(arr.try_iter(), expected);

        arr.clear().unwrap();

        assert_eq!(arr.buffer_index, 0);
        assert_eq!(arr.flush_n, 0);
        assert_entries(arr.try_iter(), vec![]);

        let mut expected_reuse: Vec<Entry<usize>> = Vec::new();
        for i in 0..=(BUFFER_LENGTH + 10) {
            arr.push(Entry { item: i, count: 2 }).unwrap();
            expected_reuse.push(Entry { item: i, count: 2 });
        }
        assert!(arr.flush_n > 0);
        assert_entries(arr.try_iter(), expected_reuse);
    }

    #[test]
    fn collector_clear_with_disk_eviction() {
        let mut collector = Collector::<usize>::new().unwrap();

        let n = BUCKETS * BUCKETS_ASSOCIATIVITY * 4;
        let mut expected_before: Vec<Entry<usize>> = Vec::new();
        for item in 0..n {
            collector.add(item, 1).unwrap();
            expected_before.push(Entry { item, count: 1 });
        }

        assert!(collector.flushed_to_disk() > 0);
        assert_entries(collector.try_iter(), expected_before);

        collector.clear().unwrap();

        assert_entries(collector.try_iter(), vec![]);

        let mut expected_reuse: Vec<Entry<usize>> = Vec::new();
        for item in 0..10 {
            collector.add(item, 2).unwrap();
            expected_reuse.push(Entry { item, count: 2 });
        }
        assert_entries(collector.try_iter(), expected_reuse);
    }
}
