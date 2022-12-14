use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{cmp, io};

use itertools::Itertools;

#[derive(Default)]
pub struct DiskFragments {
    /// A mapping of files to their applicable locations.
    ///
    /// A fragment is essentially part of a virtual file located within
    /// the segment itself, virtual files may not be contiguous
    fragments: BTreeMap<PathBuf, Vec<Range<u64>>>,
}

impl DiskFragments {
    /// Adds a fragment for a given file.
    ///
    /// If `overwrite` is true, all other fragments are removed.
    pub fn mark_fragment_location(
        &mut self,
        path: PathBuf,
        location: Range<u64>,
        overwrite: bool,
    ) {
        if overwrite {
            self.clear_fragments(&path);
        }

        self.fragments.entry(path).or_default().push(location);
    }

    /// Checks if any fragments exist for the given file.
    pub fn exists(&self, path: &Path) -> bool {
        self.fragments.contains_key(path)
    }

    /// Removes all fragments of a given file.
    pub fn clear_fragments(&mut self, path: &Path) {
        self.fragments.remove(path);
    }

    pub fn inner(&self) -> &BTreeMap<PathBuf, Vec<Range<u64>>> {
        &self.fragments
    }

    /// The total space taken up by valid fragments.
    pub fn total_size(&self) -> usize {
        let mut total = 0;

        for ranges in self.fragments.values() {
            for range in ranges {
                total += (range.end - range.start) as usize;
            }
        }

        total
    }

    /// Calculates the given file's total storage on disk.
    pub fn file_size(&self, path: &Path) -> Option<usize> {
        self.fragments.get(path).map(|fragments: &Vec<Range<u64>>| {
            fragments
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>() as usize
        })
    }

    /// Calculates what fragments of file data are required
    /// in order to read a given range of data as if it was a normal file on disk.
    pub fn get_selected_fragments(
        &self,
        path: &Path,
        file_range: Range<usize>,
    ) -> io::Result<SelectedFragments> {
        let fragments = self
            .fragments
            .get(path)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "File not found"))?;

        let mut max_selection_area = 0;
        let fragments_iter = fragments
            .iter()
            .map(|range| {
                max_selection_area = cmp::max(max_selection_area, range.end);
                range
            })
            .cloned()
            .sorted_by_key(|range| range.start);

        let mut num_bytes_to_skip = file_range.start;
        let mut total_bytes_planned_read = 0;
        let mut selected_fragments = Vec::new();
        for range in fragments_iter {
            if total_bytes_planned_read >= file_range.len() {
                break;
            }

            let fragment_len = range.end - range.start;

            if fragment_len < num_bytes_to_skip as u64 {
                num_bytes_to_skip -= fragment_len as usize;
                continue;
            }

            // We don't want to read the bytes we dont care about.
            let seek_to = range.start + num_bytes_to_skip as u64;

            // We've skipped all the bytes we need to.
            num_bytes_to_skip = 0;

            let len = range.end - seek_to;
            selected_fragments.push((seek_to, len as usize));

            total_bytes_planned_read += len as usize;
        }

        Ok(SelectedFragments {
            fragments: selected_fragments,
            minimum_flushed_pos: max_selection_area,
        })
    }
}

pub struct SelectedFragments {
    pub fragments: Vec<(u64, usize)>,
    pub minimum_flushed_pos: u64,
}
