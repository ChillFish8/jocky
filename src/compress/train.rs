use std::cmp;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;
use humansize::DECIMAL;
use memmap2::Mmap;
use zstd::zstd_safe;


pub fn train_on_samples() {
    let big_file = File::open("../datasets/data.json").unwrap();
    let start = Instant::now();
    let size = zstd::encode_all(big_file, 3).unwrap().len();
    println!("Basic {} in {:?}", humansize::format_size(size, DECIMAL), start.elapsed());

    let big_file = File::open("../datasets/data.json").unwrap();
    let big_file_reader = BufReader::with_capacity(64 << 10, big_file);
    let mut lines = big_file_reader.lines();

    let start = Instant::now();
    let mut total = 0;
    while let Some(Ok(line)) = lines.next() {
        total += zstd::encode_all(line.as_bytes(), 3).unwrap().len();
    }
    println!("Stream {} in {:?}", humansize::format_size(total, DECIMAL), start.elapsed());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trained_thing() {
        train_on_samples()
    }
}