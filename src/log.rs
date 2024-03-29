use std::sync::{Arc, Mutex};

use crate::file::{BlockId, FileManager, Page, I32_SIZE};

use crate::file::Result;

#[derive(Debug)]
pub struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file_name: String,
    log_page: Page,
    cur_block: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogManager {
    pub fn new(fm: Arc<Mutex<FileManager>>, log_file_name: String) -> Self {
        let (log_page, cur_block) = {
            let mut fm = fm.lock().unwrap();
            let block_size = fm.block_size();
            let mut log_page = Page::new(block_size);
            let log_size = fm.length(&log_file_name).unwrap();
            let cur_block = if log_size == 0 {
                let block = fm.append(&log_file_name).unwrap();
                log_page.set_i32(0, fm.block_size()).unwrap();
                fm.write(&block, &mut log_page).unwrap();
                block
            } else {
                let cur = BlockId::new(&log_file_name, log_size - 1);
                fm.read(&cur, &mut log_page).unwrap();
                cur
            };
            (log_page, cur_block)
        };

        LogManager {
            file_manager: fm,
            log_file_name,
            log_page,
            cur_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    pub fn append(&mut self, log_record: Vec<u8>) -> Result<i32> {
        let boundary = self.log_page.get_i32(0)?;
        let record_size = log_record.len() as i32;
        let byte_needed = record_size + I32_SIZE as i32;
        let boundary = if (boundary - byte_needed) < I32_SIZE.try_into().unwrap() {
            self.flush()?;
            self.cur_block = self.append_new_block()?;
            self.log_page.get_i32(0)?
        } else {
            boundary
        };
        let record_pos = boundary - byte_needed;
        self.log_page
            .set_bytes(record_pos.try_into().unwrap(), &log_record)?;
        self.log_page.set_i32(0, record_pos)?;
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn append_new_block(&mut self) -> Result<BlockId> {
        let block = {
            let mut fm = self.file_manager.lock().expect("Failed to lock");
            let block = fm.append(&self.log_file_name).unwrap();
            self.log_page.set_i32(0, fm.block_size())?;
            fm.write(&block, &mut self.log_page)?;
            block
        };
        Ok(block)
    }

    pub fn iterator(&mut self) -> Result<LogIterator> {
        self.flush().unwrap();
        Ok(LogIterator::new(Arc::clone(&self.file_manager), self.cur_block.clone()).unwrap())
    }

    pub fn flush_with_lsn(&mut self, lsn: i32) -> Result<()> {
        if lsn >= self.last_saved_lsn {
            self.flush()?;
        };
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        {
            self.file_manager
                .lock()
                .unwrap()
                .write(&self.cur_block, &mut self.log_page)?;
        }
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }
}

pub struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    block_id: BlockId,
    page: Page,
    cur_pos: i32,
    boundary: i32,
}

impl LogIterator {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, block: BlockId) -> Result<Self> {
        let (page, cur_pos, boundary) = {
            let mut fm = file_manager.lock().expect("Failed to lock");
            let mut p = Page::new(fm.block_size());

            fm.read(&block, &mut p)?;
            let boundary = p.get_i32(0)?;
            let cur_pos = boundary;
            (p, cur_pos, boundary)
        };
        Ok(Self {
            file_manager,
            block_id: block,
            page,
            cur_pos,
            boundary,
        })
    }

    pub fn has_next(&self) -> bool {
        self.cur_pos < self.file_manager.lock().unwrap().block_size() || self.block_id.number() > 0
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut fm = self.file_manager.lock().expect("Failed to lock");
        if self.cur_pos >= fm.block_size() {
            return None;
        }
        if self.cur_pos == fm.block_size() {
            let block = BlockId::new(
                self.block_id.filename().to_string(),
                self.block_id.number() - 1,
            );
            fm.read(&block, &mut self.page).unwrap();
            self.boundary = self.page.get_i32(0).unwrap();
            self.cur_pos = self.boundary;
        };
        let record = self.page.get_bytes(self.cur_pos as u64).unwrap();
        self.cur_pos += (I32_SIZE + record.len()) as i32;
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    fn create_log_record(s: String, len: i32) -> Vec<u8> {
        let npos = Page::max_length(s.len());
        let mut p = Page::new(npos + I32_SIZE as i32);
        p.set_string(0, &s).unwrap();
        p.set_i32(npos as u64, len).unwrap();
        p.contents().to_vec()
    }

    #[test]
    fn log_manager_operations() {
        let dirname = "__test_3/dir3";
        let filename = "testfile";
        let mut path = PathBuf::from(dirname);
        path.push(filename);
        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
        }

        let fm = Arc::new(Mutex::new(FileManager::new(dirname).unwrap()));
        let mut lm = LogManager::new(fm, filename.to_string());
        println!("creating records: ");
        for i in 0..35 {
            let rec = create_log_record(format!("record{}", i), i);
            lm.append(rec).unwrap();
        }

        for rec in lm.iterator().unwrap() {
            let mut page = Page::from(rec);
            let s = page.get_string(0).unwrap();
            let npos = Page::max_length(s.len());
            let val = page.get_i32(npos as u64).unwrap();
            assert_eq!(s.to_string(), format!("record{}", val).to_string());
        }

        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
            fs::remove_dir("__test_3").expect("failed to remove dir");
        }
    }
}
