use std::{cell::RefCell, io, rc::Rc};

use crate::file::{BlockId, FileManager, Page, INTEGER_SIZE};

#[derive(Debug)]
pub struct LogManager {
    file_manager: Rc<RefCell<FileManager>>,
    log_file_name: String,
    log_page: Page,
    cur_block: BlockId,
    latest_lsn: u64,
    last_saved_lsn: u64,
}

impl LogManager {
    pub fn new(fm: Rc<RefCell<FileManager>>, log_file_name: String) -> Self {
        let block_size = fm.borrow_mut().block_size();
        let mut log_page = Page::new(block_size);
        let log_size = fm.borrow_mut().length(&log_file_name).unwrap();
        let cur_block = if log_size == 0 {
            let block = fm.borrow_mut().append(&log_file_name).unwrap();
            log_page.set_int(0, fm.borrow_mut().block_size()).unwrap();
            fm.borrow_mut().write(&block, &mut log_page).unwrap();
            block
        } else {
            let cur = BlockId::new(&log_file_name, log_size - 1);
            fm.borrow_mut().read(&cur, &mut log_page).unwrap();
            cur
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

    pub fn append(&mut self, log_record: Vec<u8>) -> io::Result<u64> {
        let boundary = self.log_page.get_int(0)?;
        let record_size = log_record.len() as u64;
        let byte_needed = record_size + INTEGER_SIZE;
        let boundary = if (boundary as i64 - byte_needed as i64) < INTEGER_SIZE as i64 {
            self.flush()?;
            self.cur_block = self.append_new_block()?;
            self.log_page.get_int(0)?
        } else {
            boundary
        };
        let record_pos = boundary - byte_needed;
        self.log_page.set_bytes(record_pos, &log_record)?;
        self.log_page.set_int(0, record_pos)?;
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn append_new_block(&mut self) -> io::Result<BlockId> {
        let block = self
            .file_manager
            .borrow_mut()
            .append(&self.log_file_name)
            .unwrap();
        self.log_page
            .set_int(0, self.file_manager.borrow_mut().block_size())?;
        self.file_manager
            .borrow_mut()
            .write(&block, &mut self.log_page)?;
        Ok(block)
    }

    pub fn iterator(&mut self) -> io::Result<LogIterator> {
        self.flush().unwrap();
        Ok(LogIterator::new(
            Rc::clone(&self.file_manager),
            self.cur_block.clone(),
        ))
    }

    pub fn flush_with_lsn(&mut self, lsn: u64) -> io::Result<()> {
        if lsn >= self.last_saved_lsn {
            self.flush()?;
        };
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_manager
            .borrow_mut()
            .write(&self.cur_block, &mut self.log_page)?;
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }
}

pub struct LogIterator {
    file_manager: Rc<RefCell<FileManager>>,
    block_id: BlockId,
    page: Page,
    cur_pos: u64,
    boundary: u64,
}

impl LogIterator {
    pub fn new(file_manager: Rc<RefCell<FileManager>>, block: BlockId) -> Self {
        let p = Page::new(file_manager.borrow_mut().block_size());
        let mut log_itertor = LogIterator {
            file_manager,
            block_id: block.clone(),
            page: p,
            cur_pos: 0,
            boundary: 0,
        };

        log_itertor.move_to_block(&block).unwrap();
        log_itertor
    }

    pub fn move_to_block(&mut self, block: &BlockId) -> io::Result<()> {
        self.file_manager.borrow_mut().read(block, &mut self.page)?;
        self.boundary = self.page.get_int(0)?;
        self.cur_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.file_manager.borrow().block_size() {
            return None;
        }
        if self.cur_pos == self.file_manager.borrow_mut().block_size() {
            let block = BlockId::new(
                self.block_id.filename().to_string(),
                self.block_id.number() - 1,
            );
            self.move_to_block(&block).unwrap();
        };
        let record = self.page.get_bytes(self.cur_pos).unwrap();
        self.cur_pos += INTEGER_SIZE + record.len() as u64;
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util;

    use super::*;

    fn create_log_record(s: String, n: u64) -> Vec<u8> {
        let npos = Page::max_length(s.len());
        let mut p = Page::new(npos + INTEGER_SIZE);
        p.set_string(0, &s).unwrap();
        p.set_int(npos, n).unwrap();
        p.contents().to_vec()
    }

    #[test]
    fn log_manager_operations() {
        let dirname = "test_dir";
        let filename = "log_test";
        let fm = Rc::new(RefCell::new(FileManager::new(dirname).unwrap()));
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
            let val = page.get_int(npos).unwrap();
            assert_eq!(s.to_string(), format!("record{}", val).to_string());
        }

        test_util::remove_test_file_and_dir(dirname, filename).unwrap();
    }
}
