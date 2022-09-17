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
        let boundary = if boundary - byte_needed < INTEGER_SIZE {
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
    file_manager: FileManager,
    block_id: BlockId,
    page: Page,
    cur_pos: u64,
    boundary: u64,
}

impl LogIterator {
    pub fn new(&mut self, file_manager: FileManager, block: BlockId) -> Self {
        let p = Page::new(file_manager.block_size());
        self.move_to_block(&block).unwrap();
        LogIterator {
            file_manager,
            block_id: block,
            page: p,
            cur_pos: 0,
            boundary: 0,
        }
    }

    pub fn move_to_block(&mut self, block: &BlockId) -> io::Result<()> {
        self.file_manager.read(block, &mut self.page)?;
        self.boundary = self.page.get_int(0)?;
        self.cur_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos == self.file_manager.block_size() as u64 {
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
