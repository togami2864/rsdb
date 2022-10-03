use crate::{
    file::{BlockId, FileManager, Page},
    log::LogManager,
};
use std::{
    cell::RefCell,
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_TIME: u128 = 10000;

#[derive(Debug)]
pub struct Buffer {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager>>,
    contents: Page,
    block: Option<BlockId>,
    pins: u64,
    txnum: i64,
    lsn: i64,
}

impl Buffer {
    pub fn new(fm: Rc<RefCell<FileManager>>, lm: Rc<RefCell<LogManager>>) -> Self {
        let block_size = fm.borrow().block_size();
        Buffer {
            file_manager: fm,
            log_manager: lm,
            contents: Page::new(block_size),
            block: None,
            pins: 0, // block: BlockId::new
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.contents
    }

    pub fn set_modified(&mut self, txnum: i64, lsn: i64) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i64 {
        self.txnum
    }

    pub fn assign_to_block(&mut self, block: BlockId) {
        self.flush();
        self.file_manager
            .borrow_mut()
            .read(&block, &mut self.contents)
            .unwrap();
        self.block = Some(block);
        self.pins = 0;
    }

    fn flush(&mut self) {
        if self.txnum >= 0 {
            self.log_manager
                .borrow_mut()
                .flush_with_lsn(self.lsn as u64)
                .unwrap();
            if let Some(blk) = &self.block {
                self.file_manager
                    .borrow_mut()
                    .write(blk, &mut self.contents)
                    .unwrap();
                self.txnum -= 1;
            }
        }
    }

    fn pin(&mut self) {
        self.pins += 1;
    }

    fn unpin(&mut self) {
        self.pins -= 1;
    }
}

#[derive(Debug)]
pub struct BufferManager {
    buffer_pool: Vec<Rc<RefCell<Buffer>>>,
    num_available: u64,
}

impl BufferManager {
    pub fn new(fm: Rc<RefCell<FileManager>>, lm: Rc<RefCell<LogManager>>, num_buffs: u64) -> Self {
        let mut buffer_pool: Vec<Rc<RefCell<Buffer>>> = Vec::new();
        for index in 0..num_buffs {
            let buf = Buffer::new(Rc::clone(&fm), Rc::clone(&lm));
            buffer_pool.insert(index as usize, Rc::new(RefCell::new(buf)));
        }
        BufferManager {
            buffer_pool,
            num_available: num_buffs,
        }
    }

    pub fn available(&self) -> u64 {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i64) {
        for buf in self.buffer_pool.iter() {
            if buf.borrow().modifying_tx() == txnum {
                buf.borrow_mut().flush();
            }
        }
    }

    pub fn unpin(&mut self, buff: Rc<RefCell<Buffer>>) {
        buff.borrow_mut().unpin();
        if !buff.borrow().is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, block: BlockId) -> Rc<RefCell<Buffer>> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        if let Some(buf) = self.try_to_pin(block) {
            buf
        } else {
            todo!()
        }
    }

    fn waiting_too_long(start: u128) -> bool {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - start
            > MAX_TIME
    }

    fn try_to_pin(&mut self, block: BlockId) -> Option<Rc<RefCell<Buffer>>> {
        if let Some(buf) = self.try_to_pin(block) {
            if !buf.borrow().is_pinned() {
                self.num_available -= 1;
            };
            buf.try_borrow_mut().unwrap().pin();
            Some(buf)
        } else {
            None
        }
    }

    fn find_existing_buffer(&self, block: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|b| {
                if let Some(block_id) = &b.borrow().block {
                    block_id.eq(block)
                } else {
                    false
                }
            })
            .map(Rc::clone)
    }

    fn choose_unpinned_buffer(&self) -> Option<Rc<RefCell<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|b| !b.borrow().is_pinned())
            .map(Rc::clone)
    }
}
