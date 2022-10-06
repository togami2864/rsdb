use crate::{
    file::{BlockId, FileManager, Page},
    log::LogManager,
};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_TIME: u128 = 10000;

#[derive(Debug)]
pub struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block: Option<BlockId>,
    pins: u64,
    txnum: i64,
    lsn: i64,
}

impl Buffer {
    pub fn new(fm: Arc<Mutex<FileManager>>, lm: Arc<Mutex<LogManager>>) -> Self {
        let block_size = fm.lock().unwrap().block_size();
        Buffer {
            file_manager: fm,
            log_manager: lm,
            contents: Page::new(block_size),
            block: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
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
        let mut fm = self.file_manager.lock().unwrap();
        fm.read(&block, &mut self.contents).unwrap();
        self.block = Some(block);
        self.pins = 0;
    }

    fn flush(&mut self) {
        if self.txnum >= 0 {
            let mut fm = self.file_manager.lock().unwrap();
            let mut lm = self.log_manager.lock().unwrap();
            lm.flush_with_lsn(self.lsn as u64).unwrap();
            if let Some(blk) = &self.block {
                fm.write(blk, &mut self.contents).unwrap();
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
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    num_available: u64,
}

impl BufferManager {
    pub fn new(fm: Arc<Mutex<FileManager>>, lm: Arc<Mutex<LogManager>>, num_buffs: u64) -> Self {
        let mut buffer_pool: Vec<Arc<Mutex<Buffer>>> = Vec::new();
        for index in 0..num_buffs {
            let buf = Buffer::new(Arc::clone(&fm), Arc::clone(&lm));
            buffer_pool.insert(index as usize, Arc::new(Mutex::new(buf)));
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
            let mut buf = buf.lock().unwrap();
            if buf.modifying_tx() == txnum {
                buf.flush();
            }
        }
    }

    pub fn unpin(&mut self, buf: Arc<Mutex<Buffer>>) {
        let mut buf = buf.lock().unwrap();
        buf.unpin();
        if !buf.is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, block: BlockId) -> Arc<Mutex<Buffer>> {
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

    fn try_to_pin(&mut self, block: BlockId) -> Option<Arc<Mutex<Buffer>>> {
        if let Some(buf) = self.find_existing_buffer(&block) {
            let mut b = buf.as_ref().lock().unwrap();
            if !b.is_pinned() {
                self.num_available -= 1;
            };
            b.pin();
            drop(b);
            Some(Arc::clone(&buf))
        } else if let Some(buf) = self.choose_unpinned_buffer() {
            let mut b = buf.as_ref().lock().unwrap();
            b.assign_to_block(block);
            if !b.is_pinned() {
                self.num_available -= 1;
            };
            b.pin();
            drop(b);
            Some(Arc::clone(&buf))
        } else {
            None
        }
    }

    fn find_existing_buffer(&self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|b| {
                if let Some(block_id) = &b.lock().unwrap().block {
                    block_id.eq(block)
                } else {
                    false
                }
            })
            .map(Arc::clone)
    }

    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|b| !b.lock().unwrap().is_pinned())
            .map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::{Buffer, BufferManager};
    use crate::{
        file::{BlockId, FileManager},
        log::LogManager,
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_buffer() {
        let fm = Arc::new(Mutex::new(FileManager::new("__test_4").unwrap()));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "test_log".to_string(),
        )));
        let mut bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), 3);

        let buf1 = bm.pin(BlockId::new("testfile", 1));
        {
            let mut buf1 = buf1.lock().unwrap();
            let p = buf1.contents();
            let n = p.get_int(80).unwrap();
            p.set_int(80, n + 1).unwrap();
            buf1.set_modified(1, 0);
        }
        bm.unpin(buf1);

        let buf2 = bm.pin(BlockId::new("testfile", 2));
        let _buf3 = bm.pin(BlockId::new("testfile", 3));
        let _buf4 = bm.pin(BlockId::new("testfile", 4));

        bm.unpin(buf2);
        let buf2 = bm.pin(BlockId::new("testfile", 1));
        {
            let mut p2 = buf2.lock().unwrap();
            let p2 = p2.contents();
            p2.set_int(80, 9999).unwrap();
            buf2.lock().unwrap().set_modified(1, 0);
        }
        bm.unpin(buf2);
    }

    #[test]
    fn test_buffer_manager() {
        let fm = Arc::new(Mutex::new(FileManager::new("__test_4").unwrap()));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "test_log".to_string(),
        )));
        let mut bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), 3);

        let mut buf: Vec<Arc<Mutex<Buffer>>> = vec![bm.pin(BlockId::new("testfile", 0))];
        buf.push(bm.pin(BlockId::new("testfile", 1)));
        buf.push(bm.pin(BlockId::new("testfile", 2)));
        bm.unpin(buf[1].to_owned());

        buf.push(bm.pin(BlockId::new("testfile", 0)));
        buf.push(bm.pin(BlockId::new("testfile", 1)));
        buf.push(bm.pin(BlockId::new("testfile", 3)));
        bm.unpin(buf[2].to_owned());
        buf.push(bm.pin(BlockId::new("testfile", 3)));

        for i in 0..buf.len() {
            match buf.get(i) {
                Some(b) => {
                    println!("buf[{}] pinned to block {:?}", i, b.lock().unwrap().block)
                }
                None => panic!(),
            };
        }
    }
}
