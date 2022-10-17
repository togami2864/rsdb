use crate::{
    file::{BlockId, FileManager, Page},
    log::LogManager,
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const MAX_TIME: u128 = 10000;

#[derive(Debug)]
pub struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block: Option<BlockId>,
    pins: u64,
    /// transaction number
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
            let mut lm = self.log_manager.lock().unwrap();
            lm.flush_with_lsn(self.lsn as u64).unwrap();
            if let Some(blk) = &self.block {
                let mut fm = self.file_manager.lock().unwrap();
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

    pub fn pin(&mut self, block: BlockId) -> Result<Arc<Mutex<Buffer>>, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        while !Self::waiting_too_long(timestamp) {
            if let Some(buf) = self.try_to_pin(block.clone()) {
                return Ok(buf);
            }
            thread::sleep(Duration::new(1, 0));
        }
        Err("Algorithm using now can not get replace buffers".to_string())
    }

    fn waiting_too_long(start: u128) -> bool {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - start
            > MAX_TIME
    }

    /// Naive algorithm: choose first unpinned buffer
    ///
    /// if (find existing buffer){
    ///     - return buffer
    /// } else if(find unpinned buffer){
    ///     - associates the buffer with a disk block.
    ///     - return buffer
    /// } else {
    ///     Error!: this algorithm doesn't have replacement rule.
    /// }
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
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    #[test]
    fn test_buffer() {
        let fm = Arc::new(Mutex::new(FileManager::new("__test_4").unwrap()));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "test_log".to_string(),
        )));
        let mut bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), 3);
        //
        // buffer pool:
        //      capacity = 3
        // -------------- buffer pool ----------------
        // |  [empty buf] [empty buf] [empty buf]    |
        // ------------------------------------------
        //

        let buf1 = bm.pin(BlockId::new("testfile", 1)).unwrap();
        {
            let mut buf1 = buf1.lock().unwrap();
            let p = buf1.contents();
            let n = p.get_int(80).unwrap();
            p.set_int(80, n + 1).unwrap();
            buf1.set_modified(1, 0);
        }
        bm.unpin(buf1);

        let buf2 = bm.pin(BlockId::new("testfile2", 2)).unwrap();
        let _buf3 = bm.pin(BlockId::new("testfile3", 3)).unwrap();
        let _buf4 = bm.pin(BlockId::new("testfile4", 4)).unwrap();

        bm.unpin(buf2);
        let buf2 = bm.pin(BlockId::new("testfile5", 11)).unwrap();
        {
            let mut b2 = buf2.lock().unwrap();
            let p2 = b2.contents();
            p2.set_int(80, 9999).unwrap();
            b2.set_modified(1, 0);
        }
        bm.unpin(buf2);

        fs::remove_dir_all("__test_4").expect("failed to remove dir");
    }

    #[test]
    fn test_buffer_manager() {
        let fm = Arc::new(Mutex::new(FileManager::new("__test_5").unwrap()));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "test_log".to_string(),
        )));
        let mut bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), 3);
        assert_eq!(bm.available(), 3);
        //
        // buffer pool:
        //      capacity = 3
        // -------------- buffer pool ----------------
        // |  [empty buf] [empty buf] [empty buf]    |
        // ------------------------------------------
        //

        let mut buf: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 6];

        buf[0] = bm.pin(BlockId::new("t0", 0)).unwrap().into();
        assert_eq!(bm.available(), 2);
        //
        // buffer pool:
        //      capacity = 3
        // -------------- buffer pool ---------------
        // |  [t0, 0] [empty buf] [empty buf]       |
        // ------------------------------------------
        //

        buf[1] = bm.pin(BlockId::new("t1", 1)).unwrap().into();
        assert_eq!(bm.available(), 1);
        //
        // buffer pool:
        //      capacity = 3
        // ------------- buffer pool -----------
        // |     [t0, 0] [t1, 1] [empty buf]   |
        // -------------------------------------
        //

        buf[2] = bm.pin(BlockId::new("t2", 2)).unwrap().into();
        assert_eq!(bm.available(), 0);
        //
        // buffer pool:
        //      capacity = 3
        // ---------------- buffer pool ----------------
        // |          [t0, 0] [t1, 1] [t2, 2]          |
        // ---------------------------------------------
        //

        bm.unpin(Arc::clone(buf[1].as_ref().unwrap()));
        buf[1] = None;
        assert_eq!(bm.available(), 1);
        //
        // buffer pool:
        //      capacity = 3
        // ------------ buffer pool -------------
        // |         [t0, 0] [] [t2, 2]         |
        // --------------------------------------
        //

        buf[3] = bm.pin(BlockId::new("t3", 3)).unwrap().into();
        assert_eq!(bm.available(), 0);
        //
        // buffer pool:
        //      capacity = 3
        // -------------- buffer pool ----------------
        // |           [t0, 0] [t3, 3] [t2, 2]       |
        // -------------------------------------------
        //

        // Get existing buffer
        let b4 = bm.pin(BlockId::new("t3", 3));
        assert!(b4.is_ok());
        buf[4] = Some(b4.unwrap());

        // Pin buffer above the capacity should `error` in this naive algorithm.
        let b5 = bm.pin(BlockId::new("t5", 5));
        println!("Algorithm using in this manager can not replace buffers");
        assert!(b5.is_err());
        buf[5] = None;

        assert!(buf[0].is_some());
        assert!(buf[1].is_none());
        assert!(buf[2].is_some());
        assert!(buf[3].is_some());
        assert!(buf[4].is_some());
        assert!(buf[5].is_none());

        fs::remove_dir_all("__test_5").expect("failed to remove dir");
    }
}
