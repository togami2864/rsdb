use std::sync::{Arc, Mutex};

use crate::{
    buffer::BufferManager,
    log::LogManager,
    log_records::{CommitRecord, RollbackRecord, StartRecord},
    record::{create_log_record, LogRecord, TxType},
    tx::Transaction,
};

pub struct RecoveryManager {
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
    tx: Arc<Mutex<Transaction>>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tx_num: i32,
        lm: Arc<Mutex<LogManager>>,
        bm: Arc<Mutex<BufferManager>>,
    ) -> Self {
        let rm = Self {
            tx,
            lm: Arc::clone(&lm),
            bm,
            tx_num,
        };
        StartRecord::write_to_log(lm).unwrap();
        rm
    }

    pub fn commit(&self) {
        self.bm.lock().unwrap().flush_all(self.tx_num);
        let lsn = CommitRecord::write_to_log(Arc::clone(&self.lm)).unwrap();
        self.lm.lock().unwrap().flush_with_lsn(lsn).unwrap();
    }

    pub fn rollback(&mut self) {
        self.do_rollback();
        self.bm.lock().unwrap().flush_all(self.tx_num);
        let lsn = RollbackRecord::write_to_log(Arc::clone(&self.lm)).unwrap();
        self.lm.lock().unwrap().flush_with_lsn(lsn).unwrap();
    }

    fn do_rollback(&mut self) {
        let mut lm = self.lm.lock().unwrap();
        let mut iter = lm.iterator().unwrap();
        while iter.has_next() {
            let bytes = iter.next().unwrap();
            let rec = create_log_record(bytes).unwrap();
            if rec.tx_num() == self.tx_num && rec.op() == TxType::Start {
                return;
            }
        }
    }
}
