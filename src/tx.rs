/// Transaction:
/// 1. manage buffers
/// 2. generate log records for each update and write them to the log file
/// 3. rollback transaction on demand
/// 4. guarantee the program will satisfy the ACID isolation property
pub struct Transaction {}

impl Transaction {
    pub fn commit() {}
    /// execute rollback a specified transaction `T`
    ///
    /// Algorithm
    /// 1. Set the current record to be the most recent log record.
    /// 2. Do until the current record is the start record for T:
    ///     a) If the current record is an update record for T then:
    ///         Write the saved old value to the specified location
    ///     b) Move to the previous record in the log
    /// 3. Append a rollback record to the log
    ///
    /// This algorithm reads the log backwards from the end,
    /// instead of forward from the beginning for the efficiency amd the correctness.
    pub fn rollback() {}

    ///
    /// Algorithm
    /// # the undo stage
    /// 1. For each log record
    ///     a) If the current record is a commit record then:
    ///         Add that transaction to the lost of committed transactions.
    ///     b) If the current record is a rollback record then:
    ///         Add that transaction to the lost of rolled-back transactions.
    ///     c) If the current record is an update record for a transaction not on the committed or rollback list, then:
    ///         Restore the old value at the specified location.
    ///
    /// # the redo stage
    /// 2. For each log record
    ///     If the current record is an update record and that transaction is on the committed list,
    ///         then: Restore the new value at the specified location.
    pub fn recover() {}

    pub fn pin() {}
    pub fn unpin() {}
    pub fn get_int() {}
    pub fn get_string() {}
    pub fn set_int() {}
    pub fn set_string() {}
    pub fn available_buff() {}

    pub fn size() {}
    pub fn append() {}
    pub fn block_size() {}
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::BufferManager,
        file::{BlockId, FileManager},
        log::LogManager,
    };

    fn test_transaction() {
        let fm = Arc::new(Mutex::new(FileManager::new("test_tx").unwrap()));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "test_log".to_string(),
        )));
        let mut bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), 3);

        let tx1 = Transaction::new(fm, lm, bm);
        let b = BlockId::new("t0", 0);
        tx1.pin(b);
        tx1.set_int(b, 80, 1, false);
        tx1.set_string(b, 40, "one", false);
        tx1.commit();

        let tx2 = Transaction::new(fm, lm, bm);
    }
}
