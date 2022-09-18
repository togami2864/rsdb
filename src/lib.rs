pub mod file;
pub mod log;
mod test_util;

use file::FileManager;
use log::LogManager;
use std::{cell::RefCell, io, path::Path, rc::Rc};

#[derive(Debug)]
pub struct RSDB {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: LogManager,
}

impl RSDB {
    pub fn new(db_path: impl AsRef<Path>, _block_size: u64, _pool: u64) -> io::Result<Self> {
        let fm = Rc::new(RefCell::new(FileManager::new(db_path)?));
        let lm = LogManager::new(Rc::clone(&fm), "log_test".to_string());
        Ok(RSDB {
            file_manager: fm,
            log_manager: lm,
        })
    }
}
