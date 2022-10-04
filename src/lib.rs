pub mod buffer;
pub mod file;
pub mod log;

use file::FileManager;
use log::LogManager;
use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct RSDB {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
}

impl RSDB {
    pub fn new(db_path: impl AsRef<Path>, _block_size: u64, _pool: u64) -> io::Result<Self> {
        let fm = Arc::new(Mutex::new(FileManager::new(db_path)?));
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            "log_test".to_string(),
        )));
        Ok(RSDB {
            file_manager: fm,
            log_manager: lm,
        })
    }
}
