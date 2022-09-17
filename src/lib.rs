pub mod file;
pub mod log;
mod test_util;

use file::FileManager;
use std::{io, path::Path};

#[derive(Debug)]
pub struct RSDB {
    file_manager: FileManager,
}

impl RSDB {
    pub fn new(db_path: impl AsRef<Path>, _block_size: u64, _pool: u64) -> io::Result<Self> {
        let fm = FileManager::new(db_path)?;
        Ok(RSDB { file_manager: fm })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{BlockId, Page, BLOCK_SIZE};
    #[test]
    fn test_file_manager() {
        let db_dir = "test_dir";
        let test_file_name = "test_file";
        let db = RSDB::new(db_dir, 400, 8).unwrap();
        let mut fm = db.file_manager;

        let block_id = BlockId::new(test_file_name, 2);
        let mut p1 = Page::new(BLOCK_SIZE);
        let pos_1 = 88;
        p1.set_string(pos_1, "abcdefghijklm").unwrap();
        let size = p1.max_length("abcdefghijklm".len());
        let pos_2 = pos_1 + size;
        p1.set_int(pos_2, 345).unwrap();
        fm.write(&block_id, &mut p1).unwrap();

        test_util::remove_test_file_and_dir(db_dir, test_file_name).unwrap();
    }

    // #[test]
    // fn log_manager_operation() {
    //     let db_dir = "test_dir";
    //     let test_file_name = "test_file";
    //     let db = RSDB::new(db_dir, 400, 8).unwrap();
    // }
}
