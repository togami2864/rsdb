use std::path::PathBuf;

use file::FileManager;

pub mod file;

#[derive(Debug)]
pub struct RSDB {
    file_manager: FileManager,
}

impl RSDB {
    pub fn new(db_path: PathBuf, _block_size: u64, _pool: u64) -> Self {
        RSDB {
            file_manager: FileManager::new(db_path),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::file::{BlockId, Page, BLOCK_SIZE};
    #[test]
    fn test_file() {
        let db_path = PathBuf::from("test_dir");
        let db = RSDB::new(db_path, 400, 8);
        let mut fm = db.file_manager;

        let block_id = BlockId::new("testfile".to_string(), 2);
        let mut p1 = Page::new(BLOCK_SIZE);
        let pos_1 = 88;
        p1.set_string(pos_1, "abcdefghijklm").unwrap();
        let size = p1.max_length("abcdefghijklm".len());
        let pos_2 = pos_1 + size;
        p1.set_int(pos_2, 345).unwrap();
        fm.write(&block_id, &mut p1).unwrap();

        fs::remove_file(format!(
            "{}/{}",
            PathBuf::from("test_dir").display(),
            "testfile"
        ))
        .unwrap();
        fs::remove_dir(PathBuf::from("test_dir")).unwrap();
    }
}
