use std::cell::RefCell;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

pub const BLOCK_SIZE: u64 = 4096;
pub const INTEGER_SIZE: u64 = 8;

/// `BlockId` identifies a specific block by its file name and logical block number
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct BlockId {
    filename: String,
    block_id: u64,
}

impl BlockId {
    pub fn new(filename: impl Into<String>, block_id: u64) -> Self {
        Self {
            filename: filename.into(),
            block_id,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> u64 {
        self.block_id
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        format!("[file {}, block {}]", self.filename(), self.number()).hash(&mut hasher);
        hasher.finish()
    }
}

/// the minimum unit when reading and writing from the heap file.
#[derive(Debug)]
pub struct Page {
    bb: Cursor<Vec<u8>>,
}

impl Page {
    pub fn new(capacity: u64) -> Self {
        Page {
            bb: Cursor::new(Vec::with_capacity(capacity as usize)),
        }
    }

    /// read 8 bytes from offset value
    pub fn get_int(&mut self, offset: u64) -> io::Result<u64> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let buf: &mut [u8; 8] = &mut [0; 8];
        self.bb.read_exact(buf)?;
        Ok(u64::from_be_bytes(*buf))
    }

    /// write integer to byte buffer from offset
    pub fn set_int(&mut self, offset: u64, val: u64) -> io::Result<()> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let data = u64::to_be_bytes(val);
        self.bb.write_all(&data)?;
        Ok(())
    }

    /// read 8 bytes and return it
    pub fn get_bytes(&mut self, offset: u64) -> io::Result<Vec<u8>> {
        let len = self.get_int(offset)? as usize;
        let mut buf = vec![0; len];
        self.bb.read_exact(buf.as_mut())?;
        Ok(buf)
    }

    pub fn set_bytes(&mut self, offset: u64, byte: &[u8]) -> io::Result<()> {
        self.set_int(offset, byte.len() as u64)?;
        self.bb.write_all(byte).unwrap();
        Ok(())
    }

    /// read 8bytes and convert it to String
    pub fn get_string(&mut self, offset: u64) -> io::Result<String> {
        let byte = self.get_bytes(offset)?;
        Ok(String::from_utf8(byte).unwrap())
    }

    pub fn set_string(&mut self, offset: u64, s: &str) -> io::Result<()> {
        self.set_bytes(offset, s.as_bytes())?;
        Ok(())
    }

    pub fn max_length(&self, strlen: usize) -> u64 {
        INTEGER_SIZE + strlen as u64
    }

    fn contents(&mut self) -> &mut Vec<u8> {
        self.bb.get_mut()
    }
}

/// Read and Write pages to disk blocks
#[derive(Debug)]
pub struct FileManager {
    open_files: Rc<RefCell<HashMap<String, File>>>,
    db_dir: PathBuf,
    block_size: u64,
    is_new: bool,
}

impl FileManager {
    pub fn new(db_dir: impl AsRef<Path>) -> io::Result<Self> {
        let is_new = db_dir.as_ref().exists();
        if !is_new {
            fs::create_dir(&db_dir)?;
        };
        let paths = fs::read_dir(&db_dir)?;
        for p in paths.flatten() {
            if p.path().display().to_string().starts_with("temp") {
                fs::remove_dir(p.path()).unwrap();
            };
        }
        Ok(FileManager {
            db_dir: db_dir.as_ref().to_path_buf(),
            block_size: BLOCK_SIZE,
            open_files: Rc::new(RefCell::new(HashMap::new())),
            is_new,
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn length(&mut self, filename: &str) -> io::Result<u64> {
        let file = self.get_file(filename)?;
        let file_size = file.metadata()?.len();
        Ok(file_size / self.block_size())
    }

    pub fn read(&mut self, block_id: &BlockId, p: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.filename())?;
        let offset = BLOCK_SIZE * block_id.number() as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(p.contents())?;
        Ok(())
    }

    pub fn write(&mut self, block_id: &BlockId, p: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.filename())?;
        let offset = BLOCK_SIZE * block_id.number();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(p.contents())?;
        Ok(())
    }

    /// `append` seeks to the end of the file and writes an empty array of bytes to it,
    ///  which  causes the OS to automatically extend the file.
    pub fn append(&mut self, filename: &str) -> io::Result<BlockId> {
        let blk_num = filename.len() as u64;
        let block = BlockId::new(filename.to_string(), blk_num);
        let offset = self.block_size * block.number();

        let empty_buf = &[];
        let mut file = self.get_file(filename)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(empty_buf)?;
        Ok(block)
    }

    pub fn get_file(&mut self, filename: &str) -> io::Result<File> {
        match self.open_files.borrow_mut().entry(filename.to_string()) {
            Entry::Occupied(entry) => entry.into_mut().try_clone(),
            Entry::Vacant(entry) => {
                let path = Path::new(&self.db_dir).join(filename);
                let f = OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(path)?;
                entry.insert(f).try_clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_util;

    #[test]
    fn page_set_get_int() {
        let mut page_1 = Page::new(BLOCK_SIZE);
        page_1.set_int(0, 64).unwrap();
        assert_eq!(page_1.get_int(0).unwrap(), 64);

        let mut page_2 = Page::new(BLOCK_SIZE);
        page_2.set_int(16, 64).unwrap();
        assert_eq!(page_2.get_int(16).unwrap(), 64);
        assert_eq!(page_2.get_int(0).unwrap(), 0);
    }

    #[test]
    fn page_set_get_bytes() {
        let mut page_1 = Page::new(BLOCK_SIZE);
        page_1.set_bytes(0, &[0, 0, 0, 4]).unwrap();
        assert_eq!(page_1.get_bytes(0).unwrap(), &[0, 0, 0, 4]);

        let mut page_2 = Page::new(BLOCK_SIZE);
        page_2.set_bytes(16, &[0, 0, 0, 128]).unwrap();
        assert_eq!(page_2.get_bytes(16).unwrap(), &[0, 0, 0, 128]);
        assert_eq!(page_2.get_int(0).unwrap(), 0);
    }

    #[test]
    fn page_set_get_string() {
        let mut page = Page::new(4096);
        page.set_string(0, "abcdefghijklmn").unwrap();
        assert_eq!(page.get_string(0).unwrap(), "abcdefghijklmn");
    }

    #[test]
    fn file_manager_operations() {
        let test_dir = "test_dir_1";
        let test_db_file_name = "test.db";
        let mut file_manager = FileManager::new(test_dir).unwrap();
        let block = BlockId::new(test_db_file_name, 0);
        let mut page = Page::new(BLOCK_SIZE);
        page.set_string(0, "sample text").unwrap();

        file_manager.write(&block, &mut page).unwrap();
        file_manager.read(&block, &mut page).unwrap();
        assert_eq!(page.get_string(0).unwrap(), "sample text");
        test_util::remove_test_file_and_dir(test_dir, test_db_file_name).unwrap();
    }
}
