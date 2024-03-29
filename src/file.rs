use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Cursor, Error, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const BLOCK_SIZE: i32 = 4096;
pub const U64_SIZE: usize = mem::size_of::<u64>();
pub const I32_SIZE: usize = mem::size_of::<i32>();

#[derive(Debug)]
pub enum FileError {
    IoError(Error),
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileError::IoError(err) => write!(f, "File error: {}", err),
        }
    }
}

impl From<io::Error> for FileError {
    fn from(value: io::Error) -> Self {
        FileError::IoError(value)
    }
}

pub type Result<T> = std::result::Result<T, FileError>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct BlockNum(pub i32);
impl BlockNum {
    pub fn as_i32(&self) -> i32 {
        self.0
    }
}

/// `BlockId` identifies a specific block by its file name and logical block number
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct BlockId {
    filename: String,
    block_id: BlockNum,
}

impl BlockId {
    pub fn new(filename: impl Into<String>, block_id: i32) -> Self {
        Self {
            filename: filename.into(),
            block_id: BlockNum(block_id),
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> i32 {
        self.block_id.as_i32()
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename(), self.number())
    }
}

/// the minimum unit when reading and writing from the heap file.
#[derive(Debug)]
pub struct Page {
    bb: Cursor<Vec<u8>>,
}

impl Page {
    pub fn new(capacity: i32) -> Self {
        let buf = vec![0; capacity as usize];
        Page {
            bb: Cursor::new(buf),
        }
    }

    /// read 8 bytes from offset value
    pub fn get_u64(&mut self, offset: u64) -> Result<u64> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let buf: &mut [u8; U64_SIZE] = &mut [0; U64_SIZE];
        self.bb.read_exact(buf)?;
        Ok(u64::from_be_bytes(*buf))
    }

    /// read 4 bytes from offset value
    pub fn get_i32(&mut self, offset: u64) -> Result<i32> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let mut buf: [u8; I32_SIZE] = [0; I32_SIZE];
        self.bb.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// write u64 value to the byte buffer
    pub fn set_u64(&mut self, offset: u64, val: u64) -> Result<()> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let data = u64::to_be_bytes(val);
        self.bb.write_all(&data)?;
        Ok(())
    }

    /// write i32 value to the byte buffer
    pub fn set_i32(&mut self, offset: u64, val: i32) -> Result<()> {
        self.bb.seek(SeekFrom::Start(offset))?;
        let data = i32::to_be_bytes(val);
        self.bb.write_all(&data)?;
        Ok(())
    }

    /// read 4 bytes and return it
    pub fn get_bytes(&mut self, offset: u64) -> Result<Vec<u8>> {
        let len = self.get_i32(offset)? as usize;
        let mut buf = vec![0; len];
        self.bb.read_exact(buf.as_mut())?;
        Ok(buf)
    }

    pub fn set_bytes(&mut self, offset: u64, byte: &[u8]) -> Result<()> {
        self.set_i32(offset, byte.len() as i32)?;
        self.bb.write_all(byte).unwrap();
        Ok(())
    }

    /// read 4bytes and convert it to String
    pub fn get_string(&mut self, offset: u64) -> Result<String> {
        let byte = self.get_bytes(offset)?;
        Ok(String::from_utf8(byte).unwrap())
    }

    pub fn set_string(&mut self, offset: u64, s: &str) -> Result<()> {
        self.set_bytes(offset, s.as_bytes())?;
        Ok(())
    }

    pub fn max_length(strlen: usize) -> i32 {
        (I32_SIZE + strlen) as i32
    }

    pub fn contents(&mut self) -> &mut Vec<u8> {
        self.bb.get_mut()
    }
}

impl From<Vec<u8>> for Page {
    fn from(b: Vec<u8>) -> Self {
        Page { bb: Cursor::new(b) }
    }
}

/// Read and Write pages to disk blocks
#[derive(Debug)]
pub struct FileManager {
    open_files: HashMap<String, Arc<Mutex<File>>>,
    db_dir: PathBuf,
    block_size: i32,
    is_new: bool,
}

impl FileManager {
    pub fn new(db_dir: impl AsRef<Path>) -> Result<Self> {
        let is_exist = db_dir.as_ref().exists();
        if !is_exist {
            fs::create_dir_all(&db_dir).expect("Failed to create dir");
        };
        let paths = fs::read_dir(&db_dir)?;
        for p in paths.flatten() {
            if p.path().display().to_string().starts_with("temp") {
                fs::remove_dir(p.path()).expect("Failed to remove dir 'temp'");
            };
        }
        Ok(FileManager {
            db_dir: db_dir.as_ref().to_path_buf(),
            block_size: BLOCK_SIZE,
            open_files: HashMap::new(),
            is_new: !is_exist,
        })
    }

    pub fn block_size(&self) -> i32 {
        self.block_size
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn length(&mut self, filename: &str) -> Result<i32> {
        let f = self.get_file(filename)?;
        let file_size = f.lock().unwrap().metadata()?.len() as i32;
        Ok(file_size / self.block_size())
    }

    pub fn read(&mut self, block_id: &BlockId, p: &mut Page) -> Result<()> {
        let offset = self.block_size() * block_id.number();
        match self.get_file(block_id.filename()) {
            Ok(file) => {
                let mut f = file.lock().expect("Failed to lock");
                f.seek(SeekFrom::Start(offset as u64))?;
                let _ = f.read(p.contents())?;
            }
            Err(_) => todo!(),
        }
        Ok(())
    }

    pub fn write(&mut self, block_id: &BlockId, p: &mut Page) -> Result<()> {
        let offset = self.block_size() * block_id.number();
        match self.get_file(block_id.filename()) {
            Ok(file) => {
                let mut f = file.lock().expect("Failed to lock");
                f.seek(SeekFrom::Start(offset as u64))?;
                f.write_all(p.contents())?;
            }
            Err(_) => todo!(),
        }
        Ok(())
    }

    /// `append` seeks to the end of the file and writes an empty array of bytes to it,
    ///  which  causes the OS to automatically extend the file.
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let blk_num = filename.len() as i32;
        let block = BlockId::new(filename.to_string(), blk_num);
        let offset = self.block_size * block.number();

        let empty_buf = &[];
        {
            let mut file = self.get_file(filename)?.lock().expect("Failed to lock");
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(empty_buf)?;
        }
        Ok(block)
    }

    pub fn get_file(&mut self, filename: &str) -> Result<&mut Arc<Mutex<File>>> {
        match self.open_files.entry(filename.to_string()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let path = Path::new(&self.db_dir).join(filename);
                let f = OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(path)?;
                Ok(entry.insert(Arc::new(Mutex::new(f))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_i32_from_page() {
        let mut p1 = Page::new(BLOCK_SIZE);
        p1.set_i32(0, 64).unwrap();
        assert_eq!(p1.get_i32(0).unwrap(), 64);
        //
        // [0,0,0,64,0,0,0 .........0, 0]:[u8; 4096]
        //

        let mut p2 = Page::new(BLOCK_SIZE);
        p2.set_i32(16, 64).unwrap();
        assert_eq!(p2.get_i32(16).unwrap(), 64);
        assert_eq!(p2.get_i32(0).unwrap(), 0);
        //                              16 -|
        // [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,64,0,0,......0,0]:[u8; 4096]
        //
    }

    #[test]
    fn set_and_get_bytes_from_page() {
        let mut p1 = Page::new(BLOCK_SIZE);
        p1.set_bytes(0, &[0, 0, 0, 4]).unwrap();
        assert_eq!(p1.get_bytes(0).unwrap(), &[0, 0, 0, 4]);

        let mut p2 = Page::new(BLOCK_SIZE);
        p2.set_bytes(16, &[0, 0, 0, 128]).unwrap();
        assert_eq!(p2.get_bytes(16).unwrap(), &[0, 0, 0, 128]);
        assert_eq!(p2.get_i32(0).unwrap(), 0);
    }

    #[test]
    fn set_and_get_string_from_page() {
        let mut page = Page::new(4096);
        page.set_string(0, "abcdefghijklmn").unwrap();
        assert_eq!(page.get_string(0).unwrap(), "abcdefghijklmn");
    }

    #[test]
    fn read_and_write_file() {
        let dirname = "__test_1/dir1";
        let filename = "testfile";
        let mut path = PathBuf::from(dirname);
        path.push(filename);
        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
        }

        let mut file_manager = FileManager::new(dirname).expect("Failed to init file_manager");
        let block = BlockId::new(filename, 0);
        let mut p = Page::new(BLOCK_SIZE);
        p.set_string(0, "sample text").unwrap();

        file_manager.write(&block, &mut p).unwrap();
        file_manager.read(&block, &mut p).unwrap();
        assert_eq!(p.get_string(0).unwrap(), "sample text");

        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
            fs::remove_dir("__test_1").expect("failed to remove dir");
        }
    }

    #[test]
    fn read_and_write_file_with_offset() {
        let dirname = "__test_2/dir2";
        let filename = "testfile";
        let mut path = PathBuf::from(dirname);
        path.push(filename);
        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
        }

        let mut fm = FileManager::new(dirname).unwrap();
        let block_id = BlockId::new(filename, 2);

        let mut p1 = Page::new(BLOCK_SIZE);
        let pos_1 = 88;
        p1.set_string(pos_1, "abcdefghijklm").unwrap();

        let size = Page::max_length("abcdefghijklm".len()) as u64;
        let pos_2 = pos_1 + size;
        p1.set_i32(pos_2, 345).unwrap();
        fm.write(&block_id, &mut p1).unwrap();

        if path.to_owned().exists() {
            fs::remove_dir_all(dirname).expect("failed to remove dir");
            fs::remove_dir("__test_2").expect("failed to remove dir");
        }
    }
}
