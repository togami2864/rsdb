use std::{
    fmt, mem,
    sync::{Arc, Mutex},
};

use crate::{
    file::{FileError, Page},
    log::LogManager,
    record::{LogRecord, TxType},
};

#[derive(Debug, Default)]
pub struct CheckPointRecord {}

impl fmt::Display for CheckPointRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

impl LogRecord for CheckPointRecord {
    fn op(&self) -> TxType {
        TxType::CheckPoint
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl CheckPointRecord {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::CheckPoint as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}

#[derive(Debug, Default)]
pub struct StartRecord {}

impl fmt::Display for StartRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<START>")
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> TxType {
        TxType::Start
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl StartRecord {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::Start as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}

#[derive(Debug, Default)]
pub struct CommitRecord {}

impl fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<COMMIT>")
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> TxType {
        TxType::Commit
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl CommitRecord {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::Commit as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}

#[derive(Debug, Default)]
pub struct RollbackRecord {}

impl fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<Rollback>")
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> TxType {
        TxType::Rollback
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl RollbackRecord {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::Rollback as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}

#[derive(Debug, Default)]
pub struct SetI32Record {}

impl fmt::Display for SetI32Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<SETI32>")
    }
}

impl LogRecord for SetI32Record {
    fn op(&self) -> TxType {
        TxType::SetI32
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl SetI32Record {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::SetI32 as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}

#[derive(Debug, Default)]
pub struct SetStringRecord {}

impl fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<SETSTRING>")
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> TxType {
        TxType::SetI32
    }

    fn tx_num(&self) -> i32 {
        -1
    }
}

impl SetStringRecord {
    pub fn new() -> Self {
        Self {}
    }

    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, FileError> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new(reclen.try_into().unwrap());
        p.set_i32(0, TxType::SetString as i32)?;

        lm.lock().unwrap().append(p.contents().to_vec())
    }
}
