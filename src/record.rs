use crate::{
    file::Page,
    log_records::{
        CheckPointRecord, CommitRecord, RollbackRecord, SetI32Record, SetStringRecord, StartRecord,
    },
};
use std::fmt;

#[derive(Debug)]
pub enum LogRecordError {
    UnknownRecord,
}

impl fmt::Display for LogRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogRecordError::UnknownRecord => {
                write!(f, "Unknown Log record")
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TxType {
    CheckPoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetI32 = 4,
    SetString = 5,
}

impl TxType {
    fn from_int(n: i32) -> Option<Self> {
        match n {
            0 => Some(TxType::CheckPoint),
            1 => Some(TxType::Start),
            2 => Some(TxType::Commit),
            3 => Some(TxType::Rollback),
            4 => Some(TxType::SetI32),
            5 => Some(TxType::SetString),
            _ => None,
        }
    }
}

pub trait LogRecord {
    fn op(&self) -> TxType;
    fn tx_num(&self) -> i32;
    // fn undo(&mut self, tx_num: u64);
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>, LogRecordError> {
    let mut p = Page::from(bytes);
    let tx_type = TxType::from_int(p.get_i32(0).unwrap());

    match tx_type {
        Some(TxType::CheckPoint) => Ok(Box::new(CheckPointRecord::new())),
        Some(TxType::Start) => Ok(Box::new(StartRecord::new())),
        Some(TxType::Commit) => Ok(Box::new(CommitRecord::new())),
        Some(TxType::Rollback) => Ok(Box::new(RollbackRecord::new())),
        Some(TxType::SetI32) => Ok(Box::new(SetI32Record::new())),
        Some(TxType::SetString) => Ok(Box::new(SetStringRecord::new())),
        _ => Err(LogRecordError::UnknownRecord),
    }
}
