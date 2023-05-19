use std::error::Error;

use axum::{http::StatusCode, Json};
use redb::{Database, ReadableTable, TableDefinition};
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
pub enum YwkvError {
    #[error("encountered redb error `{0}`")]
    Redb(#[from] redb::Error),
    #[error("key not found `{0}`")]
    KeyMissing(String),
    #[error("table was empty while getting key `{0}`")]
    EmptyTable(String),
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum Status {
    Read(ReadStatus),
    Write(WriteStatus),
}

#[derive(Serialize)]
pub enum ReadStatus {
    Found,
    Missing,
    Failure,
}

#[derive(Serialize)]
pub enum WriteStatus {
    SuccessNew,
    SuccessOverwrite,
    Failure,
}

#[derive(Serialize)]
pub struct Response {
    value: String,
    status: Status,
}

impl Response {
    pub fn new(value: String, status: Status) -> Self {
        Self { value, status }
    }

    pub fn from_read_error(e: impl Error) -> (StatusCode, Json<Response>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(Response::new(
                e.to_string(),
                Status::Read(ReadStatus::Failure),
            )),
        )
    }

    pub fn from_write_error(e: impl Error) -> (StatusCode, Json<Response>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(Response::new(
                e.to_string(),
                Status::Write(WriteStatus::Failure),
            )),
        )
    }
}

pub struct Db<'a> {
    pub database: Database,
    pub table: TableDefinition<'a, &'static str, &'static str>,
}

impl<'a> Db<'a> {
    pub fn read<T: AsRef<str>>(&self, key: T) -> Result<String, YwkvError> {
        let tx = match self.database.begin_read() {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        };

        let table = match tx.open_table(self.table) {
            Ok(v) => v,
            Err(redb::Error::TableDoesNotExist(_)) => {
                return Err(YwkvError::EmptyTable(key.as_ref().to_string()))
            }
            Err(e) => return Err(e.into()),
        };

        let val = table.get(key.as_ref());
        match val {
            Ok(Some(value)) => Ok(value.value().into()),
            Ok(None) => Err(YwkvError::KeyMissing(key.as_ref().to_string())),
            Err(e) => Err(e.into()),
        }
    }

    pub fn write<T: AsRef<str>>(&self, key: T, val: T) -> Result<Option<String>, YwkvError> {
        let tx = match self.database.begin_write() {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        };

        let old_value = {
            let mut table = match tx.open_table(self.table) {
                Ok(v) => v,
                Err(e) => return Err(e.into()),
            };

            let res = table.insert(key.as_ref(), val.as_ref());
            match res {
                Ok(Some(v)) => Some(v.value().to_string()),
                Ok(None) => None,
                Err(e) => return Err(e.into()),
            }
        };

        if let Err(e) = tx.commit() {
            return Err(e.into());
        }

        Ok(old_value)
    }
}
