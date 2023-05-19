use std::{
    error::Error,
    net::{SocketAddr, SocketAddrV4},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use anyhow;
use axum::{
    extract::{Path, State},
    handler::Handler,
    http::StatusCode,
    routing::get,
    Json, Router,
};
use clap::{Arg, ArgAction};
use redb::{Database, ReadableTable, TableDefinition};
use serde::Serialize;
use tokio::sync::RwLock;
use tower_http::{compression::CompressionLayer, validate_request::ValidateRequestHeaderLayer};

#[derive(Serialize)]
#[serde(untagged)]
enum Status {
    Read(ReadStatus),
    Write(WriteStatus),
}

#[derive(Serialize)]
enum ReadStatus {
    Found,
    Missing,
    Failure,
}

#[derive(Serialize)]
enum WriteStatus {
    SuccessNew,
    SuccessOverwrite,
    Failure,
}

#[derive(Serialize)]
struct Response {
    value: String,
    status: Status,
}

impl Response {
    fn new(value: String, status: Status) -> Self {
        Self { value, status }
    }

    fn from_read_error(e: impl Error) -> (StatusCode, Json<Response>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(Response::new(
                e.to_string(),
                Status::Read(ReadStatus::Failure),
            )),
        )
    }

    fn from_write_error(e: impl Error) -> (StatusCode, Json<Response>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(Response::new(
                e.to_string(),
                Status::Write(WriteStatus::Failure),
            )),
        )
    }
}

async fn read_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
) -> (StatusCode, Json<Response>) {
    let db = state.read().await;

    let tx = match db.database.begin_read() {
        Ok(v) => v,
        Err(e) => return Response::from_read_error(e),
    };

    let table = match tx.open_table(db.table) {
        Ok(v) => v,
        Err(e) => return Response::from_read_error(e),
    };

    // Done like this to satisfy the borrow checker
    let val = table.get(key.as_str());
    match val {
        Ok(Some(value)) => (
            StatusCode::OK,
            Json::from(Response::new(
                value.value().to_string(),
                Status::Read(ReadStatus::Found),
            )),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json::from(Response::new(
                "".to_string(),
                Status::Read(ReadStatus::Missing),
            )),
        ),
        Err(e) => Response::from_read_error(e),
    }
}

async fn write_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
    payload: String,
) -> (StatusCode, Json<Response>) {
    let db = state.read().await;

    let tx = match db.database.begin_write() {
        Ok(v) => v,
        Err(e) => return Response::from_write_error(e),
    };

    let resp: Response = {
        let mut table = match tx.open_table(db.table) {
            Ok(v) => v,
            Err(e) => return Response::from_write_error(e),
        };

        let res = table.insert(key.as_str(), payload.as_str());
        match res {
            Ok(Some(v)) => Response::new(
                v.value().to_string(),
                Status::Write(WriteStatus::SuccessOverwrite),
            ),
            Ok(None) => Response::new("".to_string(), Status::Write(WriteStatus::SuccessNew)),
            Err(e) => return Response::from_write_error(e),
        }
    };

    if let Err(e) = tx.commit() {
        return Response::from_write_error(e);
    }

    (StatusCode::OK, Json::from(resp))
}

struct Db<'a> {
    database: Database,
    table: TableDefinition<'a, &'static str, &'static str>,
}

#[derive(Clone)]
struct DbState<'a>(Arc<RwLock<Db<'a>>>);

impl<'a> DbState<'a> {
    fn new<T: AsRef<str>>(path: T, table_name: &'a str) -> anyhow::Result<Self> {
        let database = {
            if let Ok(v) = Database::open(path.as_ref()) {
                v
            } else {
                Database::create(path.as_ref()).unwrap()
            }
        };

        let table = TableDefinition::new(table_name);

        return Ok(DbState(Arc::new(RwLock::new(Db { database, table }))));
    }
}

impl<'a> Deref for DbState<'a> {
    type Target = Arc<RwLock<Db<'a>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for DbState<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const TABLE_NAME: &str = "table-name";
    const PORT: &str = "port";
    const DB_FILE_NAME: &str = "db-file-name";
    const TOKEN: &str = "token";

    let args = clap::Command::new("ywkv")
        .arg(
            Arg::new(TABLE_NAME)
                .long(TABLE_NAME)
                .required(false)
                .default_value("main")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new(PORT)
                .long(PORT)
                .required(false)
                .default_value("9958")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new(DB_FILE_NAME)
                .long(DB_FILE_NAME)
                .required(false)
                .default_value("ywkv.redb")
                .action(ArgAction::Set),
        )
        .arg(Arg::new(TOKEN).required(true).action(ArgAction::Set))
        .get_matches();

    let table_name = args.get_one::<String>(TABLE_NAME).unwrap();
    let port = args.get_one::<String>(PORT).unwrap();
    let db_file_name = args.get_one::<String>(DB_FILE_NAME).unwrap();
    let token = args.get_one::<String>(TOKEN).unwrap();

    // Intentionally leaking the String here in order to create a static TableDefinition at runtime
    let state = DbState::new(db_file_name, Box::leak(table_name.clone().into_boxed_str()))?;

    let app = Router::new().route(
        "/:key",
        get(read_key.layer(CompressionLayer::new()))
            .post(write_key)
            .layer(ValidateRequestHeaderLayer::bearer(token))
            .with_state(state),
    );

    async fn shutdown() {
        let ctrlc = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Ctrl+C handler failed");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrlc => {},
            _ = terminate => {}
        }

        println!("Starting graceful shutdown");
    }

    println!("Starting server!");

    axum::Server::bind(&SocketAddr::V4(SocketAddrV4::new(
        "0.0.0.0".parse()?,
        port.parse()?,
    )))
    .serve(app.into_make_service())
    .with_graceful_shutdown(shutdown())
    .await?;

    Ok(())
}
