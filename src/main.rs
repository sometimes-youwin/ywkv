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
enum WriteStatus {
    SuccessNew,
    SuccessOverwrite,
    Failure,
}

#[derive(Serialize)]
struct WriteResponse {
    value: String,
    status: WriteStatus,
}

impl WriteResponse {
    fn new(value: String, status: WriteStatus) -> Self {
        Self { value, status }
    }

    fn from_error(e: impl Error) -> (StatusCode, Json<WriteResponse>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(WriteResponse::new(e.to_string(), WriteStatus::Failure)),
        )
    }
}

#[derive(Serialize)]
enum ReadStatus {
    Found,
    Missing,
    Failure,
}

#[derive(Serialize)]
struct ReadResponse {
    value: String,
    status: ReadStatus,
}

impl ReadResponse {
    fn new(value: String, status: ReadStatus) -> Self {
        Self { value, status }
    }

    fn from_error(e: impl Error) -> (StatusCode, Json<ReadResponse>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json::from(ReadResponse::new(e.to_string(), ReadStatus::Failure)),
        )
    }
}

async fn read_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
) -> (StatusCode, Json<ReadResponse>) {
    let db = state.read().await;

    let tx = match db.database.begin_read() {
        Ok(v) => v,
        Err(e) => return ReadResponse::from_error(e),
    };

    let table = match tx.open_table(db.table) {
        Ok(v) => v,
        Err(e) => return ReadResponse::from_error(e),
    };

    // Done like this to satisfy the borrow checker
    let val = table.get(key.as_str());
    match val {
        Ok(Some(value)) => (
            StatusCode::OK,
            Json::from(ReadResponse::new(
                value.value().to_string(),
                ReadStatus::Found,
            )),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json::from(ReadResponse::new("".to_string(), ReadStatus::Missing)),
        ),
        Err(e) => ReadResponse::from_error(e),
    }
}

async fn write_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
    payload: String,
) -> (StatusCode, Json<WriteResponse>) {
    let db = state.read().await;

    let tx = match db.database.begin_write() {
        Ok(v) => v,
        Err(e) => return WriteResponse::from_error(e),
    };

    let resp: WriteResponse = {
        let mut table = match tx.open_table(db.table) {
            Ok(v) => v,
            Err(e) => return WriteResponse::from_error(e),
        };

        let res = table.insert(key.as_str(), payload.as_str());
        match res {
            Ok(Some(v)) => WriteResponse::new(v.value().to_string(), WriteStatus::SuccessOverwrite),
            Ok(None) => WriteResponse::new("".to_string(), WriteStatus::SuccessNew),
            Err(e) => return WriteResponse::from_error(e),
        }
    };

    if let Err(e) = tx.commit() {
        return WriteResponse::from_error(e);
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
    let args = clap::Command::new("ywkv")
        .arg(
            Arg::new("table-name")
                .long("table-name")
                .required(false)
                .default_value("main")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("port")
                .long("port")
                .required(false)
                .default_value("9958")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("db-file-name")
                .long("db-file-name")
                .required(false)
                .default_value("ywkv.redb")
                .action(ArgAction::Set),
        )
        .arg(Arg::new("token").required(true).action(ArgAction::Set))
        .get_matches();

    let table_name = args.get_one::<String>("table-name").unwrap();
    let port = args.get_one::<String>("port").unwrap();
    let db_file_name = args.get_one::<String>("db-file-name").unwrap();
    let token = args.get_one::<String>("token").unwrap();

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
