use std::{
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
use redb::{Database, TableDefinition};
use tokio::sync::RwLock;
use tower_http::{compression::CompressionLayer, validate_request::ValidateRequestHeaderLayer};

use ywkv::{self, Db, Response, YwkvError};

async fn read_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
) -> (StatusCode, Json<Response>) {
    let state = state.read().await;

    match state.read(key) {
        Ok(value) => (
            StatusCode::OK,
            Json::from(Response::new(
                value,
                ywkv::Status::Read(ywkv::ReadStatus::Found),
            )),
        ),
        Err(e) => match e {
            YwkvError::KeyMissing(_) => (
                StatusCode::NOT_FOUND,
                Json::from(Response::new(
                    e.to_string(),
                    ywkv::Status::Read(ywkv::ReadStatus::Missing),
                )),
            ),
            YwkvError::EmptyTable(_) => (
                StatusCode::NOT_FOUND,
                Json::from(Response::new(
                    e.to_string(),
                    ywkv::Status::Read(ywkv::ReadStatus::Missing),
                )),
            ),
            _ => Response::from_read_error(e),
        },
    }
}

async fn write_key(
    Path(key): Path<String>,
    State(state): State<DbState<'_>>,
    payload: String,
) -> (StatusCode, Json<Response>) {
    let state = state.write().await;

    match state.write(key, payload) {
        Ok(Some(old_value)) => (
            StatusCode::CREATED,
            Json::from(Response::new(
                old_value,
                ywkv::Status::Write(ywkv::WriteStatus::SuccessOverwrite),
            )),
        ),
        Ok(None) => (
            StatusCode::CREATED,
            Json::from(Response::new(
                String::new(),
                ywkv::Status::Write(ywkv::WriteStatus::SuccessNew),
            )),
        ),
        Err(e) => Response::from_read_error(e),
    }
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
    type Target = Arc<RwLock<ywkv::Db<'a>>>;

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
