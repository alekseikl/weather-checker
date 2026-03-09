use anyhow::anyhow;
use async_rs::Runtime;
use lapin::{Connection, ConnectionProperties};
use sqlx::postgres::PgPoolOptions;
use tracing::error;

use crate::checker::Checker;
use crate::notifier::Notifier;
use crate::rpc::Rpc;

pub mod checker;
pub mod notifier;
pub mod rpc;

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://try-loco:try-loco-pwd@localhost/weather")
        .await?;

    let amqp_conn = Connection::connect_with_runtime(
        "amqp://127.0.0.1:5672",
        ConnectionProperties::default()
            .enable_auto_recover()
            .with_connection_name("weather-checker".into()),
        Runtime::tokio_current().clone(),
    )
    .await?;

    let notifier = Notifier::new(&amqp_conn).await?;
    let checker = Checker::new(pool.clone(), notifier.clone());
    let rpc = Rpc::new(&amqp_conn, checker.clone()).await?;

    let checker_handle = tokio::spawn(async move { checker.run().await });
    let rpc_handle = tokio::spawn(async move { rpc.run().await });

    let error = tokio::select! {
        Ok(Err(err)) = checker_handle => err,
        Ok(Err(err)) = rpc_handle => err,
        else => anyhow!("Something went wrong")
    };

    error!(error = error.to_string());

    Err(error)
}
