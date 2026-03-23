use anyhow::anyhow;
use async_rs::Runtime;
use lapin::{Connection, ConnectionProperties};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::broadcast;
use tokio::{signal, time};
use tracing::{error, info};

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

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let notifier = Notifier::new(&amqp_conn).await?;
    let checker = Checker::new(pool.clone(), notifier.clone());
    let rpc = Rpc::new(&amqp_conn, checker.clone()).await?;

    let checker_handle = tokio::spawn(async move { checker.run(shutdown_rx).await });
    let rpc_handle = tokio::spawn({
        let shutdown_rx = shutdown_tx.subscribe();

        async move { rpc.run(shutdown_rx).await }
    });

    tokio::pin!(checker_handle);
    tokio::pin!(rpc_handle);

    let wait_for_tasks = async {
        tokio::select! {
            Ok(Err(err)) = &mut checker_handle => err,
            Ok(Err(err)) = &mut rpc_handle => err,
            else => anyhow!("Something went wrong")
        }
    };

    tokio::select! {
        error = wait_for_tasks => {
            error!(error = error.to_string(), "Task failure. App terminated.");
            return Err(error);
        }
        _ = shutdown_signal() => {}
    }

    let _ = shutdown_tx.send(());

    info!("Shutting down");

    let wait_for_tasks = async { tokio::join!(checker_handle, rpc_handle) };

    tokio::select! {
        _ = wait_for_tasks => {
            info!("Shut down");
            Ok(())
        }
        _ = time::sleep(std::time::Duration::from_secs(10)) => {
            error!("Shutdown timeout reached");
            Err(anyhow!("Shutdown timeout reached"))
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
