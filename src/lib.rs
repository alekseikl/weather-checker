use std::sync::Arc;

use anyhow::{anyhow, bail};
use async_rs::Runtime;
use chrono::{DateTime, Utc};
use lapin::{Connection, ConnectionProperties};
use reqwest::StatusCode;
use sqlx::{
    PgPool,
    postgres::{PgPoolOptions, types::PgPoint},
};
use tokio::time;
use tracing::{error, info, warn};

use crate::notifier::Notifier;
use crate::rpc::Rpc;

pub mod notifier;
pub mod rpc;

fn serialize_pg_point<S: serde::Serializer>(
    point: &PgPoint,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    let mut s = serializer.serialize_struct("PgPoint", 2)?;
    s.serialize_field("lat", &point.x)?;
    s.serialize_field("lon", &point.y)?;
    s.end()
}

#[derive(sqlx::FromRow, serde::Serialize)]
pub struct Location {
    id: i32,
    name: String,
    #[serde(serialize_with = "serialize_pg_point")]
    coords: PgPoint,
    alert_threshold: f32,
    prev_temperature: Option<f32>,
    updated_at: Option<DateTime<Utc>>,
}

pub async fn load_locations(pool: &PgPool) -> Result<Vec<Location>, Box<dyn std::error::Error>> {
    let locations = sqlx::query_as::<_, Location>("SELECT * FROM locations")
        .fetch_all(pool)
        .await?;

    Ok(locations)
}

async fn fetch_temperature(location: &Location) -> anyhow::Result<f32> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&current=temperature_2m",
        location.coords.x, location.coords.y
    );
    let response = reqwest::get(url).await?;

    if !matches!(response.status(), StatusCode::OK) {
        bail!("Failed to fetch data.");
    }

    let body = response.text().await?;
    let json: serde_json::Value = serde_json::from_str(&body)?;
    let temperature = json["current"]["temperature_2m"]
        .as_f64()
        .map(|temp| temp as f32)
        .unwrap_or_default();

    Ok(temperature)
}

async fn check_weather(
    pool: &PgPool,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let locations = load_locations(pool).await?;

    for location in &locations {
        let temp = fetch_temperature(location).await?;

        if let Some(prev_temp) = location.prev_temperature {
            let prev_beyond = prev_temp < location.alert_threshold;
            let current_beyond = temp < location.alert_threshold;

            if current_beyond && !prev_beyond {
                info!(
                    "{} temperature ({}) is beyond threshold",
                    location.name, temp
                );

                notifier.beyond_threshold(temp, location).await?;
            } else if !current_beyond && prev_beyond {
                info!(
                    "{} temperature ({}) returned to normal",
                    location.name, temp
                );

                notifier.returned_to_normal(temp, location).await?;
            } else {
                notifier.temp_updated(temp, location).await?;
            }
        }

        let mut tx = pool.begin().await?;

        sqlx::query("UPDATE locations SET prev_temperature = $1, updated_at = $2 WHERE id = $3")
            .bind(temp)
            .bind(Utc::now())
            .bind(location.id)
            .execute(&mut *tx)
            .await?;

        if let Some(prev_temp) = location.prev_temperature
            && (prev_temp - temp).abs() < 0.0001
        {
            sqlx::query("INSERT INTO samples (location_id, temperature) VALUES ($1, $2)")
                .bind(location.id)
                .bind(temp)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        info!("{} - {}", location.name, temp);
    }

    Ok(())
}

pub async fn run_wether_checker(pool: PgPool, notifier: Arc<Notifier>) -> anyhow::Result<()> {
    let mut failure_counter = 0usize;

    loop {
        match check_weather(&pool, &notifier).await {
            Result::Ok(_) => {
                failure_counter = 0;
                info!("Check succeeded");
            }
            Result::Err(error) => {
                failure_counter += 1;
                warn!("Check failed: {}", error);
            }
        }

        if failure_counter > 5 {
            bail!("Failure count reached.");
        }

        time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://try-loco:try-loco-pwd@localhost/weather")
        .await?;

    let runtime = Runtime::tokio_current();

    let amqp_conn = Connection::connect_with_runtime(
        "amqp://127.0.0.1:5672",
        ConnectionProperties::default()
            .enable_auto_recover()
            .with_connection_name("weather-checker".into()),
        runtime.clone(),
    )
    .await?;

    let notifier = Notifier::new(&amqp_conn).await?;
    let rpc = Rpc::new(&amqp_conn, pool.clone()).await?;

    let checker_handle = tokio::spawn({
        let checker_pool = pool.clone();
        let notifier = notifier.clone();

        async { run_wether_checker(checker_pool, notifier).await }
    });

    let rpc_handle = tokio::spawn({
        let rpc = rpc.clone();

        async move { rpc.run().await }
    });

    let error = tokio::select! {
        Ok(Err(err)) = checker_handle => err,
        Ok(Err(err)) = rpc_handle => err,
        else => anyhow!("Something went wrong")
    };

    error!(error = error.to_string());

    Err(error)
}
