use std::sync::Arc;

use anyhow::{anyhow, bail};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use sqlx::{PgPool, postgres::types::PgPoint};
use tokio::{sync::broadcast, time};
use tracing::{info, warn};

use crate::notifier::Notifier;

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
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub id: i32,
    pub name: String,
    #[serde(serialize_with = "serialize_pg_point")]
    pub coords: PgPoint,
    pub alert_threshold: f32,
    pub prev_temperature: Option<f32>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct Checker {
    pool: PgPool,
    notifier: Arc<Notifier>,
}

impl Checker {
    pub fn new(pool: PgPool, notifier: Arc<Notifier>) -> Arc<Self> {
        Arc::new(Self { pool, notifier })
    }

    pub async fn load_location(&self, location_id: i32) -> anyhow::Result<Option<Location>> {
        let location = sqlx::query_as::<_, Location>("SELECT * FROM locations WHERE id = $1")
            .bind(location_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(location)
    }

    pub async fn load_locations(&self) -> anyhow::Result<Vec<Location>> {
        let locations = sqlx::query_as::<_, Location>("SELECT * FROM locations")
            .fetch_all(&self.pool)
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

    async fn check_weather(&self) -> Result<(), Box<dyn std::error::Error>> {
        let locations = self.load_locations().await?;

        for location in &locations {
            let temp = Self::fetch_temperature(location).await?;

            if let Some(prev_temp) = location.prev_temperature {
                let prev_beyond = prev_temp < location.alert_threshold;
                let current_beyond = temp < location.alert_threshold;

                if current_beyond && !prev_beyond {
                    info!(
                        "{} temperature ({}) is beyond threshold",
                        location.name, temp
                    );

                    self.notifier.beyond_threshold(temp, location).await?;
                } else if !current_beyond && prev_beyond {
                    info!(
                        "{} temperature ({}) returned to normal",
                        location.name, temp
                    );

                    self.notifier.returned_to_normal(temp, location).await?;
                } else {
                    self.notifier.temp_updated(temp, location).await?;
                }
            }

            let mut tx = self.pool.begin().await?;

            sqlx::query(
                "UPDATE locations SET prev_temperature = $1, updated_at = $2 WHERE id = $3",
            )
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

    pub async fn run(&self, mut shutdown_rx: broadcast::Receiver<()>) -> anyhow::Result<()> {
        let mut failure_counter = 0usize;

        let run_loop = async {
            loop {
                match self.check_weather().await {
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
                    break anyhow!("Failure count reached.");
                }

                time::sleep(std::time::Duration::from_secs(60)).await;
            }
        };

        tokio::select! {
            error = run_loop => Err(error),
            _ = shutdown_rx.recv() => Ok(())
        }
    }
}
