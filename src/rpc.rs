use std::sync::Arc;

use futures_lite::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use sqlx::PgPool;
use tracing::{error, info};

use crate::load_locations;

const QUEUE_NAME: &str = "weather-rpc";

pub struct Rpc {
    channel: Channel,
    pool: PgPool,
}

impl Rpc {
    pub async fn new(connection: &Connection, pool: PgPool) -> anyhow::Result<Arc<Self>> {
        let channel = connection.create_channel().await?;

        channel
            .queue_declare(
                QUEUE_NAME.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Arc::new(Self { channel, pool }))
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let consumer = self
            .channel
            .basic_consume(
                QUEUE_NAME.into(),
                "weather-rpc-server".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("RPC server listening on queue '{}'", QUEUE_NAME);

        tokio::pin!(consumer);

        while let Some(delivery) = consumer.next().await {
            let delivery = match delivery {
                Ok(d) => d,
                Err(e) => {
                    error!("Error receiving delivery: {}", e);
                    continue;
                }
            };

            let (Some(reply_to), Some(correlation_id)) = (
                delivery.properties.reply_to(),
                delivery.properties.correlation_id(),
            ) else {
                error!("Missing reply_to/correlation_id in RPC request");
                delivery.ack(BasicAckOptions::default()).await?;
                continue;
            };

            let response = match load_locations(&self.pool).await {
                Ok(locations) => serde_json::to_vec(&locations).unwrap_or_default(),
                Err(e) => {
                    let err = serde_json::json!({ "error": e.to_string() });
                    serde_json::to_vec(&err).unwrap_or_default()
                }
            };

            self.channel
                .basic_publish(
                    "".into(),
                    reply_to.clone(),
                    BasicPublishOptions::default(),
                    &response,
                    BasicProperties::default()
                        .with_correlation_id(correlation_id.clone())
                        .with_content_type("application/json".into()),
                )
                .await?
                .await?;

            delivery.ack(BasicAckOptions::default()).await?;
        }

        Ok(())
    }
}
