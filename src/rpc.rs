use std::sync::Arc;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use lapin::{
    BasicProperties, Channel, Connection,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use tokio::sync::{Semaphore, broadcast};
use tracing::{error, info};

use crate::checker::Checker;

const QUEUE_NAME: &str = "weather-rpc";
const MAX_CONCURRENT_REQUESTS: u32 = 8;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcRequest {
    method: String,
    arguments: serde_json::Value,
}

#[derive(Serialize)]
#[serde(tag = "status", content = "data", rename_all = "kebab-case")]
enum RpcResult {
    Ok(serde_json::Value),
    Error(String),
}

pub struct Rpc {
    channel: Channel,
    checker: Arc<Checker>,
    backpressure_sem: Arc<Semaphore>,
}

impl Rpc {
    pub async fn new(connection: &Connection, checker: Arc<Checker>) -> anyhow::Result<Arc<Self>> {
        let channel = connection.create_channel().await?;

        channel
            .queue_declare(
                QUEUE_NAME.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Arc::new(Self {
            channel,
            checker,
            backpressure_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS as usize)),
        }))
    }

    async fn get_location(&self, args: serde_json::Value) -> anyhow::Result<RpcResult> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Args {
            location_id: i32,
        }

        let args: Args = serde_json::from_value(args)?;

        match self.checker.load_location(args.location_id).await? {
            Some(location) => Ok(RpcResult::Ok(serde_json::to_value(&location)?)),
            None => Ok(RpcResult::Error("Location not found".into())),
        }
    }

    async fn get_locations(&self, _args: serde_json::Value) -> anyhow::Result<RpcResult> {
        let locations = self.checker.load_locations().await?;

        Ok(RpcResult::Ok(serde_json::to_value(&locations)?))
    }

    async fn execute_request(&self, request_data: &[u8]) -> anyhow::Result<serde_json::Value> {
        let request: RpcRequest = match serde_json::from_slice(request_data) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to deserialize RPC request: {}", e);
                bail!("Bad request");
            }
        };

        let args = request.arguments;

        info!(method = %request.method, "RPC method call");

        let result = match request.method.as_str() {
            "get-location" => self.get_location(args).await?,
            "get-locations" => self.get_locations(args).await?,
            _ => RpcResult::Error("Method not found".into()),
        };

        Ok(serde_json::to_value(result)?)
    }

    pub async fn process_delivery(&self, delivery: Delivery) -> anyhow::Result<()> {
        let (Some(reply_to), Some(correlation_id)) = (
            delivery.properties.reply_to(),
            delivery.properties.correlation_id(),
        ) else {
            error!("Missing reply_to/correlation_id in RPC request");
            delivery.ack(BasicAckOptions::default()).await?;
            bail!("Bad request");
        };

        let response = match self.execute_request(&delivery.data).await {
            Ok(value) => value,
            Err(_) => serde_json::to_value(RpcResult::Error("Bad request".into()))?,
        };

        self.channel
            .basic_publish(
                "".into(),
                reply_to.clone(),
                BasicPublishOptions::default(),
                &serde_json::to_vec(&response).unwrap_or_default(),
                BasicProperties::default()
                    .with_correlation_id(correlation_id.clone())
                    .with_content_type("application/json".into()),
            )
            .await?
            .await?;

        delivery.ack(BasicAckOptions::default()).await?;

        Ok(())
    }

    pub async fn run(
        self: &Arc<Self>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
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

        let run_loop = async {
            while let Some(delivery) = consumer.next().await {
                let delivery = match delivery {
                    Ok(d) => d,
                    Err(e) => {
                        error!("Error receiving delivery: {}", e);
                        continue;
                    }
                };

                let rpc = self.clone();
                let permit = self.backpressure_sem.clone().acquire_owned().await.unwrap();

                tokio::spawn(async move {
                    if let Err(error) = rpc.process_delivery(delivery).await {
                        error!("Error processing delivery: {}", error);
                    }

                    drop(permit);
                });
            }
        };

        tokio::select! {
            _ = run_loop => {},
            _ = shutdown_rx.recv() => {}
        }

        let _ = self
            .backpressure_sem
            .acquire_many(MAX_CONCURRENT_REQUESTS)
            .await;

        Ok(())
    }
}
