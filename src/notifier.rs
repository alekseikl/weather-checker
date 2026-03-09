use std::sync::Arc;

use lapin::{
    BasicProperties, Channel, Connection,
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
};
use serde::Serialize;

use crate::checker::Location;

const EXCHANGE_NAME: &str = "weather.events";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ThresholdEventInfo<'a> {
    location_id: i32,
    name: &'a str,
    alert_threshold: f32,
    temperature: f32,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum ThresholdEvent<'a> {
    TempUpdated(ThresholdEventInfo<'a>),
    ThresholdCrossed(ThresholdEventInfo<'a>),
    ReturnedToNormal(ThresholdEventInfo<'a>),
}

pub struct Notifier {
    channel: Channel,
}

impl Notifier {
    pub async fn new(connection: &Connection) -> anyhow::Result<Arc<Self>> {
        let channel = connection.create_channel().await?;

        channel
            .exchange_declare(
                EXCHANGE_NAME.into(),
                lapin::ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Arc::new(Self { channel }))
    }
    pub async fn temp_updated(
        &self,
        temperature: f32,
        location: &Location,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = ThresholdEvent::TempUpdated(ThresholdEventInfo {
            location_id: location.id,
            name: &location.name,
            alert_threshold: location.alert_threshold,
            temperature,
        });

        self.channel
            .basic_publish(
                EXCHANGE_NAME.into(),
                format!("weather.{}.temp_updated", location.id).into(),
                BasicPublishOptions::default(),
                serde_json::to_string(&payload)?.as_bytes(),
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?
            .await?;

        Ok(())
    }

    pub async fn beyond_threshold(
        &self,
        temperature: f32,
        location: &Location,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = ThresholdEvent::ThresholdCrossed(ThresholdEventInfo {
            location_id: location.id,
            name: &location.name,
            alert_threshold: location.alert_threshold,
            temperature,
        });

        self.channel
            .basic_publish(
                EXCHANGE_NAME.into(),
                format!("weather.{}.beyond-threshold", location.id).into(),
                BasicPublishOptions::default(),
                serde_json::to_string(&payload)?.as_bytes(),
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?
            .await?;

        Ok(())
    }

    pub async fn returned_to_normal(
        &self,
        temperature: f32,
        location: &Location,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = ThresholdEvent::ReturnedToNormal(ThresholdEventInfo {
            location_id: location.id,
            name: &location.name,
            alert_threshold: location.alert_threshold,
            temperature,
        });

        self.channel
            .basic_publish(
                EXCHANGE_NAME.into(),
                format!("weather.{}.returned-to-normal", location.id).into(),
                BasicPublishOptions::default(),
                serde_json::to_string(&payload)?.as_bytes(),
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?
            .await?;

        Ok(())
    }
}
