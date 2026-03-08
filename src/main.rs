#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weather_checker::run().await
}
