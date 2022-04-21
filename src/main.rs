use std::{env, error::Error, sync::Arc, time::Duration};

use furink_proto::version::{spawn_heartbeat_task, validate_and_register, HeartbeatConfig};
use log::info;

use furink_proto::discovery::{
    discovery_service_client::DiscoveryServiceClient, RegisterRequest, ServiceKind,
};
use tokio::sync::RwLock;
use tonic::transport::Endpoint;
use warp::Filter;

use crate::{
    context::Context,
    object::{build_schema, GraphQlContext},
};

mod context;
mod object;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // load dotenv when in development
    if cfg!(debug_assertions) {
        dotenv::dotenv().unwrap();
    }
    println!(
        r#"
{} v{}
Authors: {}
"#,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("CARGO_PKG_AUTHORS")
    );
    // setup logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    // register with discovery service
    let (id, channel) = validate_and_register(
        Endpoint::from_shared(env::var("DISCOVERY_URL").expect("DISCOVERY_URL"))?,
        RegisterRequest {
            kind: ServiceKind::Gateway as i32,
            address: env::var("SERVICE_HOST").expect("SERVICE_HOST"),
            port: env::var("SERVICE_PORT").expect("SERVICE_PORT").parse()?,
        },
    )
    .await?;
	// spawn heartbeat
    spawn_heartbeat_task(HeartbeatConfig {
        channel: channel.clone(),
        id: id.clone(),
        interval: Duration::from_secs(30),
    });
    // create the context
    let context = Context {
        discovery_client: RwLock::new(DiscoveryServiceClient::new(channel.clone())),
    };
    // make the context thread-safe
    let context = Arc::new(context);
    // setup context filters
    let warp_ctx = warp::any().map(move || context.clone());
    let graphql_ctx = warp_ctx.map(|context: Arc<Context>| GraphQlContext { inner: context });
    let log = warp::log("gateway");
    // create the graphql filter
    let graphql_filter = juniper_warp::make_graphql_filter(build_schema(), graphql_ctx.boxed());
    // create server and bind
    let (_, server) = warp::serve(
        warp::any()
            .and(warp::path("graphql").and(graphql_filter))
            .with(log),
    )
    .try_bind_ephemeral(([127, 0, 0, 1], 8080))?;
    // listen
    info!("Listening on http://127.0.0.1:8080");
    server.await;
    Ok(())
}
