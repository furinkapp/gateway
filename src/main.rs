use std::{env, error::Error, sync::Arc};

use furink_proto::version::VersionRequest;
use log::info;

use furink_proto::discovery::{
    discovery_service_client::DiscoveryServiceClient, RegisterRequest, ServiceKind,
};
use furink_proto::version::version_service_client::VersionServiceClient;
use furink_proto::VERSION;
use tokio::sync::RwLock;
use tonic::{transport::Channel, Request};
use url::Url;
use warp::Filter;

use crate::{
    context::Context,
    object::{build_schema, GraphQlContext},
};

mod context;
mod object;

#[tracing::instrument]
async fn connect_and_register() -> Result<DiscoveryServiceClient<Channel>, Box<dyn Error>> {
    // connect to discovery service
    info!("Connecting to service discovery...");
    let url = env::var("SERVICE_DISCOVERY_URL").expect("service discovery url must be provided");
    let channel = Channel::from_shared(url.clone())?.connect().await?;

    // check version
    let mut version_checker = VersionServiceClient::new(channel.clone());
    let version = version_checker
        .validate(Request::new(VersionRequest {
            version: VERSION.to_string(),
        }))
        .await?
        .into_inner();
    // ensure versions are thes ame
    if version.version != VERSION {
        return Err("mismatched version".into());
    }
    // register service
    let mut sd_client = DiscoveryServiceClient::new(channel);
    info!("Connected to service discovery");
    // lookup url from env
    let url = env::var("GATEWAY_URL").expect("gateway url must be provided");
    let url: Url = url.parse().expect("failed to parse service url");
    // register service
    sd_client
        .register(Request::new(RegisterRequest {
            kind: ServiceKind::Gateway as i32,
            address: url
                .host_str()
                .expect("gateway host must be provided")
                .to_string(),
            port: url.port().expect("gateway url must provide a port") as u32,
        }))
        .await?;
    // consume and return client
    Ok(sd_client)
}

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
    let discovery_client = connect_and_register().await?;
    // create the context
    let context = Context {
        discovery_client: RwLock::new(discovery_client),
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
    server.await;
    Ok(())
}
