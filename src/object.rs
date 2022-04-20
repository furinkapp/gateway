//! Defines the GraphQL schema, context, and objects.

use juniper::{graphql_object, EmptyMutation, EmptySubscription, RootNode};

use crate::context::ThreadContext;

/// The GraphQL query marker struct.
#[derive(Clone, Copy, Debug)]
pub struct Query;

/// GraphQL query implementation.
#[graphql_object(context = GraphQlContext)]
impl Query {
    async fn users() -> Vec<String> {
        todo!()
    }
}

/// Wrapper type for the thread-safe context.
#[derive(Clone, Debug)]
pub struct GraphQlContext {
    pub inner: ThreadContext,
}

impl juniper::Context for GraphQlContext {}

/// The GraphQL schema type.
pub type Schema =
    RootNode<'static, Query, EmptyMutation<GraphQlContext>, EmptySubscription<GraphQlContext>>;

/// Builds the GraphQL schema and returns it.
pub fn build_schema() -> Schema {
    Schema::new(
        Query,
        EmptyMutation::<GraphQlContext>::new(),
        EmptySubscription::<GraphQlContext>::new(),
    )
}
