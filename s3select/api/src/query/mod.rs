pub mod ast;
pub mod datasource;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod logical_planner;
pub mod parser;
pub mod session;

#[derive(Clone)]
pub struct Context {
    // maybe we need transfer some info?
}

#[derive(Clone)]
pub struct Query {
    context: Context,
    content: String,
}
