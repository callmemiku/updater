#[macro_use]
extern crate actix_web;
extern crate env_logger;
use std::{env, io};
use actix_web::{middleware, App, HttpServer};
mod update;
mod test;
mod errors;
mod models;

#[actix_rt::main]
async fn main() -> io::Result<()> {
    env::set_var("RUST_LOG", "actix_web=debug,actix_server=info");
    env_logger::init();
    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(update::update)
            .service(update::test)
            .service(update::batches)
            .service(update::batches_selective)
            .service(update::update_from_view)
    })
        .bind("0.0.0.0:9090")?
        .run()
        .await
}
