mod update;

use std::{env, io};

#[actix_rt::main]
async fn main() -> io::Result<()> {
    env::set_var("RUST_LOG", "actix_web=debug,actix_server=info");
    env_logger::init();
    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(update)
    })
        .bind("0.0.0.0:9090")?
        .run()
        .await
}
