use actix_web::{web, App, HttpServer};

mod common;
mod error;
#[allow(unused_variables)]
mod handler;
mod routes;
#[allow(unused_variables)]
mod state;
mod utils;

use routes::make_route;
use state::GlobalState;
use utils::get_config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = get_config();
    if config.is_err() {
        println!("!!! Environment Error: {:?}", config);
        std::process::exit(1);
    }

    let config = config.unwrap();
    let global_state = web::Data::new(GlobalState::new(&config.db_path));

    HttpServer::new(move || {
        App::new()
            .app_data(global_state.clone())
            .service(make_route())
    })
    .bind(&config.ip_port)
    .expect(&format!("Can't bind {}", &config.ip_port))
    .workers(config.workers)
    .run()
    .await
}
