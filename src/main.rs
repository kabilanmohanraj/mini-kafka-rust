#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;
mod client;
mod primitive_types;
mod utils;
mod traits;
mod errors;
mod api_versions;

use std::sync::Arc;

use broker::broker::Broker;

fn main() {

    // start broker service

    // create a new broker
    let kbroker = match Broker::new("127.0.0.1:9092", 5) {
        Ok(broker) => Arc::new(broker),
        Err(e) => {
            eprintln!("Error creating broker: {}", e);
            return;
        }
    };

    // start the broker and accept new connections
    match kbroker.accept_new_connections() {
        Ok(_) => println!("Broker started successfully"),
        Err(e) => eprintln!("Error starting broker: {}", e),
    }

}