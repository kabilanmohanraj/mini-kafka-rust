#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;
mod client;
mod types;
mod utils;

use broker::utils::init_broker;
use utils::build_api_version_map;

fn main() {
    // start broker service
    init_broker();
}
