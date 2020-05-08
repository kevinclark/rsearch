use std::env;
use clap::{crate_authors, crate_description, crate_name, crate_version, App};

fn main() {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
    .get_matches();
}
