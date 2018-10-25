#[macro_use]
extern crate log;
extern crate clap;
extern crate nix;
extern crate rngine;
extern crate signal;

use std::fs::File;
use std::path::Path;
use std::process;
use std::sync::Arc;

use clap::{App, Arg};
use fs2::FileExt;

use rngine::config::RgConfig;
use rngine::engine::Engine;
use rngine::rocksdb_util;
use rngine::server::{Server, Service};

fn main() {
    let matches = App::new("Rngine")
        .author("TiKV Org.")
        .about("A Distributed transactional key-value database powered by Rust and Raft")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Sets config file")
                .takes_value(true),
        ).arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Sets listening address"),
        ).arg(
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        ).get_matches();

    if matches.is_present("print-sample-config") {
        let config = RgConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }
    let config = matches
        .value_of("config")
        .map_or_else(RgConfig::default, |path| RgConfig::from_file(&path));

    let path = Path::new(&config.db_path);
    let lock_path = path.join("LOCK");

    let f = File::create(lock_path.as_path())
        .unwrap_or_else(|e| panic!("failed to create lock at {}: {:?}", lock_path.display(), e));
    if f.try_lock_exclusive().is_err() {
        panic!(
            "lock {:?} failed, maybe another instance is using this directory.",
            path
        );
    }

    // Create kv engine, storage.
    let kv_db_opts = config.rocksdb.build_opt();
    let kv_cfs_opts = config.rocksdb.build_cf_opts();
    let db = Arc::new(
        rocksdb_util::new_engine_opt(path.to_str().unwrap(), kv_db_opts, kv_cfs_opts)
            .unwrap_or_else(|s| panic!("failed to create kv engine: {:?}", s)),
    );
    let mut engine = Engine::new(db);
    engine.start();
    let service = Service::new(&mut engine);
    let _server = Server::start(&config.address, service);

    handle_signal();

    engine.stop();
}

fn handle_signal() {
    use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM};
    use signal::trap::Trap;
    let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP]);
    for sig in trap {
        match sig {
            SIGTERM | SIGINT | SIGHUP => {
                info!("receive signal {:?}, stopping server...", sig);
                break;
            }
            _ => unreachable!(),
        }
    }
}
