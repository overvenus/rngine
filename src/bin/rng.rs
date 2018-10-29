#[macro_use]
extern crate log;
extern crate clap;
extern crate env_logger;
extern crate nix;
extern crate rngine;
extern crate signal;

use std::fs::{self, File};
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
        .about("A remote storage engine for TiKV")
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

    // Install logger.
    let env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::Builder::from_env(env).init();

    let root_path = Path::new(&config.path);
    // Create root directory if missing.
    if let Err(e) = fs::create_dir_all(&root_path) {
        error!(
            "create parent directory {} failed, err {:?}",
            root_path.to_str().unwrap(),
            e
        );
        process::exit(1);
    }

    let lock_path = root_path.join("LOCK");
    let f = File::create(lock_path.as_path())
        .unwrap_or_else(|e| panic!("failed to create lock at {}: {:?}", lock_path.display(), e));
    if f.try_lock_exclusive().is_err() {
        panic!(
            "lock {:?} failed, maybe another instance is using this directory.",
            root_path
        );
    }

    // Create kv engine, storage.
    let db_path = root_path.join("db");
    let kv_db_opts = config.rocksdb.build_opt();
    let kv_cfs_opts = config.rocksdb.build_cf_opts();
    let db = Arc::new(
        rocksdb_util::new_engine_opt(db_path.to_str().unwrap(), kv_db_opts, kv_cfs_opts)
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
