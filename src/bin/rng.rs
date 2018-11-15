#[macro_use]
extern crate log;
extern crate backtrace;
extern crate clap;
extern crate env_logger;
extern crate nix;
extern crate rngine;
extern crate signal;
#[macro_use(slog_o)]
extern crate slog;

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{panic, process, thread};

use clap::{App, Arg, ArgMatches};
use fs2::FileExt;
use slog::{Drain, Logger};
use slog_async::{Async, OverflowStrategy};
use slog_scope::GlobalLoggerGuard;
use slog_term::{PlainDecorator, TermDecorator};

use rngine::config::RgConfig;
use rngine::engine::Engine;
use rngine::logger;
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
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Sets listening address"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Sets the path to store directory"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Sets log file. If not set, output log to stderr"),
        )
        .arg(
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        )
        .get_matches();

    if matches.is_present("print-sample-config") {
        let config = RgConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }
    let mut config = matches
        .value_of("config")
        .map_or_else(RgConfig::default, |path| RgConfig::from_file(&path));
    overwrite_config_with_cmd_args(&mut config, &matches);

    // Install logger.
    let guard = init_log(&config);

    info!("Welcome to Rng");
    info!("Config:\n{}", toml::to_string_pretty(&config).unwrap());

    // Install panic hook. Abort on panic.
    set_panic_hook(true, guard);

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
    engine.start(&config);
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

fn overwrite_config_with_cmd_args(config: &mut RgConfig, matches: &ArgMatches) {
    if let Some(addr) = matches.value_of("addr") {
        config.address = addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.path = data_dir.to_owned();
    }

    if let Some(log_file) = matches.value_of("log-file") {
        config.log_file = log_file.to_owned();
    }
}

// Exit the whole process when panic.
fn set_panic_hook(panic_abort: bool, guard: GlobalLoggerGuard) {
    // HACK! New a backtrace ahead for caching necessary elf sections of this
    // tikv-server, in case it can not open more files during panicking
    // which leads to no stack info (0x5648bdfe4ff2 - <no info>).
    //
    // Crate backtrace caches debug info in a static variable `STATE`,
    // and the `STATE` lives forever once it has been created.
    // See more: https://github.com/alexcrichton/backtrace-rs/blob/\
    //           597ad44b131132f17ed76bf94ac489274dd16c7f/\
    //           src/symbolize/libbacktrace.rs#L126-L159
    // Caching is slow, spawn it in another thread to speed up.
    thread::Builder::new()
        .name("backtrace-loader".to_owned())
        .spawn(::backtrace::Backtrace::new)
        .unwrap();

    // Hold the guard.
    let log_guard = Mutex::new(Some(guard));

    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info: &panic::PanicInfo| {
        if log_enabled!(::log::LogLevel::Error) {
            let msg = match info.payload().downcast_ref::<&'static str>() {
                Some(s) => *s,
                None => match info.payload().downcast_ref::<String>() {
                    Some(s) => &s[..],
                    None => "Box<Any>",
                },
            };
            let thread = thread::current();
            let name = thread.name().unwrap_or("<unnamed>");
            let loc = info
                .location()
                .map(|l| format!("{}:{}", l.file(), l.line()));
            let bt = ::backtrace::Backtrace::new();
            error!(
                "thread '{}' panicked '{}' at {:?}\n{:?}",
                name,
                msg,
                loc.unwrap_or_else(|| "<unknown>".to_owned()),
                bt
            );
        } else {
            orig_hook(info);
        }

        // To collect remaining logs, drop the guard before exit.
        drop(log_guard.lock().unwrap().take());

        if panic_abort {
            process::abort();
        } else {
            process::exit(1);
        }
    }))
}

pub fn init_log(config: &RgConfig) -> GlobalLoggerGuard {
    // Default is 128.
    // Extended since blocking is set, and we don't want to block very often.
    const SLOG_CHANNEL_SIZE: usize = 10240;
    // Default is DropAndReport.
    // It is not desirable to have dropped logs in our use case.
    const SLOG_CHANNEL_OVERFLOW_STRATEGY: OverflowStrategy = OverflowStrategy::Block;

    let log_rotation_timespan =
        chrono::Duration::from_std(config.log_rotation_timespan.clone().into())
            .expect("config.log_rotation_timespan is an invalid duration.");

    // TODO: add it config.
    let log_level = slog::Level::Debug;

    let guard = if config.log_file.is_empty() {
        let decorator = TermDecorator::new().build();
        let drain = logger::TikvFormat::new(decorator).fuse();
        let drain = Async::new(drain)
            .chan_size(SLOG_CHANNEL_SIZE)
            .overflow_strategy(SLOG_CHANNEL_OVERFLOW_STRATEGY)
            .thread_name("term-slogger".to_owned())
            .build()
            .fuse();
        let logger = Logger::root_typed(drain, slog_o!());
        logger::init_log(logger, log_level).unwrap_or_else(|e| {
            panic!("failed to initialize log: {:?}", e);
        })
    } else {
        let logger = BufWriter::new(
            logger::RotatingFileLogger::new(&config.log_file, log_rotation_timespan)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to initialize log with file {:?}: {:?}",
                        config.log_file, e
                    );
                }),
        );
        let decorator = PlainDecorator::new(logger);
        let drain = logger::TikvFormat::new(decorator).fuse();
        let drain = Async::new(drain)
            .chan_size(SLOG_CHANNEL_SIZE)
            .overflow_strategy(SLOG_CHANNEL_OVERFLOW_STRATEGY)
            .thread_name("file-slogger".to_owned())
            .build()
            .fuse();
        let logger = Logger::root_typed(drain, slog_o!());
        logger::init_log(logger, log_level).unwrap_or_else(|e| {
            panic!("failed to initialize log: {:?}", e);
        })
    };
    guard
}
