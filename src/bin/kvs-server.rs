use clap::{arg, Command};
use kvs::engines::sled::SledStore;
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool};
use kvs::KvServer;
use kvs::{Command as kCommand, KvStore, KvsEngine, Record, Result};
use kvs::{NaiveThreadPool, ThreadPool};
use log::{debug, error, info, warn};
use std::{env::current_dir, process::exit};
use stderrlog::{self, LogLevelNum, Timestamp};

fn main() -> Result<()> {
    stderrlog::new()
        .module("kvs")
        .module(module_path!())
        .timestamp(Timestamp::Second)
        .verbosity(LogLevelNum::Debug)
        .init()
        .unwrap();

    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .disable_help_subcommand(true)
        .args(
            [
                arg!(-a --addr <IPADDR> "Accepts an IP address to be listened on, 
                either v4 or v6, and a port number, with the format IP:PORT. 
                 If --addr is not specified then listen on 127.0.0.1:4000"),
                arg!(-e --engine <ENGINE_NAME> "If --engine is specified, then ENGINE-NAME must be either \"kvs\" 
                , in which case the built-in engine is used, or \"sled\", in which case 
                 sled is used. If this is the first run (there is no data previously persisted) 
                  then the default value is \"kvs\"; 
                  if there is previously persisted data 
                  then the default is the engine already in use. 
                  If data was previously persisted with a different engine than selected, 
                  print an error and exit with a non-zero exit code."),
                /* arg!(-t --thread-pool <THREADPOOL_NAME> "This option is for benchmark. 
                Specify the threadpool used. It must be one of naive, shared_queue or rayon"),
                arg!(-n --worker-num <WORKER_NUM> "This option is for benchmark. 
                Specify the worker num of the thread pool. Default 8") */
            ]
        ).get_matches();

    let default_ip = "127.0.0.1:4000".to_string();
    let default_engine = "kvs".to_string();
    /* let default_thread_pool = "shared_queue".to_string();
    let default_worker_num = 8; */

    let ip = matches.get_one::<String>("addr").unwrap_or(&default_ip);
    let engine = matches
        .get_one::<String>("engine")
        .unwrap_or(&default_engine);
    /* let thread_pool = matches.get_one::<String>("thread-pool").unwrap_or(&default_engine);
    let worker_num = matches.get_one::<u32>("worker-num").unwrap_or(&default_worker_num); */

    if engine != "kvs" && engine != "sled" {
        error!("Invalid engine. Must be 'kvs' or 'sled'");
        exit(1);
    }

    let server: KvServer;
    let path = current_dir()?.join("config.json");
    if path.exists() {
        server = KvServer::load(path)?;
        if server.engine != engine.to_string() {
            eprintln!("Wrong engine");
            exit(1);
        }
    } else {
        server = KvServer::new(path, engine.to_string())?;
    }

    let pool = SharedQueueThreadPool::new(8)?;
    // let pool = NaiveThreadPool::new(8)?;

    if engine == "kvs" {
        KvServer::start(engine, ip, KvStore::open(current_dir()?)?, pool)?;
    } else if engine == "sled" {
        KvServer::start(engine, ip, SledStore::open(current_dir()?)?, pool)?;
    }

    Ok(())
}

// rust error handling
