use clap::{arg, Command};
use kvs::engines::sled::SledStore;
use kvs::thread_pool::{SharedQueueThreadPool, RayonThreadPool};
use kvs::{Command as kCommand, KvStore, KvsEngine, Record, Result};
use kvs::{NaiveThreadPool, ThreadPool};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::{env::current_dir, process::exit};
use stderrlog::{self, LogLevelNum, Timestamp};

#[derive(Serialize, Deserialize)]
pub struct KvServer {
    engine: String,
}

impl KvServer {
    pub fn load(path: impl Into<PathBuf>) -> Result<KvServer> {
        let value = std::fs::read_to_string(path.into())?;
        let server: KvServer = serde_json::from_str(&value)?;
        Ok(server)
    }

    pub fn new(path: impl Into<PathBuf>, engine: String) -> Result<KvServer> {
        let server = KvServer { engine };
        let value = serde_json::to_string(&server)?;
        let mut f = File::create(path.into())?;
        f.write(value.as_bytes())?;
        f.flush()?;
        Ok(server)
    }

    fn serve(socket: TcpStream, store: impl KvsEngine) {
        info!("New client: {}", socket.peer_addr().unwrap());

        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut writer = BufWriter::new(socket);

        // body
        let mut buf: [u8; 4] = [0; 4];
        let n = reader.read(&mut buf).unwrap();
        if n != buf.len() {
            error!("Corrupted request, not reading enough bytes");
        }
        // big end in network programming
        let length = u32::from_be_bytes(buf);
        debug!("The total packet length is: {length}");
        let mut chunk = reader.take((length - 4).into());
        debug!("{:?}", chunk);
        let mut value = String::new();
        let n = chunk.read_to_string(&mut value).unwrap();
        debug!("{value}");
        if n < (length - 4) as usize {
            error!("Corrupted request, not reading enough bytes");
        }
        let record: Record = serde_json::from_str(&value).unwrap();
        match record.cmd {
            kCommand::Set => {
                match store.set(record.key, record.value) {
                    Ok(_) => writer.write("Successful set operation".as_bytes()).unwrap(),
                    Err(e) => {
                        writer.write("ERROR: ".as_bytes()).unwrap()
                            + writer.write(e.to_string().as_bytes()).unwrap()
                    }
                };
                // socket.shutdown(Shutdown::Both)?;
            }
            kCommand::Get => {
                match store.get(record.key.clone()).unwrap() {
                    None => {
                        writer
                            .write("ERROR: NO such key in storage".as_bytes())
                            .unwrap();
                        warn!("NO such key in storage: {}", record.key);
                    }
                    Some(value) => {
                        let len = value.len() as u32;
                        // writer.write(&len.to_be_bytes())?;
                        writer.write(value.as_bytes()).unwrap();
                    }
                };
                // socket.shutdown(Shutdown::Both)?;
            }
            kCommand::Remove => {
                match store.remove(record.key) {
                    Ok(_) => writer
                        .write("Successful remove operation".as_bytes())
                        .unwrap(),
                    Err(e) => {
                        writer.write("ERROR: ".as_bytes()).unwrap()
                            + writer.write(e.to_string().as_bytes()).unwrap()
                    }
                };
                // socket.shutdown(Shutdown::Both)?;
            }
        }
        writer.flush().unwrap();
    }

    pub fn start(&self, ip: &String, store: impl KvsEngine, pool: impl ThreadPool) -> Result<()> {
        let engine = &self.engine;
        info!(env!("CARGO_PKG_VERSION"));
        info!("ENGINE: {engine}, IP: {ip}");

        let listener = TcpListener::bind(ip)?;
        info!("Listen at {ip}");

        for socket in listener.incoming() {
            let n_store = store.clone();
            pool.spawn(move || Self::serve(socket.unwrap(), n_store))
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    stderrlog::new()
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
                arg!(-t --thread-pool <THREADPOOL_NAME> "This option is for benchmark. 
                Specify the threadpool used. It must be one of naive, shared_queue or rayon"),
                arg!(-n --worker-num <WORKER_NUM> "This option is for benchmark. 
                Specify the worker num of the thread pool. Default 8")
            ]
        ).get_matches();

    let default_ip = "127.0.0.1:4000".to_string();
    let default_engine = "kvs".to_string();
    let default_thread_pool = "shared_queue".to_string();
    let default_worker_num = 8;

    let ip = matches.get_one::<String>("addr").unwrap_or(&default_ip);
    let engine = matches
        .get_one::<String>("engine")
        .unwrap_or(&default_engine);
    let thread_pool = matches.get_one::<String>("thread-pool").unwrap_or(&default_engine);
    let worker_num = matches.get_one::<u32>("worker-num").unwrap_or(&default_worker_num);

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

    if engine == "kvs" {
        server.start(ip, KvStore::open(current_dir()?)?, pool)?;
    } else if engine == "sled" {
        server.start(ip, SledStore::open(current_dir()?)?, pool)?;
    }

    Ok(())
}

// rust error handling
