use crate::{Command as kCommand, /* KvStore, */ KvsEngine, Record, Result, SledStore};
use crate::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::env::current_dir;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct ServerConfig {
    engine: String,
    threadpool: Option<String>,
    worker_num: Option<u32>,
}

impl ServerConfig {
    pub fn load(path: impl Into<PathBuf>) -> Result<ServerConfig> {
        let value = std::fs::read_to_string(path.into())?;
        let config: ServerConfig = serde_json::from_str(&value)?;
        Ok(config)
    }

    pub fn new(path: impl Into<PathBuf>, engine: String) -> Result<ServerConfig> {
        let server = ServerConfig {
            engine,
            threadpool: None,
            worker_num: None,
        };
        let value = serde_json::to_string(&server)?;
        let mut f = File::create(path.into())?;
        f.write(value.as_bytes())?;
        f.flush()?;
        Ok(server)
    }
}

#[derive(Serialize, Deserialize)]
pub struct KvServer {
    pub engine: String,
}

impl KvServer {
    pub fn load(path: impl Into<PathBuf>) -> Result<KvServer> {
        let value = std::fs::read_to_string(path.into())?;
        let config: KvServer = serde_json::from_str(&value)?;
        Ok(config)
    }

    pub fn new(path: impl Into<PathBuf>, engine: String) -> Result<KvServer> {
        let server = KvServer { engine }; //{ engine, threadpool: None, worker_num: None };
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

    pub fn start(
        engine: &String,
        ip: &String,
        store: impl KvsEngine,
        pool: impl ThreadPool,
    ) -> Result<()> {
        info!(env!("CARGO_PKG_VERSION"));
        info!("ENGINE: {engine}, IP: {ip}");

        let listener = TcpListener::bind(ip)?;
        info!("Listen at {ip}");

        for socket in listener.incoming() {
            let n_store = store.clone();
            // pool.spawn(move || Self::serve(socket.unwrap(), n_store))
        }

        Ok(())
    }
}
