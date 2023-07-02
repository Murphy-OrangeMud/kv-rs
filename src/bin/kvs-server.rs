use clap::{Command, arg};
use kvs::engines::sled::SledStore;
use serde::{Serialize, Deserialize};
use std::{process::exit, env::current_dir};
use std::io::{Read, Write, BufReader, BufWriter};
use kvs::{Command as kCommand, Record, KvsEngine, KvStore, Result};
use std::net::{TcpListener};
use log::{info, warn, error, debug};
use stderrlog::{self, Timestamp, LogLevelNum};
use std::path::PathBuf;
use std::fs::File;

#[derive(Serialize, Deserialize)]
struct KvServer {
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
    
    fn set(&self, mut store: impl KvsEngine, key: String, value: String, mut writer: impl Write) -> Result<()> {
        match store.set(key, value) {
            Ok(_) => {
                writer.write("Successful set operation".as_bytes())?
            },
            Err(e) => {
                writer.write("ERROR: ".as_bytes())? + writer.write(e.to_string().as_bytes())?
            }
        };
        writer.flush()?;
        Ok(())
    }

    fn get(&self, mut store: impl KvsEngine, key: String, mut writer: impl Write) -> Result<()> {
        match store.get(key.clone())? {
            None => {
                writer.write("ERROR: NO such key in storage".as_bytes())?;
                warn!("NO such key in storage: {}", key);
            },
            Some(value) => {
                let len = value.len() as u32;
                //writer.write(&len.to_be_bytes())?;
                writer.write(value.as_bytes())?;
            }
        };
        writer.flush()?;
        Ok(())
    }

    fn remove(&self, mut store: impl KvsEngine, key: String, mut writer: impl Write) -> Result<()> {
        match store.remove(key) {
            Ok(_) => {
                writer.write("Successful remove operation".as_bytes())?
            },
            Err(e) => {
                writer.write("ERROR: ".as_bytes())? + writer.write(e.to_string().as_bytes())?
            }
        };
        writer.flush()?;
        Ok(())
    }

    pub fn start(&mut self, ip: &String) -> Result<()> {
        let engine = &self.engine;
        info!(env!("CARGO_PKG_VERSION"));
        info!("ENGINE: {engine}, IP: {ip}");

        let listener = TcpListener::bind(ip)?;
        info!("Listen at {ip}");

        loop {
            match listener.accept() {
                Ok((socket, addr)) => {
                    info!("New client: {addr}");
                    let mut reader = BufReader::new(socket.try_clone()?);
                    let writer = BufWriter::new(socket);
                    // TODO: add authentication system
                    /* let mut buf: [u8; 8] = [0; 8];
                    let n = socket.read(&mut buf)?;
                    if n != buf.len() {
                        error!("Corrupted request, not reading enough bytes");
                    }
                    let all_len = u64::from_be_bytes(buf);
                    let mut all_chunk = socket.take(all_len - 8);
                    let mut buffer = String::new();
                    let n = all_chunk.read_to_string(&mut buffer)?;
                    if n != all_len as usize {
                        error!("Corrupted request, not reading enough bytes");
                    }

                    let username_len = match buffer[0..1].parse::<u8>() {
                        Ok(len) => len,
                        Err(_) => {
                            error!("Parse request error");
                            error!("Request format: |    0x08    |     0x01     |     ..username_len..     |          0x04         | ..payload_len.. |");
                            error!("                | packet_len | username_len | username (less than 255) | payload_len (command) |     payload     |");
                            continue;
                        }
                    };

                    let x = (1 + username_len) as i32;

                    let mut buf: [u8; 1] = [0; 1];
                    let n = all_chunk.read(&mut buf)?;
                    if n != buf.len() {
                        error!("Corrupted request, not reading enough bytes");
                    }

                    let mut chunk = all_chunk.take(buf[0].into());
                    let mut username = String::new();
                    let n = chunk.read_to_string(&mut username)?;
                    if n < buf[0] as usize {
                        error!("Corrupted request, not reading enough bytes");
                        continue;
                    } */

                    // body
                    let mut buf: [u8; 4] = [0; 4];
                    let n = reader.read(&mut buf)?;
                    if n != buf.len() {
                        error!("Corrupted request, not reading enough bytes");
                    }
                    // big end in network programming
                    let length = u32::from_be_bytes(buf);
                    debug!("The total packet length is: {length}");
                    let mut chunk = reader.take((length - 4).into());
                    debug!("{:?}", chunk);
                    let mut value = String::new();
                    let n = chunk.read_to_string(&mut value)?;
                    debug!("{value}");
                    if n < (length - 4) as usize {
                        error!("Corrupted request, not reading enough bytes");
                        continue;
                    }
                    let record: Record = serde_json::from_str(&value)?;
                    match record.cmd {
                        kCommand::Set => {
                            if engine == "kvs" {
                                self.set(KvStore::open(current_dir()?)?, record.key, record.value, writer)?;
                            } else if engine == "sled" {
                                self.set(SledStore::open(current_dir()?)?, record.key, record.value, writer)?;
                            }
                            // socket.shutdown(Shutdown::Both)?;
                        },
                        kCommand::Get => {
                            if engine == "kvs" {
                                self.get(KvStore::open(current_dir()?)?, record.key, writer)?;
                            } else if engine == "sled" {
                                self.get(SledStore::open(current_dir()?)?, record.key, writer)?;
                            }
                            // socket.shutdown(Shutdown::Both)?;
                        },
                        kCommand::Remove => {
                            if engine == "kvs" {
                                self.remove(KvStore::open(current_dir()?)?, record.key, writer)?;
                            } else if engine == "sled" {
                                self.remove(SledStore::open(current_dir()?)?, record.key, writer)?;
                            }
                            // socket.shutdown(Shutdown::Both)?;
                        }
                    }
                    // writer.flush()?;
                },
                Err(e) => {
                    error!("Error accepting connections: {e}");
                }
            }
        }
    }
}

fn main() -> Result<()> {
    stderrlog::new().module(module_path!()).timestamp(Timestamp::Second).verbosity(LogLevelNum::Debug).init().unwrap();
    
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
                  print an error and exit with a non-zero exit code.")
            ]
        ).get_matches();
        
    let default_ip = "127.0.0.1:4000".to_string();
    let default_engine = "kvs".to_string();
    let ip = matches.get_one::<String>("addr").unwrap_or(&default_ip);
    let engine = matches.get_one::<String>("engine").unwrap_or(&default_engine);

    if engine != "kvs" && engine != "sled" {
        error!("Invalid engine. Must be 'kvs' or 'sled'");
        exit(1);
    }

    let mut server: KvServer;
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

    server.start(ip)?;

    Ok(())
}

// rust error handling